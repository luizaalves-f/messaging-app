from socket  import *
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from constMP import * #-
import threading
import random
import time
import pickle
import queue
from requests import get

# as mensagens de dados entre peers continuam em udp
# so as interaçoes cliente servidor (group manager, comparison server) usam rpc

# contador p/ garantir que recebemos os handshakes de todos os outros processos
handShakeCount = 0

PEERS = []

# sockets udp pra enviar e receber mensagens de dados
# cria o socket de envio
sendSocket = socket(AF_INET, SOCK_DGRAM)
# cria o socket de recebimento e associa com a porta
recvSocket = socket(AF_INET, SOCK_DGRAM)
# permite reutilizar a porta imediatamente apos encerramento
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# proxies rpc para os 2 servidores
groupMngr = ServerProxy('http://%s:%d' % (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
comparisonServer = ServerProxy('http://%s:%d' % (SERVER_ADDR, SERVER_PORT))

# fila de sinais de inicio recebidos do comparison server (preenchida pela thread rpc)
startQueue = queue.Queue()

# procedimento rpc chamado pelo comparison server p/ iniciar esse peer
def start(myself, nMsgs):
  startQueue.put((myself, nMsgs))
  return 'Peer process '+str(myself)+' started.'

# servidor rpc p/ receber o sinal de inicio do comparison server
rpcServer = SimpleXMLRPCServer(('0.0.0.0', PEER_TCP_PORT), logRequests=False)
rpcServer.register_function(start, 'start')
threading.Thread(target=rpcServer.serve_forever, daemon=True).start()

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# funcao p/ registrar este peer no group manager
def registerWithGroupManager():
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  print ('Registering with group manager: ', (ipAddr, PEER_UDP_PORT))
  groupMngr.register(ipAddr, PEER_UDP_PORT)

def getListOfPeers():
  print ('Getting list of peers from group manager')
  PEERS = groupMngr.list()
  print ('Got list of peers: ', PEERS)
  return PEERS

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')

    global handShakeCount

    logList = []

    # espera ate receber os handshakes de todos os outros processos
    # (pra garantir que todos os processos estejam sincronizados antes de comecar a trocar mensagens)
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':

        # to do: responder ao handshake e esperar a confirmacao

        handShakeCount = handShakeCount + 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0
    while True:
      msgPack = self.sock.recv(1024)   # recebe dados do cliente
      msg = pickle.loads(msgPack)
      if msg[0] == -1:   # conta as mensagens de stop dos outros processos
        stopCount = stopCount + 1
        if stopCount == N:
          break  # sai do loop quando todos os outros processos terminarem
      else:
        print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
        logList.append(msg)

    # grava o arquivo de log
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()

    # envia a lista de mensagens ao servidor (via rpc) pra comparacao
    print('Sending the list of messages to the server for comparison...')
    comparisonServer.submitLog(logList)

    # zera o contador de handshakes
    handShakeCount = 0

    exit(0)

# funcao pra esperar o sinal de inicio do comparison server
def waitToStart():
  return startQueue.get()

# a partir daqui o codigo é executado quando o programa inicia
registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    # shutdown() so retorna depois que a requisiçao atual (a chamada start()) for
    # respondida, entao o comparison server recebe a resposta antes de sair
    rpcServer.shutdown()
    exit(0)

  # cria handler de recebimento de mensagens
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()

  # envia hand shakes
  # to do: continuar enviando ate receber resposta de cada processo
  # enviar confirmacao de resposta
  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass  # encontrar uma forma melhor de esperar handshakes

  # envia uma sequencia de mensagens de dados para todos os outros processos
  for msgNumber in range(0, nMsgs):
    # espera um tempo aleatorio entre mensagens sucessivas
    time.sleep(random.randrange(10,100)/1000)
    msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))

  # avisa todos os processos que nao tem mais mensagens para enviar
  for addrToSend in PEERS:
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
