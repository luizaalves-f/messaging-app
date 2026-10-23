from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import queue
from constMP import *

# fila de logs recebidos dos peers (preenchida pela thread rpc, consumida pelo loop principal)
logQueue = queue.Queue()

def submitLog(logList):
	print ('Received log from peer')
	logQueue.put(logList)
	return True

# servidor rpc onde os peers entregam os logs de mensagens
server = SimpleXMLRPCServer(('0.0.0.0', SERVER_PORT), logRequests=False)
server.register_function(submitLog, 'submitLog')
threading.Thread(target=server.serve_forever, daemon=True).start()

groupMngr = ServerProxy('http://%s:%d' % (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))

def mainLoop():
	while 1:
		nMsgs = promptUser()
		peerList = groupMngr.list()
		print("List of Peers: ", peerList)
		startPeers(peerList,nMsgs)
		if nMsgs != 0:
			print('Now, wait for the message logs from the communicating peers...')
			waitForLogsAndCompare(nMsgs)
		else:
			print('Stopping.')
			server.shutdown()
			server.server_close()

			# avisa o group manager para parar
			groupMngr.stop()
			break

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# chama start() em cada um dos peers:
	peerNumber = 0
	for peer in peerList:
		peerProxy = ServerProxy('http://%s:%d' % (peer, PEER_TCP_PORT))
		print(peerProxy.start(peerNumber, nMsgs))
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	# loop para esperar os logs de mensagens para comparação:
	numPeers = 0
	msgs = [] # cada msg é uma lista de pares [remetente, msgNumber] (o xmlrpc transforma tuplas em listas)

	# recebe os logs de mensagens dos processos peer
	while numPeers < N:
		msgs.append(logQueue.get())
		numPeers = numPeers + 1

	unordered = 0

	# compara as listas de mensagens
	for j in range(0,N_MSGS-1):
		firstMsg = msgs[0][j]
		for i in range(1,N-1):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break

	print ('Found ' + str(unordered) + ' unordered message rounds')


# inicia o servidor
mainLoop()
