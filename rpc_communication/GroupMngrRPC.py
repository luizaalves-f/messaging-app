from xmlrpc.server import SimpleXMLRPCServer
import threading
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

# o SimpleXMLRPCServer ja ativa o SO_REUSEADDR (allow_reuse_address = True)
server = SimpleXMLRPCServer(('0.0.0.0', port), logRequests=False)

def register(ipaddr, peerPort):
  membership.append((ipaddr, peerPort))
  print ('Registered peer: ', (ipaddr, peerPort))
  return True

def listPeers():
  list = []
  for m in membership:
    list.append(m[0])
  print ('List of peers sent: ', list)
  return list

def unregister(ipaddr, peerPort):
  # a fazer
  return False

def stop():
  print("Stopping.")
  # shutdown() espera o serve_forever() terminar, entao precisa rodar em outra
  # thread; chama-lo aqui, dentro do tratamento da requisicao, causaria deadlock
  threading.Thread(target=server.shutdown).start()
  return True

# mesmos nomes de operaçao da versao com sockets (campo "op" do dicionario recebido)
server.register_function(register, 'register')
server.register_function(listPeers, 'list')
server.register_function(unregister, 'unregister')
server.register_function(stop, 'stop')

server.serve_forever()
server.server_close()
