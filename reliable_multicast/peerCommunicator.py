from socket  import *
from constMP import * #-
import random
import time
import pickle
from requests import get
from reliableMulticast import ReliableFifoMulticast

#handShakes = [] # not used; only if we need to check whose handshake is missing

PEERS = []

# Number of the current experiment round (the comparison server starts all peers once per round)
epoch = 0

# UDP socket to send and receive data messages (the multicast logic is in reliableMulticast.py):
udpSocket = socket(AF_INET, SOCK_DGRAM)
# Allow immediate reuse of the port after closing
udpSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
udpSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
# Allow immediate reuse of the port after closing
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Getting list of peers from group manager: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  PEERS = pickle.loads(msg)
  print ('Got list of peers: ', PEERS)
  clientSock.close()
  return PEERS

def sendLogToServer(logList):
  # Send the list of messages to the server (using a TCP socket) for comparison
  print('Sending the list of messages to the server for comparison...')
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((SERVER_ADDR, SERVER_PORT))
  clientSock.sendall(pickle.dumps(logList))
  clientSock.close()

# Function to wait for start signal from comparison server:
def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  epoch = epoch + 1
  PEERS = getListOfPeers()

  # Reliable FIFO multicast over UDP; its thread starts receiving right away
  multicast = ReliableFifoMulticast(udpSocket, [(ip, PEER_UDP_PORT) for ip in PEERS],
                                    myself, epoch, RETRANSMIT_TIMEOUT, LINGER_TIME, LOSS_RATE)

  # Handshake (retransmitted until every peer acknowledged it)
  multicast.handshake()
  print('Main Thread: handshakes completed.')

  # Send a sequence of data messages to all peers
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    multicast.multicast(msgNumber)
    print('Sent message ' + str(msgNumber))

  # Tell all processes that I have no more messages, and wait until all messages
  # from everyone were delivered (in FIFO order) and mine were acknowledged
  logList = multicast.finish()

  # Write log file
  logFile = open('logfile'+str(myself)+'.log', 'w')
  logFile.writelines(str(logList))
  logFile.close()

  sendLogToServer(logList)
