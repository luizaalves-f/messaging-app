PEER_UDP_PORT = 6789
PEER_TCP_PORT = 5679
N = 6   # Number of peers
SERVER_ADDR ='34.203.80.210'
SERVER_PORT = 5678
GROUPMNGR_ADDR = '34.203.80.210'
GROUPMNGR_TCP_PORT = 5680


# Reliable FIFO multicast (peer <-> peer, UDP)
RETRANSMIT_TIMEOUT = 0.2   # seconds before an unacknowledged packet is resent
LINGER_TIME = 2.0          # seconds a finished peer keeps acknowledging retransmissions
LOSS_RATE = 0.0            # probability of discarding each received packet (simulates loss; 0 = off)
