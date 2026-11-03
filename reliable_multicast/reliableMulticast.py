import pickle
import random
import threading
import time
from socket import timeout


class ReliableFifoMulticast:
  """Multicast over UDP with loss recovery (ACK + retransmission) and FIFO order.

  Peers are identified by their index in `peers` (a list of (ip, port), including
  this peer). Packets, all pickled tuples carrying the round number (epoch):
    ('HS',    epoch, sender)                  handshake
    ('HSACK', epoch, sender)                  handshake acknowledgement
    ('DATA',  epoch, sender, seq, payload)    payload None marks the end of the stream
    ('ACK',   epoch, acker, sender, seq)      acknowledges DATA (sender, seq)
  """

  def __init__(self, sock, peers, myself, epoch, rto=0.2, linger=2.0, lossRate=0.0):
    self.sock = sock
    self.peers = peers
    self.myself = myself
    self.epoch = epoch
    self.rto = rto
    self.linger = linger
    self.lossRate = lossRate
    self.n = len(peers)

    self.lock = threading.Lock()
    self.hsSeen = set()      # peers whose handshake we received
    self.hsAcked = set()     # peers that acknowledged our handshake
    self.pending = {}        # (dest, seq) -> [packet, time of last transmission]
    self.nextSeq = 0         # next sequence number to use when sending
    self.expected = [0] * self.n          # next seq to deliver, per sender
    self.buffer = [dict() for _ in peers]  # out-of-order packets held back, per sender
    self.finished = set()    # senders whose end-of-stream marker was delivered
    self.log = []            # delivered messages (sender, seq), in delivery order
    self.stopping = False
    self.thread = threading.Thread(target=self._receiveLoop)
    self.thread.start()

  # ---- sending ----

  def _transmit(self, dest, packet):
    self.sock.sendto(pickle.dumps(packet), self.peers[dest])

  def handshake(self):
    """Block until every peer has acknowledged our handshake and we saw theirs."""
    while True:
      with self.lock:
        if len(self.hsAcked) == self.n and len(self.hsSeen) == self.n:
          return
        missing = [p for p in range(self.n) if p not in self.hsAcked]
      for p in missing:
        self._transmit(p, ('HS', self.epoch, self.myself))
      time.sleep(self.rto)

  def multicast(self, payload):
    """Send payload to all peers (including this one); returns immediately."""
    with self.lock:
      seq = self.nextSeq
      self.nextSeq += 1
      packet = ('DATA', self.epoch, self.myself, seq, payload)
      now = time.time()
      for p in range(self.n):
        self.pending[(p, seq)] = [packet, now]
    for p in range(self.n):
      self._transmit(p, packet)

  def finish(self):
    """Announce end of stream, wait until everything is delivered and acknowledged,
    and return the log of delivered messages."""
    self.multicast(None)
    while True:
      with self.lock:
        if not self.pending and len(self.finished) == self.n:
          break
      time.sleep(0.05)
    # Our last ACKs may have been lost; keep answering retransmissions for a while.
    time.sleep(self.linger)
    self.stopping = True
    self.thread.join()
    return self.log

  # ---- receiving ----

  def _receiveLoop(self):
    self.sock.settimeout(self.rto / 4)
    while not self.stopping:
      try:
        msgPack = self.sock.recv(4096)
      except timeout:
        msgPack = None
      if msgPack is not None and random.random() >= self.lossRate:
        self._handle(pickle.loads(msgPack))
      self._retransmit()

  def _retransmit(self):
    now = time.time()
    due = []
    with self.lock:
      for (dest, seq), entry in self.pending.items():
        if now - entry[1] >= self.rto:
          entry[1] = now
          due.append((dest, entry[0]))
    for dest, packet in due:
      self._transmit(dest, packet)

  def _handle(self, msg):
    kind, epoch = msg[0], msg[1]
    if epoch != self.epoch:
      return  # leftover from another round
    if kind == 'HS':
      with self.lock:
        self.hsSeen.add(msg[2])
      self._transmit(msg[2], ('HSACK', self.epoch, self.myself))  # also for repeated HS
    elif kind == 'HSACK':
      with self.lock:
        self.hsAcked.add(msg[2])
    elif kind == 'ACK':
      with self.lock:
        self.pending.pop((msg[2], msg[4]), None)
    elif kind == 'DATA':
      sender, seq, payload = msg[2], msg[3], msg[4]
      self._transmit(sender, ('ACK', self.epoch, self.myself, sender, seq))  # also for duplicates
      with self.lock:
        if seq < self.expected[sender]:
          return  # duplicate of something already delivered
        self.buffer[sender][seq] = payload
        while self.expected[sender] in self.buffer[sender]:
          payload = self.buffer[sender].pop(self.expected[sender])
          if payload is None:
            self.finished.add(sender)
          else:
            self.log.append((sender, self.expected[sender]))
            print('Message ' + str(self.expected[sender]) + ' from process ' + str(sender))
          self.expected[sender] += 1
