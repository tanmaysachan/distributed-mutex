import time
from time import sleep
from mpi4py import MPI
import numpy as np
import random
from queue import Queue
from heapq import *

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# Defines the local time for this process
local_time = 0

def update_local_time(new_timestamp):
    global local_time
    local_time = max(local_time + 1, new_timestamp + 1)

class Raymond:
    def __init__(self):
        self.holder = max(0, (rank-1)//2)
        self.request_q = Queue()
        self.asked = False
        self.requested = False

    def loop_node(self):
        req = comm.irecv()
        while True:
            if not self.asked and random.random() < 0.01:
                print('requesting, ', rank, flush=True)
                self.requested = True
                self.make_request(1)

            # Try receiving a message
            data = req.test()
            if data[0] == True:
                req = comm.irecv()

            if not self.request_q.empty() and self.request_q.queue[0] == rank \
                        and self.asked == True and self.holder == rank:
                # pop before exec
                self.request_q.get()

                self.asked = False
                self.exec_cs()

            if self.holder == rank:
                if not self.request_q.empty() and self.request_q.queue[0] != rank:
                    self.transfer_privilege()
                    if not self.request_q.empty():
                        self.requested = True
                        self.make_request(0)

            # Request Message
            if data[0] == True and data[1] is not None and data[1][1] == 0:
                sender = data[1][0]
                self.request_q.put(sender)
                if not self.requested:
                    self.requested = True
                    self.make_request(0)

            # Receive Privilege
            if data[0] == True and data[1] is not None and data[1][1] == 1:
                sender = data[1][0]
                self.requested = False
                self.holder = rank

            sleep(random.random())

    def make_request(self, t):
        if self.holder != rank:
            # request originates
            if t == 1:
                self.asked = True
                self.request_q.put(rank)
            msg = (rank, 0)
            comm.send(msg, dest=self.holder)
        elif t == 1:
            self.asked = True
            self.request_q.put(rank)

    def transfer_privilege(self):
        rec = self.request_q.get()
        print('transferring from ', rank, ' to ', rec, flush=True)
        self.holder = rec
        msg = (rank, 1)
        comm.send(msg, dest=rec)

    def exec_cs(self):
        print('executing cs for', rank, '...', flush=True)
        update_local_time(local_time)
        critical_function()


def critical_function(filename1='tmp.txt'):
    with open(filename1, 'w+') as f:
        f.write(str(rank))

    sleep(2)
    with open(filename1, 'r') as f:
        line = f.readline().strip()
        if line == '' or int(line) != rank:
            print("\n\n\n******CS FAILED*****\nExpected {} found {}\n\n".format(rank, int(line)), flush=True)
            
        else:
            print("CS PASSED", flush=True)


if __name__ == '__main__':
    algo = Raymond()
    algo.loop_node()
