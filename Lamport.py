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
    local_time = new_timestamp + 1

class Lamport:
    def __init__(self):
        self.request_queue = []

        self.REQUESTED = False
        self.nodes_replied = set()

        self.request_timestamp = (-1, -1, -1)

    def loop_node(self):
        req = comm.irecv()
        while True:
            # With some probability, make a request
            # Do not allow re-requesting
            if not self.REQUESTED and random.random() < 0.2:
                self.request()

            # Try receiving a message
            data = req.test()
            if data[0] == True:
                req = comm.irecv()

            # Receive replies to requests
            if self.REQUESTED == True:
                if data[0] == True and data[1] is not None and data[1][2] == 0:
                    msg_timestamp = data[1][0]
                    sender = data[1][1]

                    update_local_time(msg_timestamp)

                    if msg_timestamp > self.request_timestamp[0]:
                        self.nodes_replied.add(sender)

            # All nodes replied, enter CS now
            if len(self.nodes_replied) == size - 1 and \
                self.request_queue[0][0] == self.request_timestamp[0] and \
                self.request_queue[0][1] == self.request_timestamp[1]:

                # Reset
                self.exec_cs()
                self.REQUESTED = False
                self.nodes_replied = set()

                # Pop the queue
                heappop(self.request_queue)
                self.release(self.request_timestamp)

                self.request_timestamp = (-1, -1, -1)

            # Send replies to requests
            if data[0] == True and data[1] is not None and data[1][2] == 1:
                
                msg_timestamp = data[1][0]
                sender = data[1][1]

                update_local_time(msg_timestamp)

                # Reply
                self.reply(sender, data[1])

            # Receive release
            if data[0] == True and data[1] is not None and data[1][2] == 2:
                msg_timestamp = data[1][0]
                sender = data[1][1]

                update_local_time(msg_timestamp)

                new_queue = []
                for i in self.request_queue:
                    if i != data[1][3]:
                        heappush(new_queue, i)

                self.request_queue = new_queue

            sleep(random.random())

    def request(self):
        # Request all nodes
        update_local_time(local_time)
        msg = (local_time, rank, 1)
        self.REQUESTED = True
        self.request_timestamp = msg
        print('requesting timestamp ', self.request_timestamp, flush=True)

        for r in range(size):
            if r != rank:
                # Send timestamped message
                comm.send(msg, dest=r)

        # Add req to priority_queue
        heappush(self.request_queue, msg)
        sleep(random.random())

    def reply(self, receiver, timestamp):
        if receiver != rank:
            update_local_time(local_time)
            msg = (local_time, rank, 0)
            comm.send(msg, dest=receiver)

            # Add req to priority_queue
            heappush(self.request_queue, timestamp)
        sleep(random.random())

    def release(self, timestamp):
        update_local_time(local_time)
        msg = (local_time, rank, 2, timestamp)
        self.request_timestamp = msg

        for r in range(size):
            if r != rank:
                # Send timestamped message
                comm.send(msg, dest=r)

        sleep(random.random())

    def exec_cs(self):
        print('executing cs for', rank, '... having timestamp ', self.request_timestamp, flush=True)
        update_local_time(local_time)
        critical_function()


def critical_function(filename1='tmp.txt'):
    with open(filename1, 'w+') as f:
        f.write(str(rank))

    sleep(2)
    with open(filename1, 'r') as f:
        line = f.readline().strip()
        if line == '' or int(line) != rank:
            print("\n\n\n******CS FAILED*****\n\n\n", flush=True)
            
        else:
            print("CS PASSED", flush=True)


if __name__ == '__main__':
    algo = Lamport()
    algo.loop_node()
