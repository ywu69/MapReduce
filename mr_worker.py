__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent

import mapreduce

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_map(self, job_name, input_filename):
        print 'Doing MAP '+ job_name+','+input_filename
        #DO MAP TASK
        job_name_map = job_name+'MAP'
        mapper = WordCountMap()
        # Map phase
        with open(input_filename) as inputfile:
            for line in inputfile:
                mapper.map(0, line)

        # Sort intermediate keys
        table = mapper.get_table()
        print table
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'MAPDONE')

    def do_reduce(self, job_name, mapresults):
        print 'Doing REDUCE '+ job_name
        #DO_REDUCE Task
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'REDUCEDONE')

    def do_work(self, nums):
        nums = [int(n) for n in nums]
        gevent.sleep(2)
        return str(sum(nums))

if __name__ == '__main__':
    master_addr = "127.0.0.1:4242"#sys.argv[1];
    worker_ip = "127.0.0.1"
    worker_port = sys.argv[1]
    s = zerorpc.Server(Worker(master_addr,worker_ip,worker_port))
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()

    #print socket.gethostbyname(socket.gethostname())