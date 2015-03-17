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
        self.map_table = {}
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_map(self, job_name, input_filename, chunk):
        print 'Doing MAP '+ job_name+','+input_filename
        size = chunk[0]
        offset = chunk[1]
        self.c.set_chunk_state(size, offset, 'CHUNK_MAPPING')
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'MAPPING')
        print 'size = '+ str(size)
        print 'offset = ' + str(offset)
        #DO MAP TASK
        job_name_map = job_name+'MAP'
        mapper = WordCountMap()
        # Map phase
        with open(input_filename) as inputfile:
            curr_off = -1
            firstline_done = False
            for line in inputfile:
                curr_off += len(line)
                if not firstline_done and curr_off >= offset:
                    curr_off -= len(line)
                    words = line.split(' ')
                    newline = ''
                    findoff = False
                    print words
                    for w in words:
                        curr_off += len(w)
                        if not w.endswith('\n'):
                            curr_off += 1
                        print curr_off
                        if not findoff and curr_off >= offset:
                            findoff = True
                        if findoff and curr_off <= offset+size:
                            newline += w
                            if not w.endswith('\n'):
                                newline += ' '
                    print newline
                    mapper.map(0, newline)
                    firstline_done = True

                elif firstline_done and curr_off < offset+size:
                    mapper.map(0, line)
                elif curr_off >= offset+size:
                    curr_off -= len(line)
                    words = line.split(' ')
                    newline = ''
                    for w in words:
                        curr_off += len(w)
                        if not curr_off >= offset+size:
                            newline += w
                            if not w.endswith('\n'):
                                curr_off += 1
                                newline += ' '
                        if curr_off >= offset+size:
                            break
                    mapper.map(0, newline)

        # Sort intermediate keys
        self.map_table = mapper.get_table()
        print self.map_table

        self.c.set_worker_state(self.worker_ip, self.worker_port, 'MAPDONE')
        #send to reducer

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

    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())