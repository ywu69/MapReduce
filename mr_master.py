__author__ = 'pengzhan'
import sys

import zerorpc
import os
import gevent

class Master(object):

    def __init__(self, data_dir):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.data_dir = data_dir
        self.workers = {}
        self.chunkState = {}

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print
            for w in self.workers:
                if self.workers[w][0] != "LOSS":
                    try:
                        self.workers[w][1].ping()
                    except Exception:
                        self.workers[w] = ("LOSS", self.workers[w][1])
                        print 'lost connection'
            gevent.sleep(1)

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = ('READY', c)
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)

    def set_worker_state(self, ip, port, state):
        gevent.spawn(self.set_worker_state_async, ip, port, state)

    def set_worker_state_async(self, ip, port, state):
        print 'set map state'
        self.workers[(ip,port)] = (state, self.workers[(ip,port)][1])

    def set_job(self,job_name, split_size, num_reducers, input_filename, output_filename_base):
        gevent.spawn(self.setJob_async, job_name, split_size, num_reducers, input_filename, output_filename_base)

    def setJob_async(self,job_name, split_size, num_reducers, input_filename, output_filename_base):
        #split input file
        chunks = self.split_file(input_filename, int(split_size))
        #Align map tasks to workers
        print 'MAP phase'
        i = 0;
        print chunks
        l = len(chunks)
        procs = []

        while(i<l):
            for w in self.workers:
                if self.workers[w][0] == 'READY':
                    print 'let him do map'
                    self.chunkState[chunks[i]] = ('NOTFINISH',w)
                    gevent.spawn(self.workers[w][1].do_map, job_name, input_filename, chunks[i])
                    i = i+1

        #Wait until all map tasks done
        print 'Wait until all map tasks done'
        #while True:
        #    mapDone = True
        #    for w in self.workers:
        #        if self.workers[w][0] != 'MAPDONE':
        #            mapDone = False
        #    if mapDone:
        #        break

        #Reduce
        #print 'REDUCE phase'
        #for w in self.workers:
        #    proc = gevent.spawn(self.workers[w][1].do_reduce, job_name)
        #    procs.append(proc)

        #wait for reduce done
        #print 'Wait until all reduce tasks done'
        #while True:
        #    reduceDone = True
        #    for w in self.workers:
        #        if self.workers[w][0] != 'REDUCEDONE':
        #            reduceDone = False
        #    if reduceDone:
        #        break

        #collect

    def split_file(self, filename, split_size):
        fileSize = os.path.getsize(filename)
        #print(fileSize)
        chunks = []
        with open(filename) as inputfile:
            current_size = 0
            offset = 0
            #outputfile = open('sub_inputfile_' + str(index) + '.txt', 'w')
            for line in inputfile:
                #print line
                current_size += len(line)
                offset += len(line)
                if current_size >= split_size:
                    current_size -= len(line)
                    offset -= len(line)
                    words = line.split(' ')
                    #print words
                    for w in words:
                        current_size += len(w)
                        offset += len(w)
                        if not w.endswith('\n'):
                            current_size += 1
                            offset += 1
                        print current_size
                        if current_size >= split_size:
                            done = True
                            chunks.append((current_size, offset-current_size))
                            current_size = 0


            #the last chunk
            if current_size > 0:
                chunks.append((current_size, offset-current_size))
        return chunks

    def do_job(self, nums):
        n = len(self.workers)
        chunk = len(nums) / n
        i = 0
        offset = 0
        #result = 0
        procs = []
        for w in self.workers:
            if i == (n - 1):
                sub = nums[offset:]
            else:
                sub = nums[offset:offset+chunk]

            proc = gevent.spawn(self.workers[w][1].do_work, sub)
            procs.append(proc)

            #result += int(self.workers[w][1].do_work(sub))
            i = i + 1
            offset = offset + chunk

        gevent.joinall(procs)
        return sum([int(p.value) for p in procs])

if __name__ == '__main__':
    #port = 4242#sys.argv[1]
    data_dir = "inputfile2.txt"#sys.argv[2]
    #master_addr = 'tcp://0.0.0.0:' + str(port)
    #s = zerorpc.Server(Master(data_dir))
    #s.bind(master_addr)
    #s.run()
    m = Master(data_dir)
    print m.split_file(data_dir, 18)
