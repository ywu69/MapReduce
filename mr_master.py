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
        splited_files = self.split_file(input_filename)

        #Align map tasks to workers
        print 'MAP phase'
        i = 0;
        print splited_files
        l = len(splited_files)
        print l
        procs = []

        while(i<l):
            for w in self.workers:
                if self.workers[w][0] == 'READY':
                    print 'let him do map'
                    gevent.spawn(self.workers[w][1].do_map, job_name, splited_files[i])
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

    def split_file(self,filename):
        fileSize = os.path.getsize(filename)
        print(fileSize)
        subfile_size = int(fileSize/len(self.workers))
        index = 1
        splited_files = []
        with open(filename) as inputfile:
            current_size = 0
            outputfile = open('sub_inputfile_' + str(index) + '.txt', 'w')
            for line in inputfile:
                current_size += len(line)
                #print(current_size)
                outputfile.write(line)
                if index < len(self.workers) and current_size >= subfile_size:
                    current_size = 0
                    outputfile.close()
                    splited_files.append('sub_inputfile_' + str(index) + '.txt')
                    index = index + 1
                    outputfile = open('sub_inputfile_' + str(index) + '.txt', 'w')
            outputfile.close()
            splited_files.append('sub_inputfile_' + str(index) + '.txt')
        return splited_files

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
    port = 4242#sys.argv[1]
    data_dir = "inputfile.txt"#sys.argv[2]
    master_addr = 'tcp://0.0.0.0:' + str(port)
    s = zerorpc.Server(Master(data_dir))
    s.bind(master_addr)
    s.run()
    #m = Master(data_dir)
    #m.split_file(data_dir)
