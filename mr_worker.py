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
        #print self.result_list
        count = 0
        for v in vlist:
            count = count + int(v)
        val = 0
        if self.result_list.has_key(k):
            val = self.result_list[k]
        val += count
        self.result_list[k] = val
        #self.emit(k + ':' + str(count))

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)

        #Attributes of mapper
        self.map_table = {}
        self.num_reducer_received = 0
        self.num_reducers = 0
        gevent.spawn(self.controller)

        #Attributes of reducer
        self.mappers_list = {}
        self.map_result = {}
        self.map_result_collect_state = {}
        self.reduce_id = 0
        self.result_list = {}
        self.result_sent_to_master = False

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')


    def notice_received(self):
        gevent.spawn(self.notice_received_async)
    def notice_received_async(self):
        self.num_reducer_received += 1

    def do_map(self, job_name, input_filename, chunk, num_reducers):
        gevent.spawn(self.do_map_async, job_name, input_filename, chunk, num_reducers)

    def do_map_async(self, job_name, input_filename, chunk, num_reducers):
        print 'Doing MAP '+ job_name+','+input_filename
        size = chunk[0]
        offset = chunk[1]
        self.c.set_chunk_state(size, offset, 'CHUNK_MAPPING')
        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPPING')
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
                    #print words
                    for w in words:
                        curr_off += len(w)
                        if not w.endswith('\n'):
                            curr_off += 1
                        #print curr_off
                        if not findoff and curr_off >= offset:
                            findoff = True
                        if findoff and curr_off <= offset+size:
                            newline += w
                            if not w.endswith('\n'):
                                newline += ' '
                    #print newline
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
        self.map_table.keys().sort()
        self.num_reducers = num_reducers
        #print self.map_table


        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPRESULTCOLLECT')
        while self.num_reducer_received < num_reducers:
            gevent.sleep(1)
            continue

        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPDONE')
        #send to reducer

    def do_reduce(self, job_name, reduce_id, num_chunk):
        gevent.spawn(self.do_reduce_async, job_name, reduce_id, num_chunk)

    def do_reduce_async(self, job_name, reduce_id, num_chunk):
        #wait until all map data collected
        print 'Doing REDUCE '+ job_name+' reduce id = '+str(reduce_id)
        self.reduce_id = reduce_id
        reducer = WordCountReduce()

        while len(self.map_result_collect_state) < num_chunk:
            for e in self.mappers_list:
                gevent.spawn(self.reduce_single_map_result, reducer, e, reduce_id)
            gevent.sleep(1)
        self.c.set_worker_reduce_state(self.worker_ip, self.worker_port, 'REDUCERESULTCOLLECT')
        #alldone = False

        self.result_list = reducer.get_result_list()
        while not self.result_sent_to_master:
            gevent.sleep(1)
            continue
        #while not alldone:
        #    for w in self.mappers_list:####################
        #        gevent.spawn(self.reduce_single_map_result, reducer, key_from, key_to, w)
        self.c.set_worker_reduce_state(self.worker_ip, self.worker_port, 'REDUCEDONE')

    def master_notice_received(self):
        gevent.spawn(self.master_notice_received_async)

    def master_notice_received_async(self):
        self.result_sent_to_master = True

    def reduce_single_map_result(self,reducer, chunk, reduce_id):
        w = self.mappers_list[chunk]
        c = zerorpc.Client()
        c.connect("tcp://"+w[0]+':'+w[1])
        table = c.get_map_table_part(reduce_id)
        c.notice_received()
        #print table
        self.map_result_collect_state[chunk] = 'COLLECTED'
        for e in table:
            reducer.reduce(e[0], e[1])
        #keys = table.keys()
        #for k in keys:
        #    reducer.reduce(k, table[k])


    def encode_a_map_to_list(self, map):
        pass

    def decode_a_list_to_map(self, list):
        pass

    def set_mapper_list(self, mappers_list):
        gevent.spawn(self.set_mapper_list_async, mappers_list)

    def set_mapper_list_async(self,mappers_list):
        for e in mappers_list:
            key = (e[0][0], e[0][1])
            val = (e[1][0], e[1][1])
            self.mappers_list[key] = val
        #print self.mappers_list

    def get_map_table_part(self, reduce_id):
        table = []
        keys = self.map_table.keys()
        #print keys
        each_size = len(keys)/self.num_reducers
        #print 'eachsize = ' + str(each_size)
        begin = (reduce_id-1)*each_size

        if reduce_id == self.num_reducers:
            end = len(keys)
        else:
            end = reduce_id*each_size

        tp = 0
        print begin, end
        for e in self.map_table:
            if tp>=begin and tp < end:
                table.append((e,self.map_table[e]))
            tp += 1
        print table
        return table


    def get_result_list(self):
        list = []
        for e in self.result_list:
            list.append((e,self.result_list[e]))
        return list

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