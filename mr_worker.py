__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent

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

    def do_map(self, job_name):
        print 'Doing MAP '+ self.worker_ip + ':'+self.worker_port
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'MAPDONE')

    def do_reduce(self, job_name, mapresults):
        self.c.set_worker_state(self, self.worker_ip, self.worker_port, 'REDUCEDONE')

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