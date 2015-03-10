__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent

class Worker(object):
    def __init__(self):
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_work(self, nums):
        nums = [int(n) for n in nums]
        gevent.sleep(2)
        return str(sum(nums))

if __name__ == '__main__':
    master_addr = "127.0.0.1:4242"#sys.argv[1];
    worker_ip = "127.0.0.1"
    worker_port = sys.argv[1]
    s = zerorpc.Server(Worker())
    s.bind('tcp://' + worker_ip+":"+worker_port)
    c = zerorpc.Client()
    c.connect("tcp://"+master_addr)
    c.register(worker_ip, worker_port)
    s.run()

    #print socket.gethostbyname(socket.gethostname())