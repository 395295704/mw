#!/usr/bin/python
#-*-coding:utf-8-*-
import time, multiprocessing, signal, threading, random, time, Queue

class Master(multiprocessing.Process):
    def __init__(self):
        super(Master,self).__init__();
        signal.signal(signal.SIGTERM, self.handler);
	#这个变量要传入线程用于控制线程运行，为什么用dict？充分利用线程间共享资源的特点
	#因为可变对象按引用传递，标量是传值的，不信写成self.live = true试试
        self.live = {'stat':True};                   

    def handler(self, signum, frame):
        print 'signal:',signum;
        self.live['stat'] = 0;                                   #置这个变量为0，通知子线程可以“收工”了

    def run(self):
        print 'PID:',self.pid;     
        cond = threading.Condition(threading.Lock());            #创建一个condition对象，用于子线程交互
        q = Queue.Queue();                                       #一个队列
        sender = Sender(cond, self.live, q);                     #传入共享资源
        geter = Geter(cond, self.live, q);
        sender.start();                                          #启动线程
        geter.start();
        signal.pause();                                          #主线程睡眠并等待信号
        while threading.activeCount()-1:                         #主线程收到信号并被唤醒后，检查还有多少线程活着（除掉自己）
            time.sleep(2);                                       #再睡眠等待，确保子线程都安全的结束
            print 'checking live', threading.activeCount();
        print 'mater gone';

class Sender(threading.Thread):
    def __init__(self, cond, live, queue):
        super(Sender, self).__init__(name='sender');
        self.cond = cond;
        self.queue = queue;
        self.live = live

    def run(self):
        cond = self.cond;
        while self.live['stat']:                                 #检查这个进程内的“全局”变量，为真就继续运行
            cond.acquire();                                      #获得锁，以便控制队列
            i = random.randint(0,100);
            self.queue.put(i,False);
            if not self.queue.full():
                print 'sender add:',i;
            cond.notify();                                       #唤醒等待锁的其他线程
            cond.release();                                      #释放锁
            time.sleep(random.randint(1,3));
        print 'sender done'

class Geter(threading.Thread):
    def __init__(self, cond, live, queue):
        super(Geter, self).__init__(name='geter');
        self.cond = cond;
        self.queue = queue;
        self.live = live

    def run(self):
        cond = self.cond;
        while self.live['stat']:
            cond.acquire();
            if not self.queue.empty():
                i = self.queue.get();
                print 'geter get:',i;
            cond.wait(3);
            cond.release();
            time.sleep(random.randint(1,3));
        print 'geter done'

if __name__ == '__main__':

    master = Master();
    master.start();                                              #启动子进程
