from multiprocessing import Process, Pool
import os
import time, random

def log(user_str):
    def wrapper1(func):
        print time.asctime()
        def wrapper(*args, **kwargs):
            print user_str
            print "run wrapper: " + time.asctime()
            return func(*args, **kwargs)
        return wrapper
    return wrapper1

@log("hello")
def run_proc(name):
    print('Run child process %s (%s)...' % (name, os.getpid()))
    print 'Run task %s (%s)...' % (name, os.getpid())
    start = time.time()
    time.sleep(random.random() * 3)
    end = time.time()
    print 'Task %s runs %0.2f seconds.' % (name, (end - start))


if __name__=='__main__':
    print('Parent process %s.' % os.getpid())
    #p = Process(target=run_proc, args=('test',))
   # print('Child process will start.')
    p = Pool(4)
    for i in range(4):
        p.apply_async(run_proc(str(i)))
    p.close()
    p.join()
    print('Child process end.')