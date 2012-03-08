import Queue
import logging
import threading

logger = logging.getLogger(__name__)

class Worker(threading.Thread):
    def __init__(self, tasks):
        threading.Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            f, args, kargs = self.tasks.get()
            try:
                f(*args, **kargs)
            except Exception:
                logger.exception("threadpool exception")
            finally:
                self.tasks.task_done()

class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue.Queue(num_threads)
        self.threads = []

        for t in xrange(num_threads):
            self.threads.append(Worker(self.tasks))

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def wait(self):
        self.tasks.join()
