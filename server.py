import socket
import signal
import logging
import random
import multiprocessing
from multiprocessing import Queue
from typing import List


logging.basicConfig(format="[%(levelname)s] %(asctime)s: %(message)s")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Worker():
    def __init__(self, q: Queue, p: multiprocessing.Process) -> None:
        self.queue = q
        self.process = p
            

class Server:
    def __init__(self, host: str, port: int):
        self.min_workers = 1
        self.host = host
        self.port = port
        self.socket = socket.socket()
        self.socket.bind((host, port))
        self.socket.listen(10)
        self.workers: List[Worker] = []

    def __shutdown(self):
        for worker in self.workers:
            worker.process.terminate()
        self.socket.close()
        self.running = False

    def __handle_signals(self, signum, c):
        logger.info(f"server recieved signal {signum}")
        match signum:
            case signal.SIGINT:
                self.__shutdown()
            case signal.SIGTERM:
                self.__shutdown()
            case signal.SIGUSR1:
                logger.debug("reloading server")
            case _:
                logger.debug("don't know how to handle")

    def run(self):
        signal.signal(signal.SIGINT, self.__handle_signals)
        logger.debug("spawn some worker")
        self.spawn_worker()

        logger.info("waiting for connection")

        self.running = True
        while self.running:
            try:
                client, addr = self.socket.accept()
            except OSError:
                break
            
            logger.debug(f"client with address {addr} connected")
            worker = self.workers[random.randint(0, len(self.workers)-1)]
            worker.queue.put(client)

        logger.info("server is shutting down")

    def spawn_worker(self):
        for i in range(len(self.workers), self.min_workers):
            q = Queue()
            p = multiprocessing.Process(target=run_worker, args=(q,))
            p.name = f"Worker {i}"

            worker = Worker(q, p)
            self.workers.append(worker)
            p.start()

def run_worker(queue: Queue):
    logger.info("Worker is spawnned, waiting for connections")
    while True:
        conn: socket.socket = queue.get()
        try:
            data = conn.recv(1024)
            conn.send(data)
        except Exception as exc:
            logger.error(exc)
            break
        finally:
            logger.info(f"Connection {conn.getpeername()} closed")
            conn.close()
            


