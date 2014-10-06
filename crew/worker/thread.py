import threading
import ctypes
import time


class KillableThread(threading.Thread):

    def kill(self, exc=SystemExit):
        if not self.isAlive():
            return False

        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self.ident), ctypes.py_object(exc))

        if res == 0:
            raise ValueError("nonexistent thread id")
        elif res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(self.ident, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

        while self.isAlive():
            time.sleep(0.01)

        return True
