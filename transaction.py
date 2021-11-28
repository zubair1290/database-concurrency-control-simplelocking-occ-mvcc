
class RollbackTransaction(RuntimeError):
    pass

class Transaction:

    def __init__(self, f):
        self._f = f

    def start(self):
        self._x = self._f()
        self._v = next(self._x)
        self._d = False

    def process(self, cb):
        if self._d:
            return True
        try:
            self._v = self._x.send(cb(self._v))
            return False
        except StopIteration:
            self._d = True
            return True
