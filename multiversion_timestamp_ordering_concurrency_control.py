from transaction import Transaction, RollbackTransaction
import random


OP_READ = 1
OP_WRITE = 2
OP_COMMIT = 3

class Record:
    __slots__ = ('v', 'rt', 'wt')

    def __init__(self, *a):
        if len(a) == 1:
            o = a[0]
            self.v = o.v
            self.rt = o.rt
            self.wt = o.wt
        elif len(a) == 3:
            self.v, self.rt, self.wt = a
        else:
            raise TypeError('__init__ requires either 1 or 3 arguments')

    def __str__(self):
        return f'({self.v!s}, {self.rt!s}, {self.wt!s})'

    def __repr__(self):
        return f'Record({self.v!r}, {self.rt!r}, {self.wt!r})'

def do_transaction(*t):

    data = {
        'A': [Record(0, -1, -1)],
        'B': [Record(0, -1, -1)],
        'C': [Record(0, -1, -1)],
        'D': [Record(0, -1, -1)],
    }

    ts = 0
    m = {i: x for i, x in enumerate(t)}
    tts = {i: None for i in range(len(t))}

    def cb(i):
        def f(v):
            op, args = v
            if op == OP_READ:
                print(f'Transaction {i} reads {args[0]}')
                t = tts[i]
                for x in data[args[0]]:
                    if x.wt > t:
                        break
                    r = x
                r.rt = max(r.rt, t)
                return r.v
            elif op == OP_WRITE:
                print(f'Transaction {i} writes {args[0]}')
                t = tts[i]
                l = data[args[0]]
                for x in l:
                    if x.wt > t:
                        break
                    r = x
                if t < r.rt:
                    raise RollbackTransaction()
                if t == r.wt:
                    r.v = args[1]
                else:
                    r = Record(r)
                    r.wt = t
                    r.v = args[1]
                    l.append(r)
            elif op == OP_COMMIT:
                print(f'Transaction {i} commits')
                pass
        return f

    while m:
        i = random.sample(m.keys(), 1)[0]
        x = m[i]
        try:
            if tts[i] is None:
                tts[i] = ts
                ts += 1
                x.start()
            if x.process(cb(i)):
                del m[i]
        except RollbackTransaction:
            print(f'Transaction {i} rollbacks')
            t = tts[i]
            for l in data.values():
                for i_ in range(len(l)):
                    if l[i_].wt == t:
                        del l[i_]
                        break
            tts[i] = None

    print('After:')
    print(data)

def main():
    random.seed(0xc12af7ed)

    def T1():
        a = (yield (OP_READ, ('A',)))
        b = (yield (OP_READ, ('B',)))
        a -= 1
        b += 1
        yield (OP_WRITE, ('A', a))
        yield (OP_WRITE, ('B', b))
        yield (OP_COMMIT, ())

    def T2():
        a = (yield (OP_READ, ('A',)))
        b = (yield (OP_READ, ('B',)))
        a -= 5
        b *= 2
        yield (OP_WRITE, ('A', a))
        yield (OP_WRITE, ('B', b))
        yield (OP_COMMIT, ())

    do_transaction(Transaction(T1), Transaction(T2))

if __name__ == '__main__':
    main()
