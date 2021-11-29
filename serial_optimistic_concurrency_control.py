from transaction import Transaction, RollbackTransaction
import random


OP_READ = 1
OP_WRITE = 2
OP_COMMIT = 3
OP_VALIDATE = 4


def do_transaction(*t):

    data = {
        'A': 0,
        'B': 0,
        'C': 0,
        'D': 0,
    }

    ts = 0
    m = {i: x for i, x in enumerate(t)}
    tts = {i: None for i in range(len(t))}
    rw = {i: (set(), set()) for i in range(len(t))}

    for v in m.values():
        v.start()

    val = False

    def cb(i):
        nonlocal val
        def f(v):
            nonlocal val
            op, args = v
            if op == OP_READ:
                print(f'{ts}: Transaction {m[i].name} reads {args[0]}')
                rw[i][0].add(args[0])
                return data[args[0]]
            elif op == OP_WRITE:
                print(f'{ts}: Transaction {m[i].name} writes {args[0]}')
                rw[i][1].add(args[0])
                data[args[0]] = args[1]
            elif op == OP_COMMIT:
                print(f'{ts}: Transaction {m[i].name} commits')
                pass
            elif op == OP_VALIDATE:
                print(f'{ts}: Transaction {m[i].name} validates')
                t = tts[i]
                t[1] = ts
                for k, x in tts.items():
                    if x is None or x[2] is None or x[1] >= ts:
                        continue
                    if x[2] >= t[0] and \
                            not (t[0] < x[2] < ts and \
                            rw[k][1].isdisjoint(rw[i][0])):
                        raise RollbackTransaction()
                val = True
        return f

    while m:
        i = random.sample(m.keys(), 1)[0]
        x = m[i]
        try:
            if tts[i] is None:
                tts[i] = [ts, None, None]
            if x.process(cb(i)):
                tts[i][2] = ts
                del m[i]
            if val:
                # Write is atomic
                ts += 1
                while not x.process(cb(i)):
                    ts += 1
                tts[i][2] = ts
                del m[i]
        except RollbackTransaction:
            print(f'{ts}: Transaction {x.name} rollbacks')
            temp = rw[i]
            temp[0].clear()
            temp[1].clear()
            val = False
            x.start()
            ts += 1
            tts[i] = [ts, None, None]
            while not val:
                if x.process(cb(i)):
                    tts[i][2] = ts
                    del m[i]
                    break
                ts += 1
            continue
        ts += 1

    print('After:')
    print(data)

def main():
    random.seed(0xc12af7ed)

    def T1():
        a = (yield (OP_READ, ('A',)))
        b = (yield (OP_READ, ('B',)))
        a -= 1
        b += 1
        yield (OP_VALIDATE, ())
        yield (OP_WRITE, ('A', a))
        yield (OP_WRITE, ('B', b))
        yield (OP_COMMIT, ())

    def T2():
        a = (yield (OP_READ, ('A',)))
        b = (yield (OP_READ, ('B',)))
        a -= 5
        b *= 2
        yield (OP_VALIDATE, ())
        yield (OP_WRITE, ('A', a))
        yield (OP_WRITE, ('B', b))
        yield (OP_COMMIT, ())

    def T3():
        d = (yield (OP_READ, ('D',)))
        d += 1
        yield (OP_VALIDATE, ())
        yield (OP_WRITE, ('C', 5))
        yield (OP_WRITE, ('D', d))
        yield (OP_COMMIT, ())

    def T4():
        b = (yield (OP_READ, ('B',)))
        yield (OP_VALIDATE, ())
        yield (OP_WRITE, ('D', b))
        yield (OP_COMMIT, ())

    do_transaction(
        Transaction('T1', T1),
        Transaction('T2', T2),
        Transaction('T3', T3),
        Transaction('T4', T4),
    )

if __name__ == '__main__':
    main()
