import threading
import time
import random

# Data
LEN_LIST_DATA = 4
# Kumpulan Data yang sudah dilock jika True
list_data = [False for _ in range(LEN_LIST_DATA)]

def lock_x(transaction: int, i_lock: int):
    print("Before Lock Data:", list_data)
    print(f"T{transaction} lock-X ({chr(ord('A')+i_lock)})")
    list_data[i_lock] = True

def unlock_x(transaction: int, i_unlock: int):
    print(f"T{transaction} unlock ({chr(ord('A')+i_unlock)})")
    list_data[i_unlock] = False

# Banyaknya transaksi
NUM_TRANSACTION = 3
# Jika True maka data pada sebuah transaksi sudah pernah dilock
transactions = [[False for _ in range(LEN_LIST_DATA)] for _ in range(NUM_TRANSACTION)]
# Lock data pada sebuah transaction
# Jika True maka data pada sebuah transaksi sedang dilock
lock_data_per_transaction = [[] for _ in range(NUM_TRANSACTION)]

def random_lock_x(transaction):
    # Dilock sebanyak lock_times
    lock_times = random.randint(1, 4)
    # # lock data pada sebuah transaction
    # lock_data = []
    # diiterasi selama lock_times kali
    for _ in range(lock_times):
        # print("Data:", list_data)
        if False not in transactions[transaction]:
            return
        # Cari data yang belum dilock oleh transaksi tersebut dan
        # data belum pernah dilock oleh transaksi tersebut
        while True:
            data_lock = random.randint(0, LEN_LIST_DATA-1)
            if data_lock not in lock_data_per_transaction[transaction] and not transactions[transaction][data_lock]:
                transactions[transaction][data_lock] = True
                break
        # Menunggu untuk lock-X untuk data
        print(f"T{transaction} wait lock-X ({chr(ord('A')+data_lock)})")
        while list_data[data_lock]:
            time.sleep(0.1)
        # Masukkan ke daftar data yang sedang dilock oleh transaksi tersebut
        lock_data_per_transaction[transaction].append(data_lock)
        # Melakukan lock-X
        lock_x(transaction, data_lock)
        time.sleep(3/random.randrange(1, 20))
        # Unlock data dari lock_data secara random sebanyak lock_data
        unlock_times = random.randint(0, len(lock_data_per_transaction[transaction]))
        for _ in range(unlock_times):
            idx_data_unlock = random.randint(0, len(lock_data_per_transaction[transaction])-1)
            data_unlock = lock_data_per_transaction[transaction][idx_data_unlock]
            unlock_x(transaction, data_unlock)
            lock_data_per_transaction[transaction].pop(idx_data_unlock)

    # Unlock seluruh data
    # print(f"T{transaction} Unlock ALL")
    for unlock_remaining_data in lock_data_per_transaction[transaction]:
        unlock_x(transaction, unlock_remaining_data)

def random_times_lock_x(transaction):
    # Memulai transaksi
    print(f"T{transaction} start")
    # Lock random sebanyak 1-2 kali
    times = random.randint(1, 2)
    for _ in range(times):
        random_lock_x(transaction)
    # Commit transaksi
    print(f"T{transaction} commit")

thread_pool = []

for i in range(NUM_TRANSACTION):
    thread_pool.append(threading.Thread(target=random_times_lock_x, args=(i, )))

for i in range(NUM_TRANSACTION):
    thread_pool[i].start()

for i in range(NUM_TRANSACTION):
    thread_pool[i].join()

print("Data:", list_data)
