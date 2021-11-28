import threading
import time
import random

len_list_data = 4
list_data = [False for _ in range(len_list_data)]
def lock_x(transaction: threading.Thread, i):
    print("Before Lock Data:", list_data)
    print(f"T{transaction} lock-X ({chr(ord('A')+i)})")
    list_data[i] = True

def unlock_x(transaction: threading.Thread, i):
    print(f"T{transaction} unlock ({chr(ord('A')+i)})")
    list_data[i] = False

num_transaction = 3
transactions = [[False for _ in range(len_list_data)] for _ in range(num_transaction)]
def random_lock_x(transaction):
    lock_times = random.randint(1, 4)
    
    lock_data = []
    for _ in range(lock_times):
        # print("Data:", list_data)
        if False not in transactions[transaction]:
            return
        while True:
            data = random.randint(0, len_list_data-1)
            if data not in lock_data and transactions[transaction][data] == False:
                transactions[transaction][data] = True
                break
        lock_data.append(data)
        print(f"T{transaction} wait lock-X ({chr(ord('A')+data)})")
        while list_data[data] == True:
            time.sleep(0.1)
        lock_x(transaction, data)
        time.sleep(3/random.randrange(1, 20))

        unlock_times = random.randint(0, len(lock_data))
        for _ in range(unlock_times):
            idx_data_unlock = random.randint(0, len(lock_data)-1)
            data_unlock = lock_data[idx_data_unlock]
            unlock_x(transaction, data_unlock)
            lock_data.pop(idx_data_unlock)
    # print(f"T{transaction} Unlock ALL")
    for data in lock_data:
        unlock_x(transaction, data)
    
def random_times_lock_x(transaction):
    print(f"T{transaction} start")
    times = random.randint(1, 2)
    for _ in range(times):
        random_lock_x(transaction)
    print(f"T{transaction} commit")

thread_pool = []

for i in range(num_transaction):
    thread_pool.append(threading.Thread(target=random_times_lock_x, args=(i, )))

for i in range(num_transaction):
    thread_pool[i].start()

for i in range(num_transaction):
    thread_pool[i].join()
    
print("Data:", list_data)