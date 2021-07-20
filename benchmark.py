from time_lord import TimeLord, SliceStatus
from redis import Redis
import random
import string
import time


def create_random_timelord():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return TimeLord(Redis(), f'tl::{random_key}')


def all_same_status():
    t = create_random_timelord()
    n = 20_000

    ids = list(range(n))
    random.shuffle(ids)

    print('inserting...')
    t0 = time.time()
    for i in ids:
        #status = random.choice(list(SliceStatus))
        status = SliceStatus.DONE
        t.insert_slice(timestamp=i, status=status)

    td = time.time() - t0

    print(f'done. total time: {td} requests per second: {n/td}')

    t0 = time.time()
    t.insert_slice(timestamp=n+1, status=status)
    td = time.time() - t0
    print(f'done. lst insert time: {td*1000} ms')

def main():
    all_same_status()

if __name__ == '__main__':
    main()
