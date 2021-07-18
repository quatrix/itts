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
    n = 100_000

    print('inserting...')
    t0 = time.time()
    for i in range(n):
        status = random.choice([SliceStatus.DONE, SliceStatus.PENDING])
        # status = SliceStatus.DONE
        t.insert_slice(id=i, timestamp=i, status=status)

    print('done', time.time() - t0)



def main():
    all_same_status()

if __name__ == '__main__':
    main()
