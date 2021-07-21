from itts import ITTS , SliceStatus
from redis import Redis
import random
import string
import time


def create_random_itts():
    random_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return ITTS(Redis(), f'tl::{random_key}')


def all_same_status(n, randomize_status):
    t = create_random_itts()

    ids = list(range(n))
    random.shuffle(ids)
    t0 = time.time()

    for i in ids:
        if randomize_status:
            status = random.choice(list(SliceStatus))
        else:
            status = SliceStatus.DONE

        t.insert_slice(timestamp=i, status=status)

    td = time.time() - t0

    print(f'[requests: {n} {randomize_status=}] total time: {td:.2f} seconds | rate: {n/td:.2f} requests/second')

def main():
    all_same_status(n=20_000, randomize_status=False)
    all_same_status(n=20_000, randomize_status=True)

if __name__ == '__main__':
    main()
