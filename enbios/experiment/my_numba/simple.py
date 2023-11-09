from numba import njit
import random


@njit(parallel=True)
def numba_monte_carlo_pi(nsamples):
    acc = 0
    for i in range(nsamples):
        x = random.random()
        y = random.random()
        if (x ** 2 + y ** 2) < 1.0:
            acc += 1
    return 4.0 * acc / nsamples


def monte_carlo_pi(nsamples):
    acc = 0
    for i in range(nsamples):
        x = random.random()
        y = random.random()
        if (x ** 2 + y ** 2) < 1.0:
            acc += 1
    return 4.0 * acc / nsamples


def timer(funct, *args, **kwargs):
    import time
    start = time.time()
    funct(*args, **kwargs)
    end = time.time()
    # to minutes/seconds
    return end - start

num = 100_000_000
print(timer(numba_monte_carlo_pi, num))
print(timer(monte_carlo_pi, num))
# 1.5312976837158203
# 32.987468242645264
