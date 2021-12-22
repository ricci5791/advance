import cv2 as cv
import numpy as np
from timeit import default_timer as timer

from numba import cuda

# sum_reduce = cuda.Reduce(lambda a, b: a + b)
min_reduce = cuda.Reduce(lambda a, b: min(a + b))


@cuda.reduce
def sum_reduce(a, b):
    return a + b

def test_sum_reduce(A):
    start = timer()
    # expect = A.sum()
    end = timer()
    print(end - start)

    start = timer()
    got = sum_reduce(A)
    end = timer()
    print(end - start)
    print(got)


def test_min_reduce(A):
    max_reduce = cuda.Reduce(lambda a, b: min(a, b))
    got = max_reduce(A, init=0)
    print(got)


if __name__ == '__main__':
    src = cv.imread('./20210814_104926.jpg')
    red_channel = np.array(src[:, :, 2].flatten(), dtype=np.int32)

    x_device = cuda.to_device(red_channel)

    test_sum_reduce(x_device)
    test_min_reduce(x_device)
