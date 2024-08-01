import collections
import time
import random

class RollingMedian:
    def __init__(self, n=10):
        self.n = n
        self.data = collections.deque(maxlen=n)

    def input(self, value):
        if value not in self.data:
            self.data.append(value)

            if len(self.data) == self.n:
                return self.get_median()
        return 0

    def get_median(self):
        sorted_data = sorted(self.data)
        mid = len(sorted_data) // 2

        if len(sorted_data) % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2.0
        else:
            return sorted_data[mid]

def performance_test():
    n = 1000000  # 测试数据大小
    window_size = 10  # 滚动窗口大小
    data = [random.uniform(-0.003, 0.003) for _ in range(n)]  # 生成随机测试数据

    rm = RollingMedian(window_size)
    start_time = time.time()

    for value in data:
        rm.input(value)

    end_time = time.time()
    print(f"Time taken for {n} inputs with window size {window_size}: {end_time - start_time:.6f} seconds")
    print(f"each task took: {(end_time - start_time) / n * 1000:.6f} ms")
    
def test_rolling_median():
    rm = RollingMedian(3)
    assert rm.input(1) == 0
    assert rm.input(2) == 0
    assert rm.input(3) == 2
    assert rm.input(4) == 3
    assert rm.input(5) == 4
    assert rm.input(6) == 5
    assert rm.input(7) == 6
    assert rm.input(8) == 7
    assert rm.input(9) == 8

    rm = RollingMedian(5)
    assert rm.input(1) == 0
    assert rm.input(1) == 0
    assert rm.input(1) == 0
    assert rm.input(1) == 0
    assert rm.input(2) == 0
    assert rm.input(3) == 0
    assert rm.input(4) == 0
    assert rm.input(5) == 3
    assert rm.input(6) == 4
    assert rm.input(7) == 5

    rm = RollingMedian(1)
    assert rm.input(1) == 1
    assert rm.input(2) == 2
    assert rm.input(3) == 3
    assert rm.input(4) == 4
    assert rm.input(5) == 5

    print("All tests passed!")
    
if __name__ == "__main__":
    test_rolling_median()
    performance_test()