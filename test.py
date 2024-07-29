import collections
import time
import numpy as np


class RollingMedian:
    def __init__(self, n = 5000):
        self.n = n
        self.data = collections.deque(maxlen=n)
    
    def input(self, value):
        self.data.append(value)
        return self.get_median()
    
    def get_median(self):
        sorted_data = sorted(list(set(self.data)))
        mid = len(sorted_data) // 2
        if len(sorted_data) % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2.0
        else:
            return sorted_data[mid]
        # return np.median(np.unique(self.data))
    
class RollingMedian2:
    def __init__(self, n):
        self.n = n
        self.data = set()
        self.queue = []

    def input(self, value):
        if len(self.queue) == self.n:
            old_value = self.queue.pop(0)
            if old_value not in self.queue:
                self.data.remove(old_value)
        
        self.queue.append(value)
        self.data.add(value)
        
        return self.get_median()

    def get_median(self):
        sorted_data = sorted(self.data)
        length = len(sorted_data)
        if length % 2 == 0:
            return (sorted_data[length//2 - 1] + sorted_data[length//2]) / 2
        else:
            return sorted_data[length//2]


def test_rolling_median():
    print("开始测试 RollingMedian 类...")

    # 基本功能测试
    rm = RollingMedian(5)
    test_data = [1, 3, 5, 2, 4, 6, 7, 8, 9, 10]
    expected_results = [1, 2, 3, 2.5, 3, 4, 5, 6, 7, 8]

    print("\n基本功能测试:")
    for i, val in enumerate(test_data):
        result = rm.input(val)
        print(f"输入: {val}, 输出: {result}, 预期: {expected_results[i]}")
        assert abs(result - expected_results[i]) < 1e-6, f"测试失败: 输入 {val}"

    print("基本功能测试通过")

    # 性能测试
    print("\n性能测试:")
    rm = RollingMedian(5000)
    sizes = [10000, 50000, 100000]
    for size in sizes:
        
        start_time = time.time()
        for _ in range(size):
            rm.input(np.random.rand())
        end_time = time.time()

        print(f"处理 {size} 个随机数据的时间: {end_time - start_time:.4f} 秒")

    print("\n所有测试完成")

# 运行测试
if __name__ == "__main__":
    test_rolling_median()