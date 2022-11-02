# coding: utf-8
"""
雪花算法: SnowFlake是Twitter公司采用的一种算法，目的是在分布式系统中产生全局唯一且趋势递增的ID。

id组成部分: 64bit
    0(1bit-符号位) - 00000000 00000000 00000000 00000000 00000000 0(41bit-时间戳)- 00000000 00(10bit-机器id)- 00000000 0000(12bit-序列号)
具体说明:
    1. 第一位永远是0,符号位,标识正数
    2. 时间戳 占用41bit,精确到毫秒,总共可容纳(2 ** 41 / 1000 / 60 / 60 / 24 / 365) ~= 69年.
    3. 机器id 占用10bit,其中高位5bit是数据中心ID,低位5bit是工作节点ID,做多可以容纳1024个节点.
    4. 序列号 占用12bit,每个节点每毫秒0开始不断累加,最多可以累加到4095,一共可以产生4096个ID.

SnowFlake算法在同一毫秒内最多可以生成: = 1024 X 4096 = 4194304  全局唯一ID

扩展-分布式ID生成的其他方案:
UUID: 唯一随机36位字符串（32个字符串+4个“-”）的算法。它可以保证唯一性，且据说够用N亿年，但是其业务可读性差，无法有序递增。
UidGenerator: UidGenerator是百度开源的分布式ID生成器，其基于雪花算法实现。https://github.com/baidu/uid-generator/blob/master/README.zh_cn.md
Leaf: Leaf是美团开源的分布式ID生成器，能保证全局唯一，趋势递增，但需要依赖关系数据库、Zookeeper等中间件。 https://tech.meituan.com/MT_Leaf.html
"""
import datetime
import time


class InvalidSystemClock(Exception):
    """
    时钟回拨异常
    """
    pass


# 64位ID的划分
WORKER_ID_BITS = 5
DATACENTER_ID_BITS = 5
SEQUENCE_BITS = 12

# 最大取值计算
MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)  # 2**5-1 0b11111
MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)

# 移位偏移计算
WORKER_ID_SHIFT = SEQUENCE_BITS
DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

# 序号循环掩码
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

# 开始时间截 (2017-01-01)
StartTimeStamp = int(datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').timestamp() * 1000)


class IdWorker(object):
    """
    :用于生成IDs
    """

    def __init__(self, datacenter_id, worker_id, sequence=0):
        """
        初始化
        :param datacenter_id: 数据中心（机器区域）ID
        :param worker_id: 机器ID
        :param sequence: 其实序号
        """
        # sanity check
        if worker_id > MAX_WORKER_ID or worker_id < 0:
            raise ValueError('worker_id值越界')

        if datacenter_id > MAX_DATACENTER_ID or datacenter_id < 0:
            raise ValueError('datacenter_id值越界')

        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = sequence

        self.last_timestamp = -1  # 上次计算的时间戳

    def _gen_timestamp(self):
        """
        生成整数时间戳
        :return:int timestamp
        """
        return int(time.time() * 1000)

    def get_id(self):
        """
        获取新ID
        :return:
        """
        timestamp = self._gen_timestamp()

        # 时钟回拨
        if timestamp < self.last_timestamp:
            raise InvalidSystemClock

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK
            if self.sequence == 0:
                # 同一毫秒的序列号已用完
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        new_id = ((timestamp - StartTimeStamp) << TIMESTAMP_LEFT_SHIFT) | (self.datacenter_id << DATACENTER_ID_SHIFT) | \
                 (self.worker_id << WORKER_ID_SHIFT) | self.sequence
        return new_id

    def _til_next_millis(self, last_timestamp):
        """
        等到下一毫秒
        """
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp


if __name__ == '__main__':
    worker = IdWorker(0, 2)
    print(worker.get_id())

