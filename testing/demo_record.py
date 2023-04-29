# -*- coding: utf-8 -*-
# @Time    : 2022/9/8 16:30

from fastdatasets.record import RECORD,load_dataset
from fastdatasets import gfile
data_path = gfile.glob('d:/example.tfrecords*')
print(data_path)
options = RECORD.TFRecordOptions(compression_type=None)
base_dataset = load_dataset.IterableDataset(data_path, cycle_length=1, block_length=1, buffer_size=128, options=options, with_share_memory=True)

print(type(base_dataset))
num = 0
for d in base_dataset:
    num +=1
print('base_dataset num',num)
base_dataset.reset()

def test_batch():
    global base_dataset
    base_dataset.reset()
    ds = base_dataset.repeat(2).repeat(2).repeat(3).map(lambda x: x + bytes('_aaaaaaaaaaaaaa', encoding='utf-8'))
    num = 0
    for _ in ds:
        num += 1
    print('repeat(2).repeat(2).repeat(3) num ', num)

    def filter_fn(x):
        if x != b'file2____2':
            return True
        return False
    base_dataset.reset()

    print('filter....')
    dataset = base_dataset.filter(filter_fn)
    i = 0
    for d in dataset:
        i += 1
        print(i,d)


    print('batch...')
    base_dataset.reset()
    dataset = base_dataset.batch(7)
    dataset = dataset.cache(11000)
    i = 0
    for d in dataset:
        i += 1
        print(i,d)
    print('unbatch...')
    base_dataset.reset()
    dataset = dataset.unbatch().cache(2).repeat(2).choice(10,[0,1,2]).repeat(2)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)
def test_skills():
    global base_dataset
    base_dataset.reset()
    dataset = base_dataset
    num = 0
    for _ in dataset:
        num += 1
        print(num,_)

    base_dataset.reset()
    print('skip...')
    dataset = base_dataset.skip(1)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

    print('limit...')
    base_dataset.reset()

    dataset = base_dataset.repeat(2).limit(3)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

def test_mutiprocess():
    print('mutiprocess...')
    base_dataset.reset()
    dataset = base_dataset.mutiprocess(3,0)
    dataset = dataset.repeat(2)
    i = 0
    for d in dataset:
        i += 1
        print(i,d)

    # print('mutiprocess...')
    # base_dataset.reset()
    # dataset = base_dataset.mutiprocess(3,1)
    # i = 0
    # for d in dataset:
    #     i += 1
    #     print(i,d)
    #
    # print('mutiprocess...')
    # base_dataset.reset()
    # dataset = base_dataset.mutiprocess(3,2)
    # i = 0
    # for d in dataset:
    #     i += 1
    #     print(i,d)


# test_batch()

test_skills()
# test_mutiprocess()