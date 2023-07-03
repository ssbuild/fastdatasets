# -*- coding: utf-8 -*-
# @Time    : 2022/9/19 11:20
import data_serialize
from fastdatasets.memory import load_dataset


def test_iterator():
    data = [iter(range(10)), iter(range(10))]

    base_dataset = load_dataset.IterableDataset(data,
                                                cycle_length=1, block_length=1, buffer_size=128)

    for i, d in enumerate(base_dataset):
        print(i, d)
    print('shuffle...')
    base_dataset.reset()
    base_dataset = base_dataset.shuffle(10)
    for i, d in enumerate(base_dataset):
        print(i, d)


def test_list():
    data_path = [list(range(15)),list(range(15))]

    dataset = load_dataset.RandomDataset(data_path)

    for i in range(len(dataset)):
        print(i + 1, dataset[i])

    dataset = dataset.map(lambda x: str(x) +  "asdsadasaaaaaaaa")
    print(len(dataset))

    for i in range(len(dataset)):
        print(i + 1, dataset[i])

    print('batch...')
    dataset = dataset.batch(7)
    for i in range(len(dataset)):
        print(i + 1, dataset[i])

    print('unbatch...')
    dataset = dataset.unbatch()
    for i in range(len(dataset)):
        print(i + 1, dataset[i])

    print('shuffle...')
    dataset = dataset.shuffle(10)
    for i in range(len(dataset)):
        print(i + 1, dataset[i])



    #
    # print('map...')
    # dataset = dataset.map(transform_fn=lambda x: x + b'aa22222222222222222222222222222')
    # for i in range(len(dataset)):
    #     print(i + 1, dataset[i])

    print('torch Dataset...')
    from fastdatasets.torch_dataset import Dataset

    d = Dataset(dataset)
    for i in range(len(d)):
        print(i + 1, d[i])



test_iterator()
test_list()