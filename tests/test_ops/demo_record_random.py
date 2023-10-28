# @Time    : 2022/9/18 18:13
# @Author  : tk
# @FileName: demo_record_random.py


import data_serialize
import tfrecords
from fastdatasets.record import load_dataset,gfile

data_path = gfile.glob('d:/tmp/example_writer.tfrecord0')

options = 'GZIP'
def test_record():
    dataset = load_dataset.RandomDataset(data_path,options=options)

    # dataset = dataset.map(lambda x: x+  b"adasdasdasd")
    # print(len(dataset))
    #
    for i in range(len(dataset)):
        print(i+1,dataset[i])

    print('batch...')
    dataset = dataset.batch(7,drop_remainder=False)
    for i in range(len(dataset)):
        print(i+1,dataset[i])

    print('unbatch...')
    dataset = dataset.unbatch()
    for i in range(len(dataset)):
        print(i+1,dataset[i])

    print('shuffle...')
    dataset = dataset.shuffle(10)
    for i in range(len(dataset)):
        print(i+1,dataset[i])

    print('map...')
    dataset = dataset.map(transform_fn=lambda x:x + b'aa22222222222222222222222222222')
    for i in range(len(dataset)):
        print(i+1,dataset[i])

    print(dataset[0:3])

    print('torch Dataset...')
    from fastdatasets.torch_dataset import Dataset

    d = Dataset(dataset)
    for i in range(len(d)):
        print(i + 1,d[i])

def test_mutiprocess():

    print(data_path)

    dataset = load_dataset.RandomDataset(data_path,options=options)
    dataset0 = dataset.mutiprocess(3, 0)
    dataset1 = dataset.mutiprocess(3, 1)
    dataset2 = dataset.mutiprocess(3, 2)

    print(len(dataset0),len(dataset1),len(dataset2))
    for i in range(len(dataset0)):
        print(i,dataset0[i])

    print()
    for i in range(len(dataset1)):
        print(i,dataset1[i])
    print()
    for i in range(len(dataset2)):
        print(i,dataset2[i])


test_record()
# test_mutiprocess()