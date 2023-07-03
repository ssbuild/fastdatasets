# -*- coding: utf-8 -*-
# @Time    : 2023/4/28 13:21
import os
import random

from fastdatasets.record import WriterObject,FeatureWriter,StringWriter,PickleWriter,DataType
from fastdatasets.record import load_dataset,gfile


all_num = 0
def test_string(base_path,file_no):
    global all_num
    filename = os.path.join(base_path,str(file_no))
    with StringWriter(filename) as writer:
        N = random.randint(5,100)
        for i in range(N):
            writer.write('file_{} '.format(file_no) + str(i))
            all_num += 1

base_path = r'd:/test'
if not os.path.exists(base_path):
    os.mkdir(base_path)


for i in range(6):
    test_string(base_path,i)

data_path = gfile.glob('d:/test/*')
data_path = [file for file in data_path if not file.find('INDEX') != -1]
print(data_path)




# def test_random():
#     dataset = load_dataset.RandomDataset(data_path,options='GZIP')
#     for i in range(len(dataset)):
#         print(dataset[i])
#     print(len(dataset))

def test_iter():
    dataset = load_dataset.IterableDataset(data_path,options='GZIP')
    idx = 0

    dataset = dataset.repeat(2)
    for item in dataset:
        print(idx, item)
        idx += 1
    print('total',idx)


# test_random()
test_iter()

print('total',all_num)