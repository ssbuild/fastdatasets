# @Time    : 2022/9/18 23:27
# @Author  : tk
# @FileName: demo_record_writer.py
import os.path
import pickle
import data_serialize
from fastdatasets.record import load_dataset
from fastdatasets.record import WriterObject,FeatureWriter,StringWriter,PickleWriter,DataType

def test_string(base_path,file_no):
    print('test_string ...')
    filename = os.path.join(base_path,str(file_no))
    with StringWriter(filename) as writer:
        for i in range(50):
            writer.write('file_{} '.format(file_no)  + str(i))



base_path = r'd:\\test'
if not os.path.exists(base_path):
    os.mkdir(base_path)


for i in range(20):
    test_string(base_path,i)

