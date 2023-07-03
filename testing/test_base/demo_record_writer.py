# @Time    : 2022/9/18 23:27
# @Author  : tk
# @FileName: demo_record_writer.py

import pickle
import data_serialize
import tfrecords
from fastdatasets.record import load_dataset
from fastdatasets.record import WriterObject,FeatureWriter,StringWriter,PickleWriter,DataType
from tfrecords import TFRecordOptions, TFRecordCompressionType

filename_0 =  r'd:/tmp/example_writer.tfrecord0'
filename_1 =  r'd:/tmp/example_writer.tfrecord1'
filename_2 =  r'd:/tmp/example_writer.tfrecord2'

def test_string(filename):
    

    options = 'GZIP'
    print('test_string ...')
    with StringWriter(filename,options=options) as writer:
        for i in range(2):
            writer.write(b'123' )
    writer.close()


    datasets = load_dataset.IterableDataset(filename,options=options)
    for i,d in enumerate(datasets):
        print(i, d)




def test_pickle(filename):
    print('test_pickle ...')
    with PickleWriter(filename) as writer:
        for i in range(2):
            writer.write(b'test_pickle' + b'123')
    writer.close()

    datasets = load_dataset.RandomDataset(filename)
    datasets = datasets.map(lambda x: pickle.loads(x))
    for i in range(len(datasets)):
        print(i, datasets[i])

def test_feature(filename):
    print('test_feature ...')
    with FeatureWriter(filename) as writer:
        for i in range(5):
            feature = {
                'input_ids': {
                    'dtype': DataType.int64_list,
                    'data': list(range(i + 1))
                },
                'seg_ids': {
                    'dtype': DataType.float_list,
                    'data': [i,0,1,2,3]
                },
                'other':{
                    'dtype': DataType.bytes_list,
                    'data': [b'aaa',b'bbbc1']
                },
            }
            writer.write(feature)
    writer.close()

    datasets = load_dataset.RandomDataset(filename)
    for i in range(len(datasets)):
        example = data_serialize.Example()
        example.ParseFromString(datasets[i])
        feature = example.features.feature
        print(feature)
        #print(feature)

test_string(filename_0)
# test_pickle(filename_1)
# test_feature(filename_2)

