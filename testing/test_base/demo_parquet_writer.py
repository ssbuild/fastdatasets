# -*- coding: utf-8 -*-
# @Time:  19:51
# @Author: tk
# @File：demo_arrow_writer


from fastdatasets.parquet.writer import PythonWriter
from fastdatasets.parquet.dataset import load_dataset
from tfrecords.python.io.arrow import ParquetReader,arrow


path_file = 'd:/tmp/data.parquet'



def test_write():
    fs = PythonWriter(path_file,
                        schema={'id': 'int32','text': 'str','text2': 'str'},
                        parquet_options=dict(write_batch_size = 10))
    for i in range(3):
        data = {
            "id": list(range(i * 5,(i+ 1) * 5)),
            'text': ['asdasdasdas' + str(i) for i in range(5)],
            'text2': ['asdasdasdas3asdadas' + str(i) for i in range(5)]
        }
        # fs.write_batch(data.keys(),data.values())
        fs.write_table(data.keys(),data.values())


    fs.close()

def test_random():
    dataset = load_dataset.RandomDataset(path_file)
    print('total', len(dataset))
    for i in range(len(dataset)):
        print(dataset[i])



def test_read_iter():
    dataset = load_dataset.IterableDataset(path_file,batch_size=1)
    for d in dataset:
        print('iter',d)


test_write()

test_random()

test_read_iter()