
## The update statement 

```text
2023-07-02: support arrow parquet
2023-04-28: fix lmdb mutiprocess
2023-02-13: add TopDataset with iterable_dataset and patch
2022-12-07: modify a bug for randomdataset for batch reminder
2022-11-07: add numpy writer and parser,add memory writer and parser
2022-10-29: add kv dataset 
2022-10-19: update and modify for __all__ module
```

## usage
  [numpy_io](https://github.com/ssbuild/numpy_io) 

## Install
```commandline
pip install -U fastdatasets
```


### 1. Record Write

```python
import data_serialize
from fastdatasets.record import load_dataset, gfile,TFRecordOptions, TFRecordCompressionType, TFRecordWriter

# Example Features结构兼容tensorflow.dataset
def test_write_featrue():
    options = 'GZIP'

    def test_write(filename, N=3, context='aaa'):
        with TFRecordWriter(filename, options=options) as file_writer:
            for _ in range(N):
                val1 = data_serialize.Int64List(value=[1, 2, 3] * 20)
                val2 = data_serialize.FloatList(value=[1, 2, 3] * 20)
                val3 = data_serialize.BytesList(value=[b'The china', b'boy'])
                featrue = data_serialize.Features(feature=
                {
                    "item_0": data_serialize.Feature(int64_list=val1),
                    "item_1": data_serialize.Feature(float_list=val2),
                    "item_2": data_serialize.Feature(bytes_list=val3)
                }
                )
                example = data_serialize.Example(features=featrue)
                file_writer.write(example.SerializeToString())

    test_write('d:/example.tfrecords0', 3, 'file0')
    test_write('d:/example.tfrecords1', 10, 'file1')
    test_write('d:/example.tfrecords2', 12, 'file2')


# 写任意字符串
def test_write_string():
    options = 'GZIP'

    def test_write(filename, N=3, context='aaa'):
        with TFRecordWriter(filename, options=options) as file_writer:
            for _ in range(N):
                # x, y = np.random.random(), np.random.random()
                file_writer.write(context + '____' + str(_))

    test_write('d:/example.tfrecords0', 3, 'file0')
    test_write('d:/example.tfrecords1', 10, 'file1')
    test_write('d:/example.tfrecords2', 12, 'file2')



```

### 2. record Simple Writer Demo

```python
# @Time    : 2022/9/18 23:27
import pickle
import data_serialize
import numpy as np
from fastdatasets.record import load_dataset
from fastdatasets.record import RECORD, WriterObject,FeatureWriter,StringWriter,PickleWriter,DataType,NumpyWriter

filename= r'd:\\example_writer.record'

def test_writer(filename):
    print('test_feature ...')
    options = RECORD.TFRecordOptions(compression_type='GZIP')
    f = NumpyWriter(filename,options=options)

    values = []
    n = 30
    for i in range(n):
        train_node = {
            "index": np.asarray(i, dtype=np.int64),
            'image': np.random.rand(3, 4),
            'labels': np.random.randint(0, 21128, size=(10), dtype=np.int64),
            'bdata': np.asarray(b'11111111asdadasdasdaa')
        }

        values.append(train_node)
        if (i + 1) % 10000 == 0:
            f.write_batch( values)
            values.clear()
    if len(values):
        f.write_batch(values)
    f.close()

def test_iterable(filename):
    options = RECORD.TFRecordOptions(compression_type='GZIP')
    datasets = load_dataset.IterableDataset(filename, options=options).parse_from_numpy_writer()
    for i, d in enumerate(datasets):
        print(i, d)

def test_random(filename):
    options = RECORD.TFRecordOptions(compression_type='GZIP')
    datasets = load_dataset.RandomDataset(filename, options=options).parse_from_numpy_writer()
    print(len(datasets))
    for i in range(len(datasets)):
        d = datasets[i]
        print(i, d)

test_writer(filename)
test_iterable(filename)
```

### 3. IterableDataset demo

```python
import data_serialize
from fastdatasets.record import load_dataset, gfile, RECORD

data_path = gfile.glob('d:/example.tfrecords*')
options = RECORD.TFRecordOptions(compression_type=None)
base_dataset = load_dataset.IterableDataset(data_path, cycle_length=1,
                                            block_length=1,
                                            buffer_size=128,
                                            options=options,
                                            with_share_memory=True)


def test_batch():
    num = 0
    for _ in base_dataset:
        num += 1
    print('base_dataset num', num)

    base_dataset.reset()
    ds = base_dataset.repeat(2).repeat(2).repeat(3).map(lambda x: x + bytes('_aaaaaaaaaaaaaa', encoding='utf-8'))
    num = 0
    for _ in ds:
        num += 1

    print('repeat(2).repeat(2).repeat(3) num ', num)


def test_torch():
    def filter_fn(x):
        if x == b'file2____2':
            return True
        return False

    base_dataset.reset()
    dataset = base_dataset.filter(filter_fn).interval(2, 0)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

    base_dataset.reset()
    dataset = base_dataset.batch(3)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

    # torch.utils.data.IterableDataset
    from fastdatasets.torch_dataset import IterableDataset
    dataset.reset()
    ds = IterableDataset(dataset=dataset)
    for d in ds:
        print(d)


def test_mutiprocess():
    print('mutiprocess 0...')
    base_dataset.reset()
    dataset = base_dataset.shard(num_shards=3, index=0)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

    print('mutiprocess 1...')
    base_dataset.reset()
    dataset = base_dataset.shard(num_shards=3, index=1)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

    print('mutiprocess 2...')
    base_dataset.reset()
    dataset = base_dataset.shard(num_shards=3, index=2)
    i = 0
    for d in dataset:
        i += 1
        print(i, d)

```



### 4. RandomDataset demo

```python
from fastdatasets.record import load_dataset, gfile, RECORD

data_path = gfile.glob('d:/example.tfrecords*')
options = RECORD.TFRecordOptions(compression_type=None)
dataset = load_dataset.RandomDataset(data_path, options=options,
                                     with_share_memory=True)

dataset = dataset.map(lambda x: x + b"adasdasdasd")
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

print('map...')
dataset = dataset.map(transform_fn=lambda x: x + b'aa22222222222222222222222222222')
for i in range(len(dataset)):
    print(i + 1, dataset[i])

print('torch Dataset...')
from fastdatasets.torch_dataset import Dataset

d = Dataset(dataset)
for i in range(len(d)):
    print(i + 1, d[i])


```



### 5. leveldb dataset

```python
# @Time    : 2022/10/27 20:37
# @Author  : tk
import numpy as np
from tqdm import tqdm
from fastdatasets.leveldb import DB,load_dataset,WriterObject,DataType,StringWriter,JsonWriter,FeatureWriter,NumpyWriter

db_path = 'd:\\example_leveldb_numpy'

def test_write(db_path):
    options = DB.LeveldbOptions(create_if_missing=True,error_if_exists=False)
    f = NumpyWriter(db_path, options = options)
    keys,values = [],[]
    n = 30
    for i in range(n):
        train_node = {
            "index":np.asarray(i,dtype=np.int64),
            'image': np.random.rand(3,4),
            'labels': np.random.randint(0,21128,size=(10),dtype=np.int64),
            'bdata': np.asarray(b'11111111asdadasdasdaa')
        }
        keys.append('input{}'.format(i))
        values.append(train_node)
        if (i+1) % 10000 == 0:
            f.put_batch(keys,values)
            keys.clear()
            values.clear()
    if len(keys):
        f.put_batch(keys, values)
        
    f.get_writer.put('total_num',str(n))
    f.close()



def test_random(db_path):
    options = DB.LeveldbOptions(create_if_missing=False, error_if_exists=False)
    dataset = load_dataset.RandomDataset(db_path,
                                        data_key_prefix_list=('input',),
                                        num_key='total_num',
                                        options = options)

    dataset = dataset.parse_from_numpy_writer().shuffle(10)
    print(len(dataset))
    for i in tqdm(range(len(dataset)),total=len(dataset)):
        d = dataset[i]
        print(i,d)

test_write(db_path)
test_random(db_path)

```


### 6. lmdb dataset

```python
# @Time    : 2022/10/27 20:37
# @Author  : tk

import numpy as np
from tqdm import tqdm
from fastdatasets.lmdb import DB,LMDB,load_dataset,WriterObject,DataType,StringWriter,JsonWriter,FeatureWriter,NumpyWriter

db_path = 'd:\\example_lmdb_numpy'

def test_write(db_path):
    options = DB.LmdbOptions(env_open_flag = 0,
                env_open_mode = 0o664, # 8进制表示
                txn_flag = 0,
                dbi_flag = 0,
                put_flag = 0)

    f = NumpyWriter(db_path, options = options,map_size=1024 * 1024 * 1024)

    keys, values = [], []
    n = 30
    for i in range(n):
        train_node = {
            'image': np.random.rand(3, 4),
            'labels': np.random.randint(0, 21128, size=(10), dtype=np.int64),
            'bdata': np.asarray(b'11111111asdadasdasdaa')
        }
        keys.append('input{}'.format(i))
        values.append(train_node)
        if (i + 1) % 10000 == 0:
            f.put_batch(keys, values)
            keys.clear()
            values.clear()
    if len(keys):
        f.put_batch(keys, values)

    f.get_writer.put('total_num',str(n))
    f.close()



def test_random(db_path):
    options = DB.LmdbOptions(env_open_flag=DB.LmdbFlag.MDB_RDONLY,
                               env_open_mode=0o664,  # 8进制表示
                               txn_flag=LMDB.LmdbFlag.MDB_RDONLY,
                               dbi_flag=0,
                               put_flag=0)
    dataset = load_dataset.RandomDataset(db_path,
                                        data_key_prefix_list=('input',),
                                        num_key='total_num',
                                        options = options)

    dataset = dataset.parse_from_numpy_writer().shuffle(10)
    print(len(dataset))
    for i in tqdm(range(len(dataset)), total=len(dataset)):
        d = dataset[i]
        print(d)

test_write(db_path)
test_random(db_path)
```



### 7. arrow dataset 


```python

from fastdatasets.arrow.writer import PythonWriter
from fastdatasets.arrow.dataset import load_dataset,arrow


path_file = 'd:/tmp/data.arrow'


with_stream = True
def test_write():
    fs = PythonWriter(path_file,
                        schema={'id': 'int32',
                                'text': 'str',
                                # 'text2': 'str'
                                },
                        with_stream=with_stream,
                        options=None)
    for i in range(2):
        data = {
            "id": list(range(i * 10,(i+ 1) * 10)),
            'text': ['asdasdasdas' + str(i) for i in range(10)],
            # 'text2': ['asdasdasdas3asdadas' + str(i) for i in range(10)]
        }
        # fs.write_batch(data.keys(),data.values())
        fs.write_batch(data.keys(),data.values())


    fs.close()

def test_random():
    dataset = load_dataset.RandomDataset(path_file,with_share_memory=not with_stream)
    print('total', len(dataset))
    for i in range(len(dataset)):
        print(dataset[i])



def test_read_iter():
    dataset = load_dataset.IterableDataset(path_file,with_share_memory=not with_stream,batch_size=1)
    for d in dataset:
        print('iter',d)


test_write()

test_random()
#
test_read_iter()

```

### 8. parquet dataset 

```python

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

```