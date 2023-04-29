# @Time    : 2022/10/27 20:37
# @Author  : tk

from tqdm import tqdm
from fastdatasets.leveldb import DB,load_dataset,WriterObject,DataType,StringWriter,JsonWriter,FeatureWriter

db_path = 'd:\\example_leveldb'


def test_write(db_path):
    options = DB.LeveldbOptions(create_if_missing=True,error_if_exists=False)
    f = WriterObject(db_path, options = options)

    keys,values = [],[]
    n = 30
    for i in range(n):
        keys.append('input{}'.format(i))
        keys.append('label{}'.format(i))
        values.append(str(i))
        values.append(str(i))
        if (i+1) % 10000 == 0:
            f.file_writer.put_batch(keys,values)
            keys.clear()
            values.clear()
    if len(keys):
        f.file_writer.put_batch(keys, values)
        
    f.put('total_num',str(n))
    f.close()


def test_iterable(db_path):
    options = DB.LeveldbOptions(create_if_missing=False, error_if_exists=False)
    dataset = load_dataset.IterableDataset(db_path, options = options)
    for d in dataset:
        print(d)

def test_random(db_path):
    options = DB.LeveldbOptions(create_if_missing=False, error_if_exists=False)
    dataset = load_dataset.RandomDataset(db_path,
                                        data_key_prefix_list=('input','label'),
                                        num_key='total_num',
                                        options = options)

    dataset = dataset.shuffle(10)
    print(len(dataset))
    for i in tqdm(range(len(dataset)),total=len(dataset)):
        d = dataset[i]
        print(i,d)

test_write(db_path)
test_iterable(db_path)
test_random(db_path)