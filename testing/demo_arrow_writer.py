# -*- coding: utf-8 -*-
# @Time:  19:51
# @Author: tk
# @File：demo_arrow_writer


from fastdatasets.arrow.writer import AnythingWriter
from fastdatasets.arrow.dataset import load_dataset,arrow


path_file = 'd:/tmp/data.arrow'


with_stream = False
def test_write():
    fs = AnythingWriter(path_file,
                        schema={'id': 'int32', 'text': 'str', 'text2': 'str'},
                        with_stream=with_stream,
                        options=None)
    for i in range(3):
        data = {
            "id": list(range(i * 10,(i+ 1) * 10)),
            'text': ['asdasdasdas' + str(i) for i in range(10)],
            'text2': ['asdasdasdas3asdadas' + str(i) for i in range(10)]
        }
        # fs.write_batch(data.keys(),data.values())
        fs.write_table(data.keys(),data.values())


    fs.close()

def test_random():
    dataset = load_dataset.RandomDataset(path_file,with_share_memory=not with_stream)
    print('total', len(dataset))
    for i in range(len(dataset)):
        print(dataset[i])



def test_read_iter():
    dataset = load_dataset.IterableDataset(path_file,with_share_memory=not with_stream,batch_size=4)
    for d in dataset:
        print(d)


test_write()

test_random()

# test_read_iter()