# @Time    : 2022/9/18 10:49
# @Author  : tk
# @FileName: __init__.py.py
import logging
import typing
import os
from typing import List
import tfrecords
from tfrecords import LEVELDB
from .. import RandomDatasetBase
import copy

logging.basicConfig(level=logging.INFO)


__all__ = ["SingleLeveldbRandomDataset", "MultiLeveldbRandomDataset", "tfrecords", "logging"]

class SingleLeveldbRandomDataset(RandomDatasetBase):
    def __init__(self,
                 data_path: typing.Union[typing.AnyStr,typing.Sized],
                 data_key_prefix_list=('input',),
                 num_key='total_num',
                 options=LEVELDB.LeveldbOptions(create_if_missing=False, error_if_exists=False),
                 
                 ):
        super(SingleLeveldbRandomDataset, self).__init__()

        self.data_key_prefix_list = data_key_prefix_list
        self.data_path = data_path
        self.options = options

        self.file_reader_ : typing.Optional[LEVELDB.Leveldb] = None
        self.reset()
        if self.file_reader_ is not None:
            num_key_obj = self.file_reader_.get(num_key)
            assert num_key_obj is not None
            self.length = int(num_key_obj.decode(encoding='utf-8'))
        else:
            self.length = 0

    def __del__(self):
       self.close()

    def reset(self):
        self.repeat_done_num = 0
        self.__reopen__()

    def close(self):
        if hasattr(self,'file_reader_') and self.file_reader_:
            self.file_reader_.close()
            self.file_reader_ = None

    def __reopen__(self):
        self.block_id = -1
        self.close()

        if os.path.exists(self.data_path):
            self.file_reader_ = LEVELDB.Leveldb(self.data_path,options=self.options)
        else:
            self.file_reader_ = None

        self.repeat_done_num += 1
        return True

    def __len__(self):
        return self.length

    def __getitem__(self, item):
        if self.file_reader_ is None:
            raise OverflowError

        if isinstance(item, slice):
            return self.__getitem_slice__(item)
        data = {}
        for k in self.data_key_prefix_list:
            key = '{}{}'.format(k,item)
            value = self.file_reader_.get(key)
            assert value is not None , 'missing key ' +  key
            data[key] = value
        return data


class MultiLeveldbRandomDataset(RandomDatasetBase):
    def __init__(self,
                 data_path: List[typing.Union[typing.AnyStr,typing.Sized]],
                 data_key_prefix_list=('input',),
                 num_key='total_num',
                 options = LEVELDB.LeveldbOptions(create_if_missing=False, error_if_exists=False),
                 
                 ) -> None:
        super(MultiLeveldbRandomDataset, self).__init__()

        self.options = options
        self.data_path = data_path
        self.data_key_prefix_list = data_key_prefix_list
        self.num_key = num_key
        self.reset()

    def reset(self):
        self.iterators_ = [{"valid": False,"file": self.data_path[i]} for i in range(len(self.data_path))]
        self.cicle_iterators_ = []
        self.fresh_iter_ids = False
        self.cur_id = 0
        self.__reopen__()

    def close(self):
        for iter_obj in self.iterators_:
            if iter_obj["valid"] and "instance" in iter_obj and iter_obj["instance"]:
                iter_obj["instance"].close()
                iter_obj["valid"] = False
                iter_obj["instance"] = None

    def __reopen__(self):
        for it_obj in self.iterators_:
            it_obj['inst'] = SingleLeveldbRandomDataset(it_obj["file"],
                                                        data_key_prefix_list=self.data_key_prefix_list,
                                                        num_key=self.num_key,
                                                        options=self.options)

    def __len__(self):
        total_len = 0
        for it_obj in self.iterators_:
            total_len += len(it_obj['inst'])
        return total_len

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.__getitem_slice__(item)

        cur_len = 0
        obj = None
        for i,it_obj in enumerate(self.iterators_):
            tmp_obj = it_obj['inst']
            if item < cur_len + len(tmp_obj):
                obj = tmp_obj
                break
            cur_len += len(tmp_obj)
        if obj is None:
            raise tfrecords.OutOfRangeError
        real_index =  item - cur_len
        return obj[real_index]
