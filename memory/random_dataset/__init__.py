# @Time    : 2022/9/18 10:49
# @Author  : tk
# @FileName: __init__.py.py
import logging
import typing
import os
from typing import List
from .. import RandomDatasetBase
import pickle
from collections.abc import Sized
import copy

logging.basicConfig(level=logging.INFO)


__all__ = ["SingleMemoryRandomDataset",
           "MultiMemoryRandomDataset",
           "logging"]


class SingleMemoryRandomDataset(RandomDatasetBase):
    def __init__(self,
                 data_list: typing.Union[typing.AnyStr,typing.Sized],
                 index_path: str = None,
                 use_index_cache=True,
                 options=None,
                 with_share_memory=False
                 ):
        super(SingleMemoryRandomDataset, self).__init__()

        #从内存中加载

        assert isinstance(data_list, Sized)
            

        self.with_share_memory = with_share_memory
 
        self.data_list = data_list
        self.index_path = index_path
        self.options = options
        self.use_index_cache = use_index_cache

        self.file_reader_ = None

        self.reset()

        self.gen_indexes()

        self.length = len(self.indexes)


    def gen_indexes(self):
        self.indexes = self.data_list

    def __del__(self):
       self.close()

    def reset(self):
        self.repeat_done_num = 0
        self.__reopen__()

    def close(self):
        self.file_reader_ = None

    def __reopen__(self):
        self.block_id = -1
        self.close()

        self.file_reader_ = self.data_list
        self.repeat_done_num += 1
        return True

    def __len__(self):
        return self.length

    def __getitem__(self, item):
        if self.file_reader_ is None:
            raise OverflowError

        if isinstance(item, slice):
            return self.__getitem_slice__(item)

        x = self.data_list[item]
        return x


class MultiMemoryRandomDataset(RandomDatasetBase):
    def __init__(self,
                 data_path_data_list: List[typing.Union[typing.AnyStr,typing.Sized]],
                 index_path = None,
                 use_index_cache=True,
                 options = None,
                 with_share_memory=False
                 ) -> None:
        super(MultiMemoryRandomDataset, self).__init__()

        self.with_share_memory = with_share_memory
        self.options = options
        self.data_path_data_list = data_path_data_list
        self.index_path = index_path
        self.use_index_cache = use_index_cache
        self.reset()

    def reset(self):
        self.iterators_ = [{"valid": False,"file": self.data_path_data_list[i]} for i in range(len(self.data_path_data_list))]
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
            it_obj['inst'] = SingleMemoryRandomDataset(it_obj["file"], index_path=self.index_path,
                                                       use_index_cache=self.use_index_cache,
                                                       options=self.options,
                                                       with_share_memory=self.with_share_memory)

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
            raise OverflowError
        real_index =  item - cur_len
        return obj[real_index]
