# @Time    : 2022/9/18 10:49
# @Author  : tk
# @FileName: __init__.py.py
import logging
import typing
import os
from typing import List
import tfrecords
from .. import RandomDatasetBase
import pickle
# from collections.abc import Sized
import copy

logging.basicConfig(level=logging.INFO)


__all__ = ["SingleRecordRandomDataset","MultiRecordRandomDataset", "tfrecords", "logging"]


class SingleRecordRandomDataset(RandomDatasetBase):
    def __init__(self,
                 path: typing.Union[typing.AnyStr,typing.Sized],
                 index_path: str = None,
                 use_index_cache=True,
                 options=tfrecords.TFRecordOptions(tfrecords.TFRecordCompressionType.NONE),
                 with_share_memory=False
                 ):
        super(SingleRecordRandomDataset, self).__init__()



        self.with_share_memory = with_share_memory

        if index_path is None:
            index_path = os.path.join(os.path.dirname(path), '.' + os.path.basename(path)+ '.INDEX')
        else:
            index_path = os.path.join(index_path, '.' + os.path.basename(path)+ '.INDEX')

        self.path = path
        self.index_path = index_path
        self.options = options
        self.use_index_cache = use_index_cache

        self.file_reader_ = None

        self.reset()

        self.gen_indexes()

        self.length = len(self.indexes)


    def gen_indexes(self):
        is_need_update_idx = False
        cur_file_size = 0
        cur_st_ctime = -1

        filestat = None
        if self.use_index_cache and os.path.exists(self.index_path):
            if os.path.exists(self.path):
                filestat = os.stat(self.path)
                cur_file_size = filestat.st_size
                cur_st_ctime = filestat.st_ctime

            with open(self.index_path, mode='rb') as f:
                filemeta = pickle.load(f)

            cache_file_size = -1
            cache_st_ctime = -1

            if not isinstance(filemeta, tuple):
                is_need_update_idx = True
            elif len(filemeta) != 3:
                is_need_update_idx = True
            else:
                self.indexes, cache_file_size, cache_st_ctime = filemeta

            if cache_file_size != cur_file_size or cache_st_ctime != cur_st_ctime:
                is_need_update_idx = True
        else:
            is_need_update_idx = True

        if is_need_update_idx:
            logging.info('update index for {}...'.format(self.path))
            if os.path.exists(self.path):
                if filestat is None:
                    filestat = os.stat(self.path)
                cur_file_size = filestat.st_size
                cur_st_ctime = filestat.st_ctime
            try:
               if self.file_reader_ is not None:
                   self.indexes = self.file_reader_.read_offsets(0)
            except Exception as e:
                self.indexes = []
            if self.use_index_cache and len(self.indexes):
                with open(self.index_path, mode='wb') as f:
                    pickle.dump((self.indexes, cur_file_size, cur_st_ctime), f)
            logging.info('update index for {} finish'.format(self.path))


    def __del__(self):
       self.close()

    def reset(self):
        self.repeat_done_num = 0
        self.__reopen__()

    def close(self):
        if hasattr(self, 'file_reader_') and self.file_reader_:
            self.file_reader_.close()
            self.file_reader_ = None


    def __reopen__(self):
        self.block_id = -1
        self.close()
        if os.path.exists(self.path):
            self.file_reader_ = tfrecords.tf_record_random_reader(self.path, options=self.options,
                                                                  with_share_memory=self.with_share_memory)
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

        pos_inf = self.indexes[item]
        x, _ = self.file_reader_.read(pos_inf[0])
        return x


class MultiRecordRandomDataset(RandomDatasetBase):
    def __init__(self,
                 data_path_data_list: List[typing.Union[typing.AnyStr,typing.Sized]],
                 index_path = None,
                 use_index_cache=True,
                 options = tfrecords.TFRecordOptions(tfrecords.TFRecordCompressionType.NONE),
                 with_share_memory=False
                 ) -> None:
        super(MultiRecordRandomDataset, self).__init__()

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
            it_obj['inst'] = SingleRecordRandomDataset(it_obj["file"], index_path=self.index_path,
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
            raise tfrecords.OutOfRangeError
        real_index =  item - cur_len
        return obj[real_index]
