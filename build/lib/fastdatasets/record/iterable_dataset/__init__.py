"""Load tfrecord files into torch datasets."""
# -*- coding: utf-8 -*-
# @Time    : 2022/9/8 15:49

import os
import warnings
import typing
# from collections.abc import Iterator
import tfrecords
from multiprocessing import cpu_count
from .. import IterableDatasetBase
import copy
from ..default import global_default_options

__all__ = [
    "SingleRecordIterableDataset",
    "MultiRecordIterableDataset",
]

class SingleRecordIterableDataset(IterableDatasetBase):
    def __init__(self,
                 path: typing.Union[typing.AnyStr,typing.Iterator],
                 buffer_size: typing.Optional[int] = 64,
                 batch_size: typing.Optional[int] = None,
                 block_length=1,
                 options=copy.deepcopy(global_default_options),
                 with_share_memory=False
                 ):

        assert block_length > 0
        self.with_share_memory = with_share_memory

        self.batch_size = batch_size if batch_size is not None else 1
        assert self.batch_size > 0
        self.block_length = block_length
        self.path = path
        self.options  = options

        self.block_id = -1
        if buffer_size is None:
            buffer_size = 1
        self.buffer_size = buffer_size
        assert self.buffer_size > 0

        self.buffer = []
        self.iterator_ = None
        self.reset()

    def __del__(self):
       self.close()

    def reset(self):
        self.repeat_done_num = 0
        self.buffer.clear()
        self.__reopen__()

    def close(self):
        if hasattr(self, 'iterator_') and self.iterator_:
            self.iterator_.close()
            self.iterator_ = None


    def __reopen__(self):
        self.block_id = -1
        self.close()

        if os.path.exists(self.path):
            self.iterator_ = tfrecords.tf_record_iterator(self.path,
                                                          options=self.options,
                                                          with_share_memory=self.with_share_memory)
        else:
            self.iterator_ = None


        self.repeat_done_num += 1
        return True

    def reach_block(self):
        if (self.block_id  + 1) % self.block_length == 0:
            return True
        return False

    def __iter__(self):
        return self

    def __next__(self):
        iter = self.__next_ex__()
        self.block_id += 1
        return iter

    def __next_ex__(self):
        iterator = self.iterator_
        if iterator is None:
            raise StopIteration

        if len(self.buffer) < self.batch_size:
            try:
                for _ in range(max(self.buffer_size,self.batch_size-len(self.buffer) + 1)):
                    self.buffer.append(next(iterator))
            except StopIteration:
                pass
            except tfrecords.DataLossError:
                warnings.warn('data corrupted in {} Is this even a TFRecord file?'.format(self.path))
                pass

        if len(self.buffer) == 0:
            raise StopIteration

        if self.batch_size == 1:
            return self.buffer.pop(0)

        return [self.buffer.pop(0) for i in range(min(len(self.buffer), self.batch_size))]

class MultiRecordIterableDataset(IterableDatasetBase):
    """Parse (generic) TFRecords dataset into `IterableDataset` object,
    which contain `np.ndarrays`s. By default (when `sequence_description`
    is None), it treats the TFRecords as containing `tf.Example`.
    Otherwise, it assumes it is a `tf.SequenceExample`.

    Params:
    -------
    data_path: List
        The path to the tfrecords file.
    buffer_size: int, optional, default=None
        Length of buffer. Determines how many records are queued to
        sample from.
    cycle_length : a callable, default = min(len(filename),cpu_num)
    block_length: default 1
    options: TFRecordOptions
    """

    def __init__(self,
                 path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                 buffer_size: typing.Optional[int]=64,
                 batch_size: typing.Optional[int] = 1,
                 cycle_length=None,
                 block_length=1,
                 options=copy.deepcopy(global_default_options),
                 with_share_memory=False
                 ) -> None:
        super(MultiRecordIterableDataset, self).__init__()

        assert block_length > 0

        if cycle_length is None:
            cycle_length = cpu_count()

        self.with_share_memory = with_share_memory
        self.options = options
        self.cycle_length = min(cycle_length,len(path))
        self.block_length = block_length
        self.path = path
        self.buffer_size = buffer_size
        self.batch_size = batch_size

        if self.buffer_size is None:
            self.buffer_size = 1
        self.reset()

    def reset(self):
        self.iterators_ = [{"valid": False,"file": self.path[i]} for i in range(len(self.path))]
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
        iterators_ = [x for x in self.iterators_]
        for it_obj in iterators_:
            if len(self.cicle_iterators_) >= self.cycle_length:
                break
            self.iterators_.remove(it_obj)
            self.cicle_iterators_.append(
                {
                    "class": SingleRecordIterableDataset,
                    "file": it_obj["file"],
                    "instance": None
                }
            )


    def get_iterator(self):
        if len(self.cicle_iterators_) == 0 or self.fresh_iter_ids:
            self.fresh_iter_ids = False
            if len(self.cicle_iterators_) < self.cycle_length:
                self.__reopen__()
            if len(self.cicle_iterators_) == 0:
                return None
        it_obj = self.cicle_iterators_[self.cur_id]
        it_obj['id'] = self.cur_id
        return it_obj


    def __iter__(self):
        return self

    def __next__(self):
        it = None
        while True:
            if len(self.cicle_iterators_) > 0 or len(self.iterators_):
                try:
                    it = self.__next_ex()
                    break
                except StopIteration:
                    pass
            else:
                raise StopIteration
        return it

    def __next_ex(self):
        iter_obj = self.get_iterator()
        if iter_obj is None:
            raise StopIteration
        try:
            if iter_obj['instance'] is None:
                iter_obj['instance'] = iter_obj['class'](path=iter_obj["file"],
                                                         buffer_size=self.buffer_size,
                                                         batch_size=self.batch_size,
                                                         block_length= self.block_length,
                                                         options= self.options,
                                                         with_share_memory=self.with_share_memory)
            iter = iter_obj['instance']
            it = next(iter)
            if iter.reach_block():
                self.cur_id += 1
                self.cur_id = self.cur_id % len(self.cicle_iterators_) if len(self.cicle_iterators_) else 0
            return it
        except StopIteration:
            self.cicle_iterators_.remove(iter_obj)
            self.fresh_iter_ids = True
            raise StopIteration

