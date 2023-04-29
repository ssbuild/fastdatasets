# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py

import typing
from tfrecords.python.io import gfile
from tfrecords import RECORD
from collections.abc import Iterator,Sized
from typing import Union,List,AnyStr
from .iterable_dataset import SingleRecordIterableDataset,MultiRecordIterableDataset
from .random_dataset import SingleRecordRandomDataset,MultiRecordRandomDataset


__all__ = [
           #  "SingleRecordIterableDataset",
           # "MultiRecordIterableDataset",
           # "SingleRecordRandomDataset",
           # "MultiRecordRandomDataset",
           "RECORD",
           "load_dataset",
           "gfile",
           ]

def RecordIterableDatasetLoader(path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                     buffer_size: typing.Optional[int] = 128,
                     cycle_length=1,
                     block_length=1,
                     options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                     with_share_memory=False):
    if isinstance(path, list):
        if len(path) == 1:
            cls = SingleRecordIterableDataset(path[0], buffer_size, block_length, options,
                with_share_memory=with_share_memory
            )
        else:
            cls = MultiRecordIterableDataset(path, buffer_size, cycle_length, block_length, options)
    elif isinstance(path, str):
        cls = SingleRecordIterableDataset(path, buffer_size, block_length, options,
            with_share_memory=with_share_memory
        )
    else:
        raise Exception('data_path must be list or single string')
    return cls

def RecordRandomDatasetLoader(path: typing.Union[typing.List,typing.AnyStr],
                            index_path=None,
                            use_index_cache=True,
                            options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                            with_share_memory=False):
    if isinstance(path, list):
        if len(path) == 1:
            cls = SingleRecordRandomDataset(path[0], index_path=index_path, use_index_cache=use_index_cache, options=options,
                                            with_share_memory=with_share_memory
                                            )
        else:
            cls = MultiRecordRandomDataset(path, index_path=index_path, use_index_cache=use_index_cache, options=options,
                                           with_share_memory=with_share_memory)
    elif isinstance(path, str):
        cls = SingleRecordRandomDataset(path, index_path=index_path, use_index_cache=use_index_cache, options=options,
                                        with_share_memory=with_share_memory)
    else:
        raise Exception('data_path must be list or single string')
    return cls

class load_dataset:

    @staticmethod
    def IterableDataset(path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                     buffer_size: typing.Optional[int] = 128,
                     cycle_length=1,
                     block_length=1,
                     options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                     with_share_memory=False):
        return RecordIterableDatasetLoader(path,
                     buffer_size,
                     cycle_length,
                     block_length,
                     options=options,with_share_memory=with_share_memory)

    @staticmethod
    def SingleIterableDataset( path: typing.Union[typing.AnyStr,typing.Iterator],
                 buffer_size: typing.Optional[int] = 64,
                 block_length=1,
                 options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                 with_share_memory=False):

            return SingleRecordIterableDataset(path, buffer_size,block_length,options,
                                               with_share_memory=with_share_memory)

    @staticmethod
    def MultiIterableDataset(path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                 buffer_size: typing.Optional[int]=64,
                 cycle_length=None,
                 block_length=1,
                 options = RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                 with_share_memory=False):

            return MultiRecordIterableDataset(path, buffer_size,cycle_length,block_length,options,
                                              with_share_memory=with_share_memory)

    @staticmethod
    def RandomDataset(path: typing.Union[typing.List, typing.AnyStr],
                      index_path=None,
                      use_index_cache=True,
                      options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                      with_share_memory=False):

        return RecordRandomDatasetLoader(path,index_path,use_index_cache,options,
                                         with_share_memory=with_share_memory)

    @staticmethod
    def SingleRandomDataset(path: typing.Union[typing.AnyStr],
                 index_path: str = None,
                 use_index_cache=True,
                 options=RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                 with_share_memory=False):
        return SingleRecordRandomDataset(path, index_path, use_index_cache, options=options,
                                         with_share_memory=with_share_memory)

    @staticmethod
    def MutiRandomDataset(path: List[typing.Union[typing.AnyStr]],
                 index_path = None,
                 use_index_cache=True,
                 options = RECORD.TFRecordOptions(RECORD.TFRecordCompressionType.NONE),
                 with_share_memory=False):
        return MultiRecordRandomDataset(path, index_path, use_index_cache, options,
                                        with_share_memory=with_share_memory)




