# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py

import typing
from tfrecords.python.io import gfile
from collections.abc import Iterator,Sized
from typing import Union,List,AnyStr
from .iterable_dataset import SingleMemoryIterableDataset,MultiMemoryIterableDataset
from .random_dataset import SingleMemoryRandomDataset,MultiMemoryRandomDataset

__all__ = [
   "load_dataset",
   "gfile"
]


class load_dataset:

    @staticmethod
    def IterableDataset(data_list: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                        buffer_size: typing.Optional[int] = 128,
                        batch_size: typing.Optional[int] = None,
                        cycle_length=1,
                        block_length=1,
                        options=None):

        if isinstance(data_list, list) and len(data_list) == 1:
            data_list = data_list[0]

        if isinstance(data_list, list):
            cls = MultiMemoryIterableDataset(data_list,
                                             buffer_size=buffer_size,
                                             batch_size=batch_size,
                                             cycle_length=cycle_length,
                                             block_length=block_length,
                                             options=options)
        elif isinstance(data_list, Iterator):
            cls = SingleMemoryIterableDataset(data_list,
                                              buffer_size=buffer_size,
                                              block_length=block_length,
                                              options=options)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleIterableDataset(data_list: typing.Union[typing.AnyStr,typing.Iterator],
                              buffer_size: typing.Optional[int] = 64,
                              batch_size: typing.Optional[int] = None,
                              block_length=1,
                              options=None):
        return SingleMemoryIterableDataset(data_list,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           block_length=block_length,
                                           options=options)

    @staticmethod
    def MultiIterableDataset(data_list: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                             buffer_size: typing.Optional[int]=64,
                             batch_size: typing.Optional[int] = None,
                             cycle_length=None,
                             block_length=1,
                             options = None):

        return MultiMemoryIterableDataset(data_list,
                                          buffer_size=buffer_size,
                                          batch_size=batch_size,
                                          cycle_length=cycle_length,
                                          block_length=block_length,
                                          options=options)

    @staticmethod
    def RandomDataset(data_list: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                      index_path=None,
                      use_index_cache=True,
                      options=None):

        if isinstance(data_list, list):
            if len(data_list) > 0 and isinstance(data_list[0], list):
                cls = MultiMemoryRandomDataset(data_list,
                                               index_path=index_path,
                                               use_index_cache=use_index_cache,
                                               options=options)
            else:
                cls = SingleMemoryRandomDataset(data_list,
                                                index_path=index_path,
                                                use_index_cache=use_index_cache,
                                                options=options)

        elif isinstance(data_list, Sized):
            cls = SingleMemoryRandomDataset(data_list,
                                            index_path=index_path,
                                            use_index_cache=use_index_cache,
                                            options=options)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleRandomDataset(data_list: typing.Union[typing.AnyStr,typing.Sized],
                            index_path: str = None,
                            use_index_cache=True,
                            options=None):
        return SingleMemoryRandomDataset(data_list,
                                         index_path=index_path,
                                         use_index_cache=use_index_cache,
                                         options=options)

    @staticmethod
    def MutiRandomDataset(data_list: List[typing.Union[typing.AnyStr,typing.Sized]],
                          index_path = None,
                          use_index_cache=True,
                          options = None):
        return MultiMemoryRandomDataset(data_list,
                                        index_path=index_path,
                                        use_index_cache=use_index_cache,
                                        options=options)




