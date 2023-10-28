# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py
import copy
import typing
from tfrecords.python.io import gfile
from tfrecords import RECORD
from collections.abc import Iterator,Sized
from typing import Union,List,AnyStr
from .iterable_dataset import SingleRecordIterableDataset,MultiRecordIterableDataset
from .random_dataset import SingleRecordRandomDataset,MultiRecordRandomDataset
from .default import global_default_options

__all__ = [
    "RECORD",
    "load_dataset",
    "gfile",
]



class load_dataset:

    @staticmethod
    def IterableDataset(path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                        buffer_size: typing.Optional[int] = 128,
                        batch_size: typing.Optional[int] = None,
                        cycle_length=1,
                        block_length=1,
                        options=copy.deepcopy(global_default_options),
                        with_share_memory=False):
        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiRecordIterableDataset(path,
                                             buffer_size=buffer_size,
                                             batch_size=batch_size,
                                             block_length=block_length,
                                             cycle_length=cycle_length,
                                             options=options,
                                             with_share_memory=with_share_memory)
        elif isinstance(path, str):
            cls = SingleRecordIterableDataset(path,
                                              buffer_size=buffer_size,
                                              batch_size=batch_size,
                                              block_length=block_length,
                                              options=options,
                                              with_share_memory=with_share_memory)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleIterableDataset(path: typing.Union[typing.AnyStr,typing.Iterator],
                              buffer_size: typing.Optional[int] = 128,
                              batch_size: typing.Optional[int] = None,
                              block_length=1,
                              options=copy.deepcopy(global_default_options),
                              with_share_memory=False):

        return SingleRecordIterableDataset(path,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           block_length=block_length,
                                           options=options,
                                           with_share_memory=with_share_memory)

    @staticmethod
    def MultiIterableDataset(path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                             buffer_size: typing.Optional[int]=64,
                             batch_size: typing.Optional[int] = None,
                             cycle_length=None,
                             block_length=1,
                             options=copy.deepcopy(global_default_options),
                             with_share_memory=False):

        return MultiRecordIterableDataset(path,
                                          buffer_size=buffer_size,
                                          batch_size=batch_size,
                                          cycle_length=cycle_length,
                                          block_length=block_length,
                                          options=options,
                                          with_share_memory=with_share_memory)

    @staticmethod
    def RandomDataset(path: typing.Union[typing.List, typing.AnyStr],
                      index_path=None,
                      use_index_cache=True,
                      options=copy.deepcopy(global_default_options),
                      with_share_memory=False):

        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiRecordRandomDataset(path,
                                           index_path=index_path,
                                           use_index_cache=use_index_cache,
                                           options=options, with_share_memory=with_share_memory)
        elif isinstance(path, str):
            cls = SingleRecordRandomDataset(path,
                                            index_path=index_path,
                                            use_index_cache=use_index_cache,
                                            options=options, with_share_memory=with_share_memory)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleRandomDataset(path: typing.Union[typing.AnyStr],
                            index_path: str = None,
                            use_index_cache=True,
                            options=copy.deepcopy(global_default_options),
                            with_share_memory=False):
        return SingleRecordRandomDataset(path,
                                         index_path=index_path,
                                         use_index_cache=use_index_cache,
                                         options=options,
                                         with_share_memory=with_share_memory)

    @staticmethod
    def MutiRandomDataset(path: List[typing.Union[typing.AnyStr]],
                          index_path = None,
                          use_index_cache=True,
                          options=copy.deepcopy(global_default_options),
                          with_share_memory=False):
        return MultiRecordRandomDataset(path,
                                        index_path=index_path,
                                        use_index_cache=use_index_cache,
                                        options=options,
                                        with_share_memory=with_share_memory)



