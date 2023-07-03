# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py
import copy
import typing
from tfrecords.python.io import gfile
from tfrecords import LMDB as DB
from typing import Union,List,AnyStr

from .iterable_dataset import SingleLmdbIterableDataset,MultiLmdbIterableDataset
from .random_dataset import SingleLmdbRandomDataset,MultiLmdbRandomDataset
from .default import global_default_options

__all__ = [
    "DB",
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
                        map_size=0):
        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiLmdbIterableDataset(path,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           cycle_length=cycle_length,
                                           block_length=block_length,
                                           options=options,
                                           map_size=map_size)
        elif isinstance(path, str):
            cls = SingleLmdbIterableDataset(path,
                                            buffer_size=buffer_size,
                                            batch_size=batch_size,
                                            block_length=block_length,
                                            options=options,
                                            map_size=map_size)
        else:
            raise Exception('path must be list or single string')
        return cls

    @staticmethod
    def SingleIterableDataset(path: typing.Union[typing.AnyStr,typing.Iterator],
                              buffer_size: typing.Optional[int] = 64,
                              batch_size: typing.Optional[int] = None,
                              block_length=1,
                              options=copy.deepcopy(global_default_options),
                              map_size=0):

        return SingleLmdbIterableDataset(path,
                                         buffer_size=buffer_size,
                                         batch_size=batch_size,
                                         block_length=block_length,
                                         options=options,
                                         map_size=map_size)

    @staticmethod
    def MultiIterableDataset(path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                             buffer_size: typing.Optional[int]=64,
                             batch_size: typing.Optional[int] = None,
                             cycle_length=None,
                             block_length=1,
                             options=copy.deepcopy(global_default_options),
                             map_size=0):

        return MultiLmdbIterableDataset(path,
                                        buffer_size=buffer_size,
                                        batch_size=batch_size,
                                        cycle_length=cycle_length,
                                        block_length=block_length,
                                        options=options,
                                        map_size=map_size)

    @staticmethod
    def RandomDataset(path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                      data_key_prefix_list=('input',),
                      num_key='total_num',
                      options=copy.deepcopy(global_default_options),
                      map_size=0,
                      ):

        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiLmdbRandomDataset(path,
                                         data_key_prefix_list=data_key_prefix_list,
                                         num_key=num_key,
                                         options=options,
                                         map_size=map_size)
        elif isinstance(path, str):
            cls = SingleLmdbRandomDataset(path,
                                          data_key_prefix_list=data_key_prefix_list,
                                          num_key=num_key,
                                          options=options,
                                          map_size=map_size)
        else:
            raise Exception('path must be list or single string')
        return cls

    @staticmethod
    def SingleRandomDataset(path: typing.Union[typing.AnyStr,typing.Sized],
                            data_key_prefix_list=('input',),
                            num_key='total_num',
                            options=copy.deepcopy(global_default_options),
                            map_size=0):
        return SingleLmdbRandomDataset(path,
                                       data_key_prefix_list=data_key_prefix_list,
                                       num_key=num_key,
                                       options=options, map_size=map_size)

    @staticmethod
    def MutiRandomDataset(path: List[typing.Union[typing.AnyStr,typing.Sized]],
                          data_key_prefix_list=('input',),
                          num_key='total_num',
                          options=copy.deepcopy(global_default_options),
                          map_size=0):

        return MultiLmdbRandomDataset(path,
                                      data_key_prefix_list=data_key_prefix_list,
                                      num_key=num_key,
                                      options=options, map_size=map_size)

