# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py
import copy
import typing
from tfrecords.python.io import gfile
from tfrecords import LEVELDB as DB
from typing import Union,List,AnyStr

from .iterable_dataset import SingleLeveldbIterableDataset,MultiLeveldbIterableDataset
from .random_dataset import SingleLeveldbRandomDataset,MultiLeveldbRandomDataset
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
                        options=copy.deepcopy(global_default_options)):
        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiLeveldbIterableDataset(path,
                                              buffer_size=buffer_size,
                                              batch_size=batch_size,
                                              cycle_length=cycle_length,
                                              block_length=block_length,
                                              options=options)
        elif isinstance(path, str):
            cls = SingleLeveldbIterableDataset(path,
                                               buffer_size=buffer_size,
                                               block_length=block_length,
                                               options=options)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleIterableDataset(data_path_or_iterator: typing.Union[typing.AnyStr,typing.Iterator],
                              buffer_size: typing.Optional[int] = 64,
                              batch_size: typing.Optional[int] = None,
                              block_length=1,
                              options=copy.deepcopy(global_default_options)):

        return SingleLeveldbIterableDataset(data_path_or_iterator,
                                            buffer_size = buffer_size,
                                            batch_size=batch_size,
                                            block_length = block_length,
                                            options = options)

    @staticmethod
    def MultiIterableDataset(data_path_or_iterator: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                             buffer_size: typing.Optional[int]=64,
                             batch_size: typing.Optional[int] = None,
                             cycle_length=None,
                             block_length=1,
                             options=copy.deepcopy(global_default_options)):

        return MultiLeveldbIterableDataset(data_path_or_iterator,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           cycle_length=cycle_length,
                                           block_length=block_length,
                                           options = options)

    @staticmethod
    def RandomDataset(path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                      data_key_prefix_list=('input',),
                      num_key='total_num',
                      options=copy.deepcopy(global_default_options)):

        if isinstance(path, list) and len(path) == 1:
            path = path[0]

        if isinstance(path, list):
            cls = MultiLeveldbRandomDataset(path,
                                            data_key_prefix_list=data_key_prefix_list,
                                            num_key=num_key,
                                            options=options)
        elif isinstance(path, str):
            cls = SingleLeveldbRandomDataset(path,
                                             data_key_prefix_list=data_key_prefix_list,
                                             num_key=num_key,
                                             options=options)
        else:
            raise Exception('data_path must be list or single string')
        return cls

    @staticmethod
    def SingleRandomDataset(data_path: typing.Union[typing.AnyStr,typing.Sized],
                            data_key_prefix_list=('input',),
                            num_key='total_num',
                            options=copy.deepcopy(global_default_options)):
        return SingleLeveldbRandomDataset(data_path,
                                          data_key_prefix_list=data_key_prefix_list,
                                          num_key = num_key,
                                          options=options,
                                          )

    @staticmethod
    def MutiRandomDataset(data_path: List[typing.Union[typing.AnyStr,typing.Sized]],
                          data_key_prefix_list=('input',),
                          num_key='total_num',
                          options=copy.deepcopy(global_default_options)):

        return MultiLeveldbRandomDataset(data_path,
                                         data_key_prefix_list = data_key_prefix_list,
                                         num_key=num_key,
                                         options = options)

