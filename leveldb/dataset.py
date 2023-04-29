# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py

import typing
from tfrecords.python.io import gfile
from tfrecords import LEVELDB as DB
from typing import Union,List,AnyStr

from .iterable_dataset import SingleLeveldbIterableDataset,MultiLeveldbIterableDataset
from .random_dataset import SingleLeveldbRandomDataset,MultiLeveldbRandomDataset


__all__ = [
           #  "SingleLeveldbIterableDataset",
           # "MultiLeveldbIterableDataset",
           # "SingleLeveldbRandomDataset",
           # "MultiLeveldbRandomDataset",
           "DB",
           "load_dataset",
           "gfile",
           ]

_DefaultOptions = DB.LeveldbOptions(create_if_missing=False, error_if_exists=False)

def LeveldbIterableDatasetLoader(data_path: Union[List[Union[AnyStr, typing.Iterator]], AnyStr, typing.Iterator],
                                 buffer_size: typing.Optional[int] = 128,
                                 cycle_length=1,
                                 block_length=1,
                                 options=_DefaultOptions,
                                 ):
    if isinstance(data_path, list):
        if len(data_path) == 1:
            cls = SingleLeveldbIterableDataset(data_path[0], buffer_size, block_length, options,

                                               )
        else:
            cls = MultiLeveldbIterableDataset(data_path, buffer_size, cycle_length, block_length, options)
    elif isinstance(data_path, str):
        cls = SingleLeveldbIterableDataset(data_path, buffer_size, block_length, options,

                                           )
    else:
        raise Exception('data_path must be list or single string')
    return cls

def LeveldbRandomDatasetLoader(data_path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                               data_key_prefix_list=('input',),
                               num_key='total_num',
                               options=_DefaultOptions,
                               ):
    if isinstance(data_path, list):
        if len(data_path) == 1:
            cls = SingleLeveldbRandomDataset(data_path[0], data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,

                                             )
        else:
            cls = MultiLeveldbRandomDataset(data_path, data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,
                                            )
    elif isinstance(data_path, str):
        cls = SingleLeveldbRandomDataset(data_path, data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,
                                         )
    else:
        raise Exception('data_path must be list or single string')
    return cls

class load_dataset:

    @staticmethod
    def IterableDataset(data_path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                     buffer_size: typing.Optional[int] = 128,
                     cycle_length=1,
                     block_length=1,
                     options=_DefaultOptions,
                     ):
        return LeveldbIterableDatasetLoader(data_path,
                                            buffer_size,
                                            cycle_length,
                                            block_length,
                                            options=options, )

    @staticmethod
    def SingleIterableDataset( data_path_or_iterator: typing.Union[typing.AnyStr,typing.Iterator],
                 buffer_size: typing.Optional[int] = 64,
                 block_length=1,
                 options=_DefaultOptions,
                 ):

            return SingleLeveldbIterableDataset(data_path_or_iterator, buffer_size, block_length, options,
                                                )

    @staticmethod
    def MultiIterableDataset(data_path_or_iterator: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                 buffer_size: typing.Optional[int]=64,
                 cycle_length=None,
                 block_length=1,
                 options = _DefaultOptions,
                 ):

            return MultiLeveldbIterableDataset(data_path_or_iterator, buffer_size, cycle_length, block_length, options,
                                               )

    @staticmethod
    def RandomDataset(data_path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                      data_key_prefix_list=('input',),
                      num_key='total_num',
                      options = _DefaultOptions,
                      ):

        return LeveldbRandomDatasetLoader(data_path,
                                          data_key_prefix_list,
                                          num_key, options,
                                          )

    @staticmethod
    def SingleRandomDataset(data_path: typing.Union[typing.AnyStr,typing.Sized],
                            data_key_prefix_list=('input',),
                            num_key='total_num',
                 options=_DefaultOptions
                 ):
        return SingleLeveldbRandomDataset(data_path,
                                          data_key_prefix_list,
                                          num_key,
                                          options=options,
                                          )

    @staticmethod
    def MutiRandomDataset(data_path: List[typing.Union[typing.AnyStr,typing.Sized]],
                        data_key_prefix_list=('input',),
                        num_key='total_num',
                        options =_DefaultOptions,
                        ):
        return MultiLeveldbRandomDataset(data_path,
                                         data_key_prefix_list,
                                         num_key,
                                         options,
                                         )




