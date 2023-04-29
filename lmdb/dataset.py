# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py

import typing
from tfrecords.python.io import gfile
from tfrecords import LMDB as DB
from typing import Union,List,AnyStr

from .iterable_dataset import SingleLmdbIterableDataset,MultiLmdbIterableDataset
from .random_dataset import SingleLmdbRandomDataset,MultiLmdbRandomDataset

__all__ = [
           # "SingleLmdbIterableDataset",
           # "MultiLmdbIterableDataset",
           # "SingleLmdbRandomDataset",
           # "MultiLmdbRandomDataset",
            "DB",
           "load_dataset",
           "gfile",
           ]

_DefaultOptions = DB.LmdbOptions(env_open_flag = DB.LmdbFlag.MDB_RDONLY,
                env_open_mode = 0o664, # 8进制表示
                txn_flag = DB.LmdbFlag.MDB_RDONLY,
                dbi_flag = 0,
                put_flag = 0)

def LmdbIterableDatasetLoader(data_path: Union[List[Union[AnyStr, typing.Iterator]], AnyStr, typing.Iterator],
                                 buffer_size: typing.Optional[int] = 128,
                                 cycle_length=1,
                                 block_length=1,
                                 options=_DefaultOptions,
                                 map_size=0,
                                 ):
    if isinstance(data_path, list):
        if len(data_path) == 1:
            cls = SingleLmdbIterableDataset(data_path[0], buffer_size, block_length, options=options,map_size=map_size
                                               )
        else:
            cls = MultiLmdbIterableDataset(data_path, buffer_size, cycle_length, block_length, options=options,map_size=map_size
                                           )
    elif isinstance(data_path, str) :
        cls = SingleLmdbIterableDataset(data_path, buffer_size, block_length, options=options,map_size=map_size
                                           )
    else:
        raise Exception('data_path must be list or single string')
    return cls

def LmdbRandomDatasetLoader(data_path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                               data_key_prefix_list=('input',),
                               num_key='total_num',
                               options=_DefaultOptions,
                            map_size=0,
                               ):
    if isinstance(data_path, list):
        if len(data_path) == 1:
            cls = SingleLmdbRandomDataset(data_path[0], data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,map_size=map_size
                                             )
        else:
            cls = MultiLmdbRandomDataset(data_path, data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,map_size=map_size
                                            )
    elif isinstance(data_path, str) :
        cls = SingleLmdbRandomDataset(data_path, data_key_prefix_list=data_key_prefix_list, num_key=num_key, options=options,map_size=map_size
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
                     map_size=0,
                     ):
        return LmdbIterableDatasetLoader(data_path,
                                            buffer_size,
                                            cycle_length,
                                            block_length,
                                            options=options, 
                                            map_size=map_size
                                         )

    @staticmethod
    def SingleIterableDataset( data_path: typing.Union[typing.AnyStr,typing.Iterator],
                 buffer_size: typing.Optional[int] = 64,
                 block_length=1,
                 options=_DefaultOptions,
                 map_size=0
                 ):

            return SingleLmdbIterableDataset(data_path, buffer_size, block_length, options,map_size=map_size,
                                                )

    @staticmethod
    def MultiIterableDataset(data_path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                 buffer_size: typing.Optional[int]=64,
                 cycle_length=None,
                 block_length=1,
                 options = _DefaultOptions,
                 map_size=0,
                 ):

            return MultiLmdbIterableDataset(data_path, buffer_size, cycle_length, block_length, options=options,map_size=map_size,
                                               )

    @staticmethod
    def RandomDataset(data_path: typing.Union[typing.List, typing.AnyStr, typing.Sized],
                      data_key_prefix_list=('input',),
                      num_key='total_num',
                      options=_DefaultOptions,
                      map_size=0,
                      ):

        return LmdbRandomDatasetLoader(data_path,
                                          data_key_prefix_list,
                                          num_key, options=options,map_size=map_size
                                          )

    @staticmethod
    def SingleRandomDataset(data_path: typing.Union[typing.AnyStr,typing.Sized],
                            data_key_prefix_list=('input',),
                            num_key='total_num',
                            options=_DefaultOptions,
                            map_size=0,
                 ):
        return SingleLmdbRandomDataset(data_path,
                                          data_key_prefix_list,
                                          num_key,
                                          options=options,
                                          map_size=map_size
                                          )

    @staticmethod
    def MutiRandomDataset(data_path: List[typing.Union[typing.AnyStr,typing.Sized]],
                        data_key_prefix_list=('input',),
                        num_key='total_num',
                        options = _DefaultOptions,
                        map_size=0,
                        ):
        return MultiLmdbRandomDataset(data_path,
                                    data_key_prefix_list,
                                    num_key,
                                    options=options,
                                    map_size=map_size)

