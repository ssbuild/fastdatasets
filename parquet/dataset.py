# @Time    : 2022/9/20 21:55
# @Author  : tk
# @FileName: dataset.py
import copy
import typing
from tfrecords.python.io import gfile
from tfrecords.python.io.arrow import arrow,parquet
from typing import Union,List,AnyStr
from .iterable_dataset import SingleParquetIterableDataset,MultiParquetIterableDataset
from .random_dataset import SingleParquetRandomDataset,MultiParquetRandomDataset
from .default import global_default_options

__all__ = [
    "load_dataset",
    "gfile",
]

def RecordIterableDatasetLoader(path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                                col_names: typing.Optional[typing.List[str]] = None,
                                buffer_size: typing.Optional[int] = 1,
                                batch_size: typing.Optional[int] = None,
                                cycle_length=1,
                                block_length=1,
                                options=copy.deepcopy(global_default_options),
                                with_share_memory=False):
    
    if isinstance(path, list):
        if len(path) == 1:
            cls = SingleParquetIterableDataset(path[0],
                                               col_names=col_names,
                                               buffer_size=buffer_size,
                                               batch_size=batch_size,
                                               block_length=block_length,
                                               options=options,
                                               with_share_memory=with_share_memory)
        else:
            cls = MultiParquetIterableDataset(path,
                                              col_names=col_names,
                                              buffer_size= buffer_size,
                                              batch_size=batch_size,
                                              cycle_length=cycle_length,
                                              block_length=block_length,
                                              options=options)
    elif isinstance(path, str):
        cls = SingleParquetIterableDataset(path,
                                           col_names=col_names,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           block_length=block_length,
                                           options=options,
                                           with_share_memory=with_share_memory)
    else:
        raise Exception('data_path must be list or single string')
    return cls

def RecordRandomDatasetLoader(path: typing.Union[typing.AnyStr,typing.Sized],
                              col_names: typing.Optional[typing.List[str]] = None,
                              options=copy.deepcopy(global_default_options),
                              with_share_memory=False):
    if isinstance(path, list):
        if len(path) == 1:
            cls = SingleParquetRandomDataset(path[0],
                                             col_names=col_names,
                                             options=options,
                                             with_share_memory=with_share_memory)
        else:
            cls = MultiParquetRandomDataset(path,
                                            col_names=col_names,
                                            options=options,
                                            with_share_memory=with_share_memory)
    elif isinstance(path, str):
        cls = SingleParquetRandomDataset(path,
                                         col_names=col_names,
                                         options=options,
                                         with_share_memory=with_share_memory)
    else:
        raise Exception('data_path must be list or single string')
    return cls

class load_dataset:

    @staticmethod
    def IterableDataset(path: Union[List[Union[AnyStr,typing.Iterator]],AnyStr,typing.Iterator],
                        col_names: typing.Optional[typing.List[str]] = None,
                        buffer_size: typing.Optional[int] = 1,
                        batch_size: typing.Optional[int] = None,
                        cycle_length=1,
                        block_length=1,
                        options=copy.deepcopy(global_default_options),
                        with_share_memory=False):
        return RecordIterableDatasetLoader(path,
                                           col_names=col_names,
                                           buffer_size=buffer_size,
                                           batch_size=batch_size,
                                           cycle_length=cycle_length,
                                           block_length=block_length,
                                           options=options,
                                           with_share_memory=with_share_memory)

    @staticmethod
    def SingleIterableDataset(path: typing.Union[typing.AnyStr,typing.Iterator],
                              col_names: typing.Optional[typing.List[str]] = None,
                              buffer_size: typing.Optional[int] = 1,
                              batch_size: typing.Optional[int] = None,
                              block_length=1,
                              options=copy.deepcopy(global_default_options),
                              with_share_memory=False):

        return SingleParquetIterableDataset(path,
                                            col_names=col_names,
                                            buffer_size=buffer_size,
                                            batch_size=batch_size,
                                            block_length=block_length,
                                            options=options,
                                            with_share_memory=with_share_memory)

    @staticmethod
    def MultiIterableDataset(path: typing.List[typing.Union[typing.AnyStr,typing.Iterator]],
                             col_names: typing.Optional[typing.List[str]] = None,
                             buffer_size: typing.Optional[int]=1,
                             batch_size: typing.Optional[int] = None,
                             cycle_length=None,
                             block_length=1,
                             options=copy.deepcopy(global_default_options),
                             with_share_memory=False):

        return MultiParquetIterableDataset(path,
                                           col_names=col_names,
                                           buffer_size= buffer_size,
                                           batch_size=batch_size,
                                           cycle_length=cycle_length,
                                           block_length= block_length,
                                           options=options,
                                           with_share_memory=with_share_memory)

    @staticmethod
    def RandomDataset(path: typing.Union[typing.AnyStr,typing.Sized],
                      col_names: typing.Optional[typing.List[str]] = None,
                      options=copy.deepcopy(global_default_options),
                      with_share_memory=False):

        return RecordRandomDatasetLoader(path,
                                         col_names=col_names,
                                         options = options,
                                         with_share_memory=with_share_memory)

    @staticmethod
    def SingleRandomDataset(path: typing.Union[typing.AnyStr,typing.Sized],
                            col_names: typing.Optional[typing.List[str]] = None,
                            options=copy.deepcopy(global_default_options),
                            with_share_memory=False):
        return SingleParquetRandomDataset(path,
                                          col_names=col_names,
                                          options=options,
                                          with_share_memory=with_share_memory)

    @staticmethod
    def MutiRandomDataset(path: typing.Union[typing.AnyStr,typing.Sized],
                          col_names: typing.Optional[typing.List[str]] = None,
                          options=copy.deepcopy(global_default_options),
                          with_share_memory=False):
        return MultiParquetRandomDataset(path,
                                         col_names=col_names,
                                         options=options,
                                         with_share_memory=with_share_memory)
