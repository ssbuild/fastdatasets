# -*- coding: utf-8 -*-
# @Time    : 2022/11/11 9:04
import typing
from .py_features import Final

__all__ = [
    'MemoryOptions',
    'MemoryWriter'
]





class MemoryOptions:
    def __init__(self, reserve=True):
        self.reserve = reserve


class MemoryWriter:
    def __init__(self, filename: typing.List, options=MemoryOptions(reserve=True)):
        if options is None:
            options = MemoryOptions(reserve=True)
        self.options = options

        self.filename = filename
        assert isinstance(self.filename,list)

        self.__data__ = self.filename
        self.__data__.clear()


    def __del__(self):
        self.close()

    def data(self):
        return self.__data__

    def write(self,data):
        self.data().append(data)
        return data

    def write_batch(self,data_list):
        self.data().extend(data_list)
        return data_list

    def flush(self):...

    def close(self):...