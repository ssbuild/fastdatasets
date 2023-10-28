# -*- coding: utf-8 -*-
# @Time:  8:41
# @Author: tk
# @File：writer_object
import typing

class WriterObjectBaseForKV:
    @property
    def get_writer(self):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError

    def put_batch(self,keys : typing.List[typing.Union[str , bytes]],values : typing.List[typing.Union[str , bytes]]):
        raise NotImplementedError

    def put(self,key : typing.Union[bytes,str],value : typing.Union[bytes,str]):
        raise NotImplementedError

    def get(self,key : typing.Union[bytes,str],default_value=None):
        raise NotImplementedError

    def remove(self,key : typing.Union[bytes,str],):
        raise NotImplementedError

class WriterObjectBaseForRecord:

    @property
    def get_writer(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def write(self, data, *args, **kwargs):
        raise NotImplementedError

    def write_batch(self,data, *args, **kwargs):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError