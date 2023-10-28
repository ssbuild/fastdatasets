# @Time    : 2022/10/27 18:24
# @Author  : tk
# @FileName: kv_writer.py

import typing
import json
import pickle
import numpy as np
import data_serialize
from tfrecords import LEVELDB

__all__ = [
    'LEVELDB',
    'pickle',
    'json',
    'DataType',
    'data_serialize',
    'WriterObject',
    "StringWriter",
    "BytesWriter",
    "JsonWriter",
    "PickleWriter",
    "FeatureWriter",
    "NumpyWriter",
]

from ..common.writer_object import WriterObjectBaseForKV
from ..common.writer import serialize_numpy, serialize_tensorflow_record


class DataType:
    int64_list = 0
    float_list = 1
    bytes_list = 2


class WriterObject(WriterObjectBaseForKV):
    def __init__(self,filename,options=LEVELDB.LeveldbOptions(create_if_missing=True,error_if_exists=False)):
        self.options = options
        self.file_writer = LEVELDB.Leveldb(filename,options=options)
    def __del__(self):
        self.close()

    @property
    def get_writer(self):
        return self.file_writer

    def close(self):
        if self.file_writer is not None:
            self.file_writer.close()
            self.file_writer = None

    def put_batch(self,keys : typing.List[typing.Union[str , bytes]],values : typing.List[typing.Union[str , bytes]]):
        return self.file_writer.put_batch(keys, values)

    def put(self,key : typing.Union[bytes,str],value : typing.Union[bytes,str]):
        return self.file_writer.put(key,value)

    def get(self,key : typing.Union[bytes,str],default_value=None):
        return self.file_writer.get(key,default_value)

    def remove(self,key : typing.Union[bytes,str],):
        return self.file_writer.remove(key)



class StringWriter(WriterObject):
    def put(self, key : typing.Union[bytes,str],value : typing.Union[bytes,str],*args, **kwargs):
        return super(StringWriter, self).put(key,value)

    def put_batch(self,keys : typing.List[typing.Union[str , bytes]],values : typing.List[typing.Union[str , bytes]],*args, **kwargs):
        return super(StringWriter, self).put_batch(keys, values)

class BytesWriter(WriterObject):
    def put(self, key : typing.Union[bytes,str],value : typing.Union[bytes,str],*args, **kwargs):
        return super(BytesWriter, self).put(key,value)

    def put_batch(self, keys: typing.List[typing.Union[str, bytes]], values: typing.List[typing.Union[str, bytes]],*args, **kwargs):
        return super(BytesWriter, self).put_batch(keys, values)

class PickleWriter(WriterObject):
    def put(self, key : typing.Union[bytes,str],value,*args, **kwargs):
        return super(PickleWriter, self).put(key,pickle.dumps(value,*args,**kwargs))

    def put_batch(self, keys: typing.List[typing.Union[str, bytes]], values: typing.List[typing.Union[str, bytes]],*args, **kwargs):
        return super(PickleWriter, self).put_batch(keys, [pickle.dumps(value,*args,**kwargs) for value in values])

class JsonWriter(WriterObject):
    def put(self,key : typing.Union[bytes,str],value : typing.Union[bytes,str],*args, **kwargs):
        return super(JsonWriter, self).put(key,json.dumps(value,*args,**kwargs))

    def put_batch(self, keys: typing.List[typing.Union[str, bytes]], values: typing.List[typing.Union[str, bytes]],*args, **kwargs):
        return super(JsonWriter, self).put_batch(keys, [json.dumps(value,*args,**kwargs) for value in values])

class FeatureWriter(WriterObject):
    def put(self,key : typing.Union[bytes,str],value : typing.Dict,*args, **kwargs):
        x = serialize_tensorflow_record(value)
        return super(FeatureWriter, self).put(key,x)

    def put_batch(self, keys: typing.List[typing.Union[str, bytes]], values: typing.List[dict],*args, **kwargs):
        x_values = []
        for value in values:
            x = serialize_tensorflow_record(value)
            x_values.append(x)
        return super(FeatureWriter, self).put_batch(keys,x_values)


class NumpyWriter(WriterObject):
    def put(self,key: typing.Union[str, bytes],value : typing.Dict,*args, **kwargs):
        x = serialize_numpy(value)
        return super(NumpyWriter, self).put(key,x)

    def put_batch(self, keys: typing.List[typing.Union[str, bytes]], values: typing.List[dict],*args, **kwargs):
        x_values = []
        for value in values:
            x = serialize_numpy(value)
            x_values.append(x)
        return super(NumpyWriter, self).put_batch(keys,x_values)
