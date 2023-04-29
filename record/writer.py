# @Time    : 2022/9/18 23:07
# @Author  : tk
# @FileName: simple_record.py
import json
import pickle
import typing
import numpy as np
import data_serialize

from ..common.writer_object import WriterObjectBaseForRecord
from ..common.writer import serialize_numpy, serialize_tensorflow_record
from tfrecords import TFRecordOptions,TFRecordCompressionType,TFRecordWriter,RECORD

__all__ = [
    "data_serialize",
    'pickle',
    'json',
    'RECORD',
    "DataType",
    "TFRecordOptions",
    "TFRecordCompressionType",
    "TFRecordWriter",
    "WriterObject",
    "StringWriter",
    "BytesWriter",
    "PickleWriter",
    "FeatureWriter",
    "NumpyWriter",
]


class DataType:
    int64_list = 0
    float_list = 1
    bytes_list = 2


class WriterObject(WriterObjectBaseForRecord):
    def __init__(self, filename, options=TFRecordOptions(compression_type='GZIP')):
        self.filename = filename
        self.options = options
        self.file_writer = TFRecordWriter(filename, options=options)

    def __del__(self):
        self.close()

    @property
    def get_writer(self):
        return self.file_writer

    def close(self):
        if self.file_writer is not None:
            self.file_writer.close()

    def write(self, data, *args, **kwargs):
        return self.file_writer.write(data)

    def write_batch(self,data, *args, **kwargs):
        return self.file_writer.write_batch(data)

    def flush(self):
        return self.file_writer.flush()

    def __enter__(self):
        if  self.file_writer is None:
            self.file_writer = TFRecordWriter(self.filename, options=self.options)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False



class StringWriter(WriterObject):
    def write(self, data,*args, **kwargs):
        return super(StringWriter, self).write(data)

    def write_batch(self, data,*args, **kwargs):
        return super(StringWriter, self).write_batch(data)

class BytesWriter(WriterObject):
    def write(self, data,*args, **kwargs):
        return super(BytesWriter, self).write(data)

    def write_batch(self, data, *args, **kwargs):
        return super(BytesWriter, self).write_batch(data)

class PickleWriter(WriterObject):
    def write(self, data,*args, **kwargs):
        return super(PickleWriter, self).write(pickle.dumps(data,*args,**kwargs))

    def write_batch(self, data, *args, **kwargs):
        return super(PickleWriter, self).write_batch([pickle.dumps(d,*args,**kwargs) for d in data])

class JsonWriter(WriterObject):
    def write(self, data,*args, **kwargs):
        super(JsonWriter, self).write(json.dumps(data,*args,**kwargs))

    def write_batch(self, data, *args, **kwargs):
        return super(JsonWriter, self).write_batch([json.dumps(d,*args,**kwargs) for d in data])

class FeatureWriter(WriterObject):
    def write(self,data : typing.Dict,*args, **kwargs):
        x = serialize_tensorflow_record(data)
        return super(FeatureWriter, self).write(x)

    def write_batch(self, data, *args, **kwargs):
        x_values = []
        for feature in data:
            x = serialize_tensorflow_record(feature)
            x_values.append(x)
        return super(FeatureWriter, self).write_batch(x_values)



class NumpyWriter(WriterObject):
    def write(self,data : typing.Dict,*args, **kwargs):
        x = serialize_numpy(data)
        return super(NumpyWriter, self).write(x)

    def write_batch(self, data: typing.List[dict], *args, **kwargs):
        x_values = []
        for feature in data:
            x = serialize_numpy(feature)
            x_values.append(x)
        return super(NumpyWriter, self).write_batch(x_values)
