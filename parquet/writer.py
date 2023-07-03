# -*- coding: utf-8 -*-
# @Time:  20:26
# @Author: tk
# @File：writer
import typing

import numpy as np
from tfrecords.python.io.arrow import arrow,parquet,ParquetWriter

__all__ = [
    'arrow',
    'parquet',
    'ParquetWriter',
    'AnythingWriter',
    'PythonWriter',
    'MAP_DTYPE'
]



MAP_DTYPE = {
    'int8': (arrow.int8(),arrow.Int8Builder),
    'int16': (arrow.int16(),arrow.Int16Builder),
    'int32': (arrow.int32(),arrow.Int32Builder),
    'int64': (arrow.int64(),arrow.Int64Builder),
    'int': (arrow.int32(), arrow.Int32Builder),
    'long': (arrow.int64(),arrow.Int64Builder),
    'uint8': (arrow.uint8(),arrow.UInt8Builder),
    'uint16': (arrow.uint16(),arrow.UInt16Builder),
    'uint32': (arrow.uint32(),arrow.UInt32Builder),
    'uint64': (arrow.uint64(),arrow.UInt64Builder),
    'uint': (arrow.uint32(),arrow.UInt32Builder),
    'ulong': (arrow.uint64(),arrow.UInt64Builder),
    'float16': (arrow.float16(),arrow.HalfFloatBuilder),
    'float32': (arrow.float32(),arrow.FloatBuilder),
    'float63': (arrow.float64(),arrow.DoubleType),
    'float': (arrow.float32(),arrow.FloatBuilder),
    'double': (arrow.float64(),arrow.DoubleType),
    'bytes': (arrow.binary(),arrow.BinaryType),
    'large_bytes': (arrow.large_binary(),arrow.LargeBinaryType),
    'binary': (arrow.binary(),arrow.BinaryType),
    'large_binary': (arrow.large_binary(),arrow.LargeBinaryType),
    'str': (arrow.utf8(),arrow.StringBuilder),
    'large_str': (arrow.utf8(),arrow.LargeStringType),
    'string': (arrow.large_utf8(),arrow.StringBuilder),
    'large_string': (arrow.large_utf8(),arrow.LargeStringType),
}

class PythonWriter:
    def __init__(self,filename,
                 schema: typing.Dict,
                 parquet_options: typing.Optional[typing.Dict]=None,
                 arrow_options: typing.Optional[typing.Dict]=None):
        assert len(schema)
        global MAP_DTYPE
        self.schema = arrow.schema([arrow.field(k, arrow.list(MAP_DTYPE.get(v.lower())[0])) for k, v in schema.items()])
        self.builder = {k: MAP_DTYPE.get(v.lower())[1]() for k, v in schema.items()}
        self.file_writer_ = ParquetWriter(filename,self.schema,parquet_options,arrow_options)

    def __get_values__(self, keys, values):
        values_list = []
        for k, v in zip(keys, values):
            if isinstance(v, np.ndarray):
                assert v.ndim == 1
            d = v.tolist() if isinstance(v, np.ndarray) else v
            assert len(d)
            offsets_val = [0]
            values_val = []
            pos = 0
            for value in d:
                if not isinstance(value, list):
                    value = [value]
                pos += len(value)
                offsets_val.append(pos)
                values_val.extend(value)

            builder = arrow.Int32Builder()
            builder.AppendValues(offsets_val)
            offsets_: arrow.Int32Array = builder.Finish().Value()

            builder2 = self.builder[k]
            builder2.AppendValues(values_val)
            values_ = builder2.Finish().Value()
            values_: arrow.ListArray = arrow.ListArray.FromArrays(offsets=offsets_, values=values_).Value()
            values_list.append(values_)
        return values_list

    def write_batch(self,keys,values):
        values_list =  self.__get_values__(keys,values)
        batch = arrow.RecordBatch.Make(self.schema,num_rows = values_list[0].length(),columns=values_list)
        self.file_writer_.write_record_batch(batch)

    def write_table(self,keys,values):
        values_list = self.__get_values__(keys, values)
        table = arrow.Table.Make(self.schema, arrays=values_list)
        self.file_writer_.write_table(table)

    def close(self):
        self.file_writer_.close()


AnythingWriter = PythonWriter