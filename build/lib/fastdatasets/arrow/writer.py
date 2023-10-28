# -*- coding: utf-8 -*-
# @Time:  20:26
# @Author: tk
# @File：writer
import typing

import numpy as np
from tfrecords.python.io.arrow import arrow,parquet,IPC_Writer


__all__ = [
    'arrow',
    'parquet',
    'IPC_Writer',
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
    'float64': (arrow.float64(),arrow.DoubleBuilder),
    'float': (arrow.float32(),arrow.FloatBuilder),
    'double': (arrow.float64(),arrow.DoubleBuilder),
    'bytes': (arrow.binary(),arrow.BinaryBuilder),
    'large_bytes': (arrow.large_binary(),arrow.LargeBinaryBuilder),
    'binary': (arrow.binary(),arrow.BinaryBuilder),
    'large_binary': (arrow.large_binary(),arrow.LargeBinaryBuilder),
    'str': (arrow.utf8(),arrow.StringBuilder),
    'large_str': (arrow.utf8(),arrow.LargeBinaryBuilder),
    'string': (arrow.large_utf8(),arrow.StringBuilder),
    'large_string': (arrow.large_utf8(),arrow.LargeStringBuilder),
    'int8_list':(arrow.list(arrow.int8()),arrow.Int8Builder),
    'int16_list':(arrow.list(arrow.int16()),arrow.Int16Builder),
    'int32_list':(arrow.list(arrow.int32()),arrow.Int32Builder),
    'int64_list': (arrow.list(arrow.int64()),arrow.Int64Builder),
    'int_list':(arrow.list(arrow.int32()),arrow.Int32Builder),
    'long_list':(arrow.list(arrow.int64()),arrow.Int64Builder),
    'uint8_list': (arrow.list(arrow.int8()), arrow.UInt8Builder),
    'uint16_list': (arrow.list(arrow.int16()), arrow.UInt16Builder),
    'uint32_list': (arrow.list(arrow.int32()), arrow.UInt32Builder),
    'uint64_list': (arrow.list(arrow.int64()), arrow.UInt64Builder),
    'uint_list':(arrow.list(arrow.uint32()),arrow.UInt32Builder),
    'ulong_list':(arrow.list(arrow.uint64()),arrow.UInt64Builder),
    'float16_list': (arrow.list(arrow.float16()), arrow.HalfFloatBuilder),
    'float32_list': (arrow.list(arrow.float32()), arrow.FloatBuilder),
    'float64_list': (arrow.list(arrow.float64()), arrow.DoubleBuilder),
    'float_list': (arrow.list(arrow.float32()), arrow.FloatBuilder),
    'double_list': (arrow.list(arrow.float64()), arrow.DoubleBuilder),
    'str_list': (arrow.list(arrow.utf8()), arrow.StringBuilder),
    'large_str_list': (arrow.list(arrow.large_utf8()), arrow.LargeStringBuilder),
    'binary_list': (arrow.list(arrow.binary()), arrow.BinaryBuilder),
    'large_binary_list': (arrow.list(arrow.large_binary()), arrow.LargeBinaryBuilder),
    'bytes_list': (arrow.list(arrow.binary()), arrow.BinaryBuilder),
    'large_bytes_list': (arrow.list(arrow.large_binary()), arrow.LargeBinaryBuilder),
    'map': (arrow.map(arrow.utf8(), arrow.utf8()), arrow.StringBuilder),
    'large_map': (arrow.map(arrow.utf8(), arrow.utf8()), arrow.LargeStringBuilder),
    'map_list':(arrow.list(arrow.map(arrow.utf8(),arrow.utf8())),arrow.StringBuilder),
    'large_map_list':(arrow.list(arrow.map(arrow.large_utf8(),arrow.large_utf8())),arrow.LargeStringBuilder),
}


def build_string_array(arr):
    b = arrow.StringBuilder()
    b.AppendValues(list(arr))
    return b.Finish().Value()

def build_int32_array(arr):
    b = arrow.Int32Builder()
    b.AppendValues(list(arr))
    return b.Finish().Value()

class PythonWriter:
    def __init__(self,filename,
                 schema: typing.Dict,
                 with_stream = True,
                 options: typing.Optional[typing.Dict]=None):
        assert len(schema)
        global MAP_DTYPE
        self.raw_schema = {k:MAP_DTYPE.get(v.lower())[0] for k,v in schema.items()}
        self.schema = arrow.schema([arrow.field(k,MAP_DTYPE.get(v.lower())[0]) for k,v in schema.items()])
        self.builder = {k:MAP_DTYPE.get(v.lower())[1]() for k, v in schema.items()}
        self.file_writer_ : IPC_Writer = IPC_Writer(filename,self.schema,with_stream=with_stream,options=options)

    def _build_batch_list_data(self, key, batch):
        offsets_val = [0]
        values_val = []
        pos = 0
        for value in batch:
            if not isinstance(value, list):
                value = [value]
            pos += len(value)
            offsets_val.append(pos)
            values_val.extend(value)

        builder = arrow.Int32Builder()
        builder.AppendValues(offsets_val)
        offsets_: arrow.Int32Array = builder.Finish().Value()

        builder2 = self.builder[key]
        builder2.AppendValues(values_val)
        values_ = builder2.Finish().Value()
        values_: arrow.ListArray = arrow.ListArray.FromArrays(offsets=offsets_, values=values_).Value()
        return values_

    def _build_batch_map_data(self, key, batch_values):
        ks,vs = [],[]
        offsets_,offsets2_  = [0],[0]
        pos,pos2 = 0,0
        for value in batch_values:
            if isinstance(value, list):
                for sub in value:
                    pos += len(value[0].keys())
                    offsets_.append(pos)
                    build_string_array(sub.keys())
                    ks.extend(list(sub.keys()))
                    vs.extend(list(sub.values()))

                pos2 += len(value)
                offsets2_.append(pos2)
            else:
                assert isinstance(value, dict)
                pos += len(value.keys())
                offsets_.append(pos)
                build_string_array(value.keys())
                ks.extend(list(value.keys()))
                vs.extend(list(value.values()))

        ks = build_string_array(ks)
        vs = build_string_array(vs)

        offsets = build_int32_array(offsets_)
        values_ = arrow.MapArray.FromArrays(offsets, ks, vs).Value()

        if isinstance(batch_values[0], list):
            offsets = build_int32_array(offsets2_)
            values_ = arrow.ListArray.FromArrays(offsets, values_).Value()
        return values_

    def __get_values__(self, keys, values):
        values_list = []
        for k, batch_values in zip(keys, values):
            if isinstance(batch_values, np.ndarray):
                assert batch_values.ndim == 1
            batch_values = batch_values.tolist() if isinstance(batch_values, np.ndarray) else batch_values
            assert len(batch_values)
            if self.raw_schema[k].id() == arrow.Type.LIST:
                if isinstance(batch_values[0], list) and isinstance(batch_values[0][0], dict):
                    values_ = self._build_batch_map_data(k, batch_values)
                else:
                    values_ = self._build_batch_list_data(k, batch_values)
            elif self.raw_schema[k].id() == arrow.Type.MAP:
                values_ = self._build_batch_map_data(k, batch_values)
            else:
                builder = self.builder[k]
                builder.AppendValues(batch_values)
                values_ = builder.Finish().Value()
            values_list.append(values_)
        return values_list

    def write_batch(self,keys,values):
        values_list = self.__get_values__(keys,values)
        batch = arrow.RecordBatch.Make(self.schema,num_rows = values_list[0].length(),columns=values_list)
        return self.file_writer_.write_record_batch(batch)

    def write_table(self,keys,values):
        values_list = self.__get_values__(keys, values)
        table = arrow.Table.Make(self.schema, arrays=values_list)
        return self.file_writer_.write_table(table)

    def get_file_writer(self):
        return self.file_writer_

    def close(self):
        self.file_writer_.close()



AnythingWriter = PythonWriter