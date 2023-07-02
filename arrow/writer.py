# -*- coding: utf-8 -*-
# @Time:  20:26
# @Author: tk
# @File：writer
import typing

import numpy as np
from tfrecords.python.io.arrow import arrow,parquet,IPC_Writer



data_map = {
    'int8': (arrow.int8(),arrow.Int8Builder),
    'int16': (arrow.int16(),arrow.Int16Builder),
    'int32': (arrow.int32(),arrow.Int32Builder),
    'int64': (arrow.int64(),arrow.Int64Builder),
    'uint8': (arrow.uint8(),arrow.UInt8Builder),
    'uint16': (arrow.uint16(),arrow.UInt16Builder),
    'uint32': (arrow.uint32(),arrow.UInt32Builder),
    'uint64': (arrow.uint64(),arrow.UInt64Builder),
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

class AnythingWriter:
    def __init__(self,filename,
                 schema: typing.Dict,
                 with_stream = True,
                 options: typing.Optional[typing.Dict]=None):
        assert len(schema)
        global data_map
        self.schema = arrow.schema([arrow.field(k,data_map.get(v)[0]) for k,v in schema.items()])
        self.builder = {k:data_map.get(v)[1]() for k, v in schema.items()}
        self.file_writer_ : IPC_Writer = IPC_Writer(filename,self.schema,with_stream=with_stream,options=options)

    def write_batch(self,keys,values):
        values_list = []
        for k,v in zip(keys,values):
            builder = self.builder[k]
            builder.AppendValues(v.tolist() if isinstance(v,np.ndarray) else v)
            value = builder.Finish().Value()
            values_list.append(value)
        batch = arrow.RecordBatch.Make(self.schema,num_rows = values_list[0].length(),columns=values_list)
        self.file_writer_.write_record_batch(batch)

    def write_table(self,keys,values):
        values_list = []
        for k, v in zip(keys, values):
            builder = self.builder[k]
            builder.AppendValues(v.tolist() if isinstance(v,np.ndarray) else v)
            value = builder.Finish().Value()
            values_list.append(value)

        table = arrow.Table.Make(self.schema, arrays=values_list)

        self.file_writer_.write_table(table)

    def close(self):
        self.file_writer_.close()
