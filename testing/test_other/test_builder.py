# -*- coding: utf-8 -*-
# @Time:  11:12
# @Author: tk
# @File：test_load
import typing
from pprint import pprint

import numpy as np
from tfrecords.python.io.arrow import arrow,parquet,IPC_Writer

def build_string_array(arr):
    b = arrow.StringBuilder()
    b.AppendValues(list(arr))
    return b.Finish().Value()

def build_int32_array(arr):
    b = arrow.Int32Builder()
    b.AppendValues(list(arr))
    return b.Finish().Value()

def _build_batch_map_data( key, batch):
    print("!!!!!!!!!!",batch)
    offsets_val = [0]
    k_, v_ = [], []
    pos = 0
    for item in batch:
        assert isinstance(item, dict)
        pos += len(item)
        k_.extend(list(item.keys()))
        v_.extend(list(item.values()))
        offsets_val.append(pos)

    builder = arrow.Int32Builder()
    builder.AppendValues(offsets_val)
    offsets_: arrow.Int32Array = builder.Finish().Value()

    builder = arrow.StringBuilder()
    builder.AppendValues(k_)
    k_ = builder.Finish().Value()

    builder = arrow.StringBuilder()
    builder.AppendValues(v_)
    v_ = builder.Finish().Value()

    values_: arrow.MapArray = arrow.MapArray.FromArrays(offsets_, k_, v_).Value()
    return values_


# d单条


batch_values = [
    [
        {
            "a": "aa",
            "b": "bb",
        },
        {
            "a": "aa2",
            "b": "bb2",
        }
    ],

]


ks = []
vs = []
for value in batch_values:
    for sub in value:
        ks.extend(list(sub.keys()))
        vs.extend(list(sub.values()))

ks = build_string_array(ks)
vs = build_string_array(vs)
print(ks)
print(vs)


offsets = build_int32_array([0,2,4])
x = arrow.MapArray.FromArrays(offsets,ks,vs).Value()

offsets = build_int32_array([0,2])
x = arrow.ListArray.FromArrays(offsets,x).Value()

print(x)

print(x.length())
# values_: arrow.StructArray = arrow.StructArray.Make(vs, ks).Value()
#
# # arrow.RecordBatch.
# # arrow.RecordBatch.FromStructArray()
# print(values_)
#
# print(values_.length())