# -*- coding: utf-8 -*-
# @Time    : 2022/11/22 8:59
import typing
import data_serialize
import numpy as np

__all__ = [
    'DataType',
    'serialize_tensorflow_record',
    'deserialize_tensorflow_record',
    'serialize_numpy',
    'deserialize_numpy',
]



class DataType:
    int64_list = 0
    float_list = 1
    bytes_list = 2


def serialize_tensorflow_record(data: typing.Dict):
    assert data is not None
    dict_data = {}
    for k, v in data.items():
        val = v['data']
        if v['dtype'] == DataType.int64_list:
            dict_data[k] = data_serialize.Feature(int64_list=data_serialize.Int64List(value=val))
        elif v['dtype'] == DataType.float_list:
            dict_data[k] = data_serialize.Feature(float_list=data_serialize.FloatList(value=val))
        elif v['dtype'] == DataType.bytes_list:
            dict_data[k] = data_serialize.Feature(bytes_list=data_serialize.BytesList(value=val))
        else:
            raise ValueError('serialize_tensorflow_record:not support data type',k,v['dtype'])

    feature = data_serialize.Features(feature=dict_data)
    example = data_serialize.Example(features=feature)
    return example.SerializeToString()

def deserialize_tensorflow_record(data: typing.Dict,meta: typing.Dict[typing.AnyStr,DataType]):
    example = data_serialize.Example()
    example.ParseFromString(data)
    feature = example.features.feature
    data_dict = {}
    for k,dtype in meta.items():
        if dtype == DataType.int64_list:
            val = feature[k].int64_list
        elif dtype == DataType.float_list:
            val = feature[k].float_list
        elif dtype == DataType.bytes_list:
            val = feature[k].bytes_list
        else:
            raise ValueError('deserialize_tensorflow_record:not support data type', k, dtype)
        data_dict[k] = list(val.value)
    return data_dict

def serialize_numpy(data: typing.Dict):
    assert data is not None,ValueError('numpy_serialize_string: data is None')
    dict_data = {}
    for k, v in data.items():
        assert isinstance(v, np.ndarray), 'assert error in {}'.format(k)
        if v.dtype.kind == 'i':
            value_key = 'int64'
            val = v.reshape((-1,)).tolist()
        elif v.dtype.kind == 'b':
            value_key = 'int64'
            val = v.reshape((-1,)).astype(np.int64).tolist()
        elif v.dtype == np.float32:
            value_key = 'float32'
            val = v.reshape((-1,)).tolist()
        elif v.dtype == np.float or v.dtype == np.float64:
            value_key = 'float64'
            val = v.reshape((-1,)).tolist()
        elif v.dtype.kind == 'S':
            value_key = 'bytes'
            val = v.tobytes()
        else:
            raise Exception('serialize_numpy: no support dtype', k, v.dtype)
        kwargs_data = {
            "header": '',
            "dtype": str(v.dtype),
            "shape": list(v.shape),
            value_key: val,
        }
        dict_data[k] = data_serialize.NumpyObject(**kwargs_data)
    example = data_serialize.NumpyObjectMap(numpyobjects=dict_data)
    return example.SerializeToString()

def deserialize_numpy(data: typing.AnyStr, keys: typing.List = None):
    example = data_serialize.NumpyObjectMap()
    example.ParseFromString(data)
    data_dict = {}
    for k, v in example.numpyobjects.items():
        if keys is not None:
            if k not in keys:
                continue
        dtype = np.dtype(v.dtype)
        if dtype.kind == 'i':
            val = np.asarray(v.int64, dtype=dtype)
            if len(v.shape) > 1:
                val = val.reshape(v.shape)
        elif dtype == np.float32:
            val = np.asarray(v.float32, dtype=dtype)
            if len(v.shape) > 1:
                val = val.reshape(v.shape)
        elif dtype == np.float64:
            val = np.asarray(v.float64, dtype=dtype)
            if len(v.shape) > 1:
                val = val.reshape(v.shape)
        elif dtype.kind == 'S':
            val = np.asarray(v.bytes, dtype=dtype)
        else:
            raise Exception('deserialize_numpy: no support dtype', k, dtype)
        data_dict[k] = val
    return data_dict





