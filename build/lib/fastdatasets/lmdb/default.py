# -*- coding: utf-8 -*-
# @Time:  20:04
# @Author: tk
# @File：default
from tfrecords import LMDB

global_default_options = LMDB.LmdbOptions( env_open_flag = LMDB.LmdbFlag.MDB_RDONLY,
                env_open_mode = 0o664, # 8进制表示
                txn_flag = LMDB.LmdbFlag.MDB_RDONLY,
                dbi_flag = 0,
                put_flag = 0)


global_default_options2 = LMDB.LmdbOptions( env_open_flag = LMDB.LmdbFlag.MDB_RDONLY,
                env_open_mode = 0o664, # 8进制表示
                txn_flag = 0,
                dbi_flag = 0,
                put_flag = 0)