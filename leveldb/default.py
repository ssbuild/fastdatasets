# -*- coding: utf-8 -*-
# @Time:  19:57
# @Author: tk
# @File：default

from tfrecords import LEVELDB

global_default_options = LEVELDB.LeveldbOptions(create_if_missing=False, error_if_exists=False)