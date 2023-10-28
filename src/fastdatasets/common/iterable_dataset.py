# @Time    : 2022/9/3 13:19

import typing
import numpy as np
from .writer import deserialize_numpy

__all__ = [
    'IterableDatasetBase',
    'CacheIterableDataset',
    'ConcatIterableDataset',
    'MapIterableDataset',
    'FilterIterableDataset',
    'SkipIterableDataset',
    'ChoiseIterableDataset',
    'IntervalIterableDataset',
    'UnBatchIterableDataset',
    'BatchIterableDataset',
    'RepeatIterableDataset',
    'ShuffleIterableDataset'
]


def numpy_parse_transform_fn(x):
    if isinstance(x, tuple):
        serialize = x[1]
    else:
        serialize = x
    data_dict = deserialize_numpy(serialize)
    return data_dict




class IterableDatasetBase:

    def cache(self,buffer_size:int = 128):
        cls = CacheIterableDataset(self, buffer_size=buffer_size)
        return cls

    def concat(self,dataset_other_list: typing.List):
        cls = ConcatIterableDataset(self,dataset_other_list)
        return cls

    '''
        repeats element repeat numbers , -1 for forever
    '''
    def repeat(self,repeats=1):
        cls = RepeatIterableDataset(self, count=repeats)
        return cls
    def shuffle(self,buffer_size, seed=None):
        cls = ShuffleIterableDataset(self, buffer_size, seed)
        return cls

    def apply(self, transform_fn: typing.Callable):
        cls = MapIterableDataset(self, transform_fn)
        return cls

    def map(self,transform_fn: typing.Callable):
        cls = MapIterableDataset(self, transform_fn)
        return cls

    def filter(self,filter_fn: typing.Callable):
        cls = FilterIterableDataset(self, filter_fn)
        return cls

    def interval(self, n:int , size:typing.Union[int,typing.List[int]]=0):
        cls = ChoiseIterableDataset(self, n, size)
        return cls

    def choice(self,n:int , size:typing.Union[int,typing.List[int]]):
        cls = ChoiseIterableDataset(self, n, size)
        return cls


    def skip(self,n: int):
        cls = SkipIterableDataset(self, n)
        return cls

    def limit(self,n: int):
        cls = TopRandomDataset(self, n)
        return cls

    def shard(self,num_shards,index):
        cls = ChoiseIterableDataset(self, num_shards, [index])
        return cls

    def mutiprocess(self,process_num,process_id):
        cls = ChoiseIterableDataset(self, process_num, [process_id])
        return cls

    def batch(self,batch_size:int,drop_remainder:bool=False):
        cls = BatchIterableDataset(self, batch_size,drop_remainder)
        return cls

    def unbatch(self):
        cls = UnBatchIterableDataset(self)
        return cls

    def reset(self):
        raise NotImplementedError

    def close(self):
        return self.reset()

    def parse_from_numpy_writer(self,keys:typing.List[str]=None):
        cls = MapIterableDataset(self, numpy_parse_transform_fn)
        return cls


class CacheIterableDataset(IterableDatasetBase):
    def __init__(self, dataset, buffer_size:int = 128):
        self.dataset = dataset
        self.buffer_size = buffer_size
        self.buffer = []

    def reset(self):
        self.buffer.clear()
        if self.dataset:
            self.dataset.reset()

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.buffer) > 0:
            return self.buffer.pop(0)
        try:
            for i in range(self.buffer_size):
                it = next(self.dataset)
                self.buffer.append(it)
        except StopIteration:
            pass
        if len(self.buffer) > 0:
            return self.buffer.pop(0)
        raise StopIteration

class ConcatIterableDataset(IterableDatasetBase):
    def __init__(self, dataset, dataset_other_list : typing.List):
        self.dataset = dataset
        self.dataset_other_list = dataset_other_list
        self.all_dataset_list = [dataset] + dataset_other_list
        self.cur_idx = 0
        assert all([isinstance(d,typing.Iterator) for d in self.all_dataset_list]) , 'datasets must be Iterator'

    def reset(self):
        self.cur_idx = 0
        for dataset in self.all_dataset_list:
            dataset.reset()

    def __iter__(self):
        return self

    def __next__(self):
        while self.cur_idx < len(self.all_dataset_list):
            dataset = self.all_dataset_list[self.cur_idx]
            try:
                return next(dataset)
            except StopIteration:
                self.cur_idx += 1
        raise StopIteration

class MapIterableDataset(IterableDatasetBase):
    def __init__(self,dataset,transform_fn: typing.Callable = None):
      self.dataset = dataset
      self.transform_fn = transform_fn

    def reset(self):
      if self.dataset:
          self.dataset.reset()
    def __iter__(self):
      return self

    def __next__(self):
      it = next(self.dataset)
      if self.transform_fn:
          it = self.transform_fn(it)
      return it

class FilterIterableDataset(IterableDatasetBase):
    def __init__(self,dataset,filter_fn: typing.Callable = None):
      self.dataset = dataset
      self.filter_fn = filter_fn

    def reset(self):
      if self.dataset:
          self.dataset.reset()
    def __iter__(self):
      return self

    def __next__(self):
        it = next(self.dataset)
        if self.filter_fn:
          find_flag = False
          try:
              while True:
                  bok = self.filter_fn(it)
                  if bok:
                      find_flag = True
                      break
                  it = next(self.dataset)
          except StopIteration:
              if not find_flag:
                  raise StopIteration
        else:
            it = next(self.dataset)
        return it

class SkipIterableDataset(IterableDatasetBase):
    def __init__(self,dataset,n: int):
      assert n >=0
      self.dataset = dataset
      self.N = n
      self.cur_n = 0

    def reset(self):
      self.cur_n = 0
      if self.dataset:
          self.dataset.reset()
    def __iter__(self):
      return self

    def __next__(self):
        it = None
        while self.cur_n < self.N:
            self.cur_n += 1
            _ = next(self.dataset)
        it = next(self.dataset)
        return it

class TopRandomDataset(IterableDatasetBase):
    def __init__(self, dataset, n: int):
        assert n >= 0
        self.dataset = dataset
        self.N = n
        self.cur_n = 0

    def reset(self):
        self.cur_n = 0
        if self.dataset:
            self.dataset.reset()

    def __iter__(self):
        return self

    def __next__(self):
        it = None
        if self.cur_n < self.N:
            it = next(self.dataset)
        else:
            raise StopIteration
        self.cur_n += 1
        return it

class ChoiseIterableDataset(IterableDatasetBase):
    '''
    select one from N
    '''
    def __init__(self,dataset,N: int,size : typing.Union[int,typing.List[int]]):
      assert N >0
      self.is_integer = False
      if isinstance(size,list):
          size = list(set(size))
          assert len(size) < N
          assert all([_ < N and _ >= 0 for _ in size]) ,'invalid size'
      else:
          self.is_integer = True
          assert size < N

      self.dataset = dataset
      self.size = size
      self.interval = N

      self.buffer = []

    def reset(self):
      self.buffer.clear()
      if self.dataset:
          self.dataset.reset()
    def __iter__(self):
      return self

    def __next1__(self):
        it = None
        if self.interval > 0:
            n = 0
            find_flag = False
            try:
                while n < self.interval:
                    if self.size == n:
                        it = next(self.dataset)
                        find_flag = True
                    else:
                        next(self.dataset)
                    n += 1
            except StopIteration:
                if not find_flag:
                    raise StopIteration
        else:
            next(self.dataset)
        return it

    def __next2__(self):
        it = None
        if self.interval > 0:
            if len(self.buffer) > 0:
                return self.buffer.pop(0)

            n = 0
            find_flag = False
            try:
                while n < self.interval:
                    if n in self.size:
                        it = next(self.dataset)
                        find_flag = True
                        self.buffer.append(it)
                    else:
                        next(self.dataset)
                    n += 1
            except StopIteration:
                if not find_flag:
                    raise StopIteration
            if len(self.buffer) == 0:
                raise StopIteration
            it = self.buffer.pop(0)
        else:
            next(self.dataset)
        return it
    def __next__(self):
        return self.__next1__() if self.is_integer  else self.__next2__()

class IntervalIterableDataset(IterableDatasetBase):
    '''
    select one from N
    '''
    def __init__(self, dataset, n: int, index : int = 0):
      assert index >= 0
      assert n >= 0
      assert index < n
      self.dataset = dataset
      self.index = index
      self.interval = n

    def reset(self):
      if self.dataset:
          self.dataset.reset()
    def __iter__(self):
      return self

    def __next__(self):
        it = None
        if self.interval > 0:
            n = 0
            find_flag = False
            try:
                while n < self.interval:
                    if self.index == n:
                        it = next(self.dataset)
                        find_flag = True
                    else:
                        next(self.dataset)
                    n += 1
            except StopIteration:
                if not find_flag:
                    raise StopIteration
        else:
            next(self.dataset)
        return it

class UnBatchIterableDataset(IterableDatasetBase):
    def __init__(self,dataset):
        self.dataset = dataset
        d = dataset
        is_batch = False
        while True:
            if hasattr(d,'dataset'):
                if isinstance(d, BatchIterableDataset):
                    is_batch = True
                    break
            else:
                break
            d = d.dataset

        assert is_batch,'dataset must be BatchDataset'

        self.buffer = []

    def reset(self):
        if self.dataset:
            self.dataset.reset()
    def __iter__(self):
        return self

    def __next__(self):

        if self.buffer and len(self.buffer) > 0:
            return self.buffer.pop(0)

        self.buffer.extend([item for item in next(self.dataset)])
        if self.buffer and len(self.buffer) > 0:
            return self.buffer.pop(0)
        else:
            raise StopIteration

class BatchIterableDataset(IterableDatasetBase):
    def __init__(self,dataset,batch_size : int,drop_remainder:bool =False):
        self.dataset = dataset
        self.batch_size = batch_size
        self.drop_remainder = drop_remainder


    def reset(self):
        if self.dataset:
            self.dataset.reset()

    def __iter__(self):
        return self

    def __next__(self):
        batch = ()
        try:
            n = 0
            while n < self.batch_size:
                it = next(self.dataset)
                batch += (it,)
                n += 1
        except StopIteration:
            if len(batch) == 0:
                raise StopIteration

            if self.drop_remainder:
               if len(batch) != self.batch_size:
                   raise StopIteration

        return batch

class RepeatIterableDataset(IterableDatasetBase):
  """A `Dataset` that repeats its input several times."""
  def __init__(self, dataset, count):
    """See `Dataset.repeat()` for details."""
    self.count = count
    self.cur_epoch = 0
    self.dataset = dataset

  def reset(self):
      self.cur_epoch = 0
      if self.dataset:
          self.dataset.reset()

  def __iter__(self):
      return self

  def __next__(self):
      it = self.__next_ex__()
      return it

  def __next_ex__(self):
      try:
          it = next(self.dataset)
          return it
      except StopIteration:
          self.cur_epoch += 1

      if self.count > 0 and self.cur_epoch >= self.count:
          raise StopIteration
      else:
         self.dataset.reset()
         it = next(self.dataset)
         return it

class ShuffleIterableDataset(IterableDatasetBase):
    def __init__(self,dataset,buffer_size=128, seed=None):
        self.dataset = dataset
        self.buffer_size = buffer_size
        self.seed = seed
        self.buffer = []
        np.random.seed(seed)

    def reset(self):
        if self.dataset:
            self.dataset.reset()

    def __iter__(self):
        return self

    def __next__(self):
        it = self.__next_ex__()
        return it

    def __next_ex__(self):
        if self.buffer_size > 1:
            if len(self.buffer) == 0:
                try:
                    for _ in range(self.buffer_size):
                        self.buffer.append(next(self.dataset))
                except StopIteration:
                    pass
                    # warnings.warn("Number of elements in the iterator is less than the "
                    #               f"queue size (N={self.buffer_size}).")
            if len(self.buffer) == 0:
                raise StopIteration
            index = np.random.randint(0,len(self.buffer))
            return self.buffer.pop(index)
        else:
            iterator = next(self.dataset)
        return iterator
