# coding=utf8
# @Time    : 2023/10/28 11:48
# @Author  : tk
# @FileName: test_hf

from fastdatasets.parquet.dataset import load_dataset


# hf common_voice
ds = load_dataset.RandomDataset(r"F:\tmp\common_voice-validated-00003-of-00004.parquet")

for i in range(len(ds)):
    d = ds[i]
    print(d.keys())

    print( d["audio.bytes"])

    print(d["sentence"])
    with open('./test.mp3',mode='wb') as f:
        f.write(d["audio.bytes"])
    break