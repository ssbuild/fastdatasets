import sys
sys.path.append('.')

import pprint
import setuptools
import platform
import os
# import shutil

from setuptools import Extension
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py as _build_py

cur_path = os.path.dirname(__file__)

package_name = 'fastdatasets'
current_dir = os.path.dirname(os.path.abspath(__file__))
title = 'fastdatasets: datasets for tfrecords'

with open(os.path.join(current_dir, 'README.md'), mode='r', encoding='utf-8') as f:
    project_description_str = f.read()

platforms_name = sys.platform + '_' + platform.machine()


class PrecompiledExtesion(Extension):
    def __init__(self, name):
        super().__init__(name, sources=[])


class build_ext(_build_ext):
    def build_extension(self, ext):
        if not isinstance(ext, PrecompiledExtesion):
            return super().build_extension(ext)


exclude = ['setup', 'setup_wrapper', 'tests','testing']


class build_py(_build_py):
    def find_package_modules(self, package, package_dir):
        modules = super().find_package_modules(package, package_dir)
        return [(pkg, mod, file,) for (pkg, mod, file,) in modules if mod not in exclude]



if __name__ == '__main__':
    package_list = setuptools.find_packages('./fastdatasets', exclude=['tests.*'])
    pprint.pprint(package_list)
    package_list = ['fastdatasets'] +  [ 'fastdatasets.' + p for p in package_list]

    setuptools.setup(
        platforms=platforms_name,
        name=package_name,
        version="0.9.7",
        author="ssbuild",
        author_email="9727464@qq.com",
        description=title,
        long_description_content_type='text/markdown',
        long_description=project_description_str,
        url="https://github.com/ssbuild/fastdatasets",
        packages=package_list,
        # package_dir=package_dir_list,
        # packages=['fastdatasets','fastdatasets.datasets','fastdatasets.python'],   # 指定需要安装的模块
        include_package_data=True,
        package_data={'': ['*.pyd', '*.dll', '*.so', '*.h', '*.c', '*.java','*.md']},
        #ext_modules=[PrecompiledExtesion('fastdatasets')],
        cmdclass={'build_ext': build_ext, 'build_py': build_py},
        # data_files =[('',["nn_sdk/easy_tokenizer.so","nn_sdk/engine_csdk.so"])],
        # packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
        # py_modules=["six"], # 剔除不属于包的单文件Python模块
        # install_requires=['peppercorn'], # 指定项目最低限度需要运行的依赖项
        python_requires='>=3, <4',  # python的依赖关系
        install_requires=['tfrecords >= 0.2.6 , < 0.3','data_serialize >= 0.2.1','numpy'],
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Intended Audience :: Education',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: C++',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Topic :: Scientific/Engineering',
            'Topic :: Scientific/Engineering :: Mathematics',
            'Topic :: Scientific/Engineering :: Artificial Intelligence',
            'Topic :: Software Development',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules',
        ],
        license='Apache 2.0',
        keywords=[package_name, "fastdatasets", 'tfrecords', 'dataset', 'datasets'],
    )


