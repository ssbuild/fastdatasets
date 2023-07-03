import sys
sys.path.append('.')

import pprint
import setuptools
import os
cur_path = os.path.dirname(__file__)

package_name = 'fastdatasets'
current_dir = os.path.dirname(os.path.abspath(__file__))
title = 'fastdatasets: datasets for tfrecords'

with open(os.path.join(current_dir, 'README.md'), mode='r', encoding='utf-8') as f:
    project_description_str = f.read()

if __name__ == '__main__':
    package_list = setuptools.find_packages('./fastdatasets', exclude=['tests.*'])
    pprint.pprint(package_list)
    package_list = ['fastdatasets'] +  [ 'fastdatasets.' + p for p in package_list]

    setuptools.setup(
        name=package_name,
        version="0.9.10",
        author="ssbuild",
        author_email="9727464@qq.com",
        description=title,
        long_description_content_type='text/markdown',
        long_description=project_description_str,
        url="https://github.com/ssbuild/fastdatasets",
        packages=package_list,
        python_requires='>=3, <4',  # python的依赖关系
        install_requires=['tfrecords >= 0.2.11 , < 0.3','data_serialize >= 0.2.1','numpy'],
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
            'Programming Language :: Python :: 3.11',
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


