import os
from setuptools import find_packages,setup


with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md'), mode='r', encoding='utf-8') as f:
    project_description_str = f.read()

if __name__ == '__main__':
    setup(
        name="fastdatasets",
        version="0.9.17",
        author="ssbuild",
        author_email="9727464@qq.com",
        description='fastdatasets: datasets for tfrecords',
        long_description_content_type='text/markdown',
        long_description=project_description_str,
        url="https://github.com/ssbuild/fastdatasets",
        package_dir={"": "src"},
        packages=find_packages("src"),
        include_package_data=True,
        package_data={"": ["**/*.cu", "**/*.cpp", "**/*.cuh", "**/*.h", "**/*.pyx"]},
        python_requires='>=3, <4',  # python的依赖关系
        install_requires=['tfrecords >= 0.2.16 , < 0.3','data_serialize >= 0.2.1','numpy'],
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
            'Programming Language :: Python :: 3.12',
            'Topic :: Scientific/Engineering',
            'Topic :: Scientific/Engineering :: Mathematics',
            'Topic :: Scientific/Engineering :: Artificial Intelligence',
            'Topic :: Software Development',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules',
        ],
        license='Apache 2.0',
        keywords=["fastdatasets", 'tfrecords', 'dataset', 'datasets'],
    )


