from setuptools import setup


setup(
    name="libmc-ctypes",
    version = "0.1",
    author = "Davies Liu",
    author_email = "davies.liu@gmail.com",
    description=("A libmemcached wrapper that uses ctypes, aims to be a drop-in "
        "replacement for clibmemcached"),
    packages=["libmc"],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Memcached",
    ],
    test_suite="test",
)
