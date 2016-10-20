from distutils.core import setup
from Cython.Build import cythonize

setup(name='coad',
      version='1.1',
      description='Class-Object-Attribute Data manipution for Plexos',
      author='Harry Sorensen',
      author_email='harry.sorensen@nrel.gov',
      packages=['coad'],
      ext_modules=cythonize("coad/compress_interval.pyx")
    )
