from setuptools import setup, find_packages
from distutils.spawn import find_executable
import sys
import os
import subprocess

version = '0.2.1'

# Find the Protocol Compiler.
if 'PROTOC' in os.environ and os.path.exists(os.environ['PROTOC']):
    protoc = os.environ['PROTOC']
elif os.path.exists("../src/protoc"):
    protoc = "../src/protoc"
elif os.path.exists("../src/protoc.exe"):
    protoc = "../src/protoc.exe"
elif os.path.exists("../vsprojects/Debug/protoc.exe"):
    protoc = "../vsprojects/Debug/protoc.exe"
elif os.path.exists("../vsprojects/Release/protoc.exe"):
    protoc = "../vsprojects/Release/protoc.exe"
else:
    protoc = find_executable("protoc")


def generate_proto(source):
    """Invokes the Protocol Compiler to generate a _pb2.py from the given
    .proto file.  Does nothing if the output already exists and is newer than
    the input."""

    output = source.replace(".proto", "_pb2.py").replace("../src/", "")

    if (not os.path.exists(output) or
        (os.path.exists(source) and
         os.path.getmtime(source) > os.path.getmtime(output))):
        print ("Generating %s..." % output)

        if not os.path.exists(source):
            sys.stderr.write("Can't find required file: %s\n" % source)
            sys.exit(-1)

        if protoc == None:
            sys.stderr.write(
               "protoc is not installed nor found in ../src.  Please compile it "
               "or install the binary package.\n")
            sys.exit(-1)

        protoc_command = [ protoc, "-I.", "--python_out=.", source ]
        if subprocess.call(protoc_command) != 0:
            sys.exit(-1)


if __name__ == '__main__':
    # generate necessary proto files
    generate_proto('recall/proto/rpc_meta.proto')
    generate_proto('tests/test_proto/test.proto')

    setup(name='recall',
          version=version,
          description="Python High performance RPC framework based on protobuf",
          classifiers=[
              'Development Status :: 3 - Alpha',
              'Environment :: Web Environment',
              'Intended Audience :: Developers',
              'License :: OSI Approved :: MIT License',
              'Operating System :: POSIX',
              'Programming Language :: Python',
              'Programming Language :: Python :: 2.7',
              'Topic :: Internet'
	  ],
          keywords='rpc gevent',
          author='Yaolong Huang',
          author_email='airekans@gmail.com',
          url='https://github.com/airekans/recall',
          license='MIT',
          packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
          include_package_data=True,
          zip_safe=False,
          install_requires=[
              'gevent',
              'protobuf>=2.3'
          ],
          entry_points="""
          # -*- Entry points: -*-
          """,
      )
