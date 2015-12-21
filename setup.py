from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='funniest',
      version='0.1',
      description='Creates historical usage slides based on Cloudera Manager statistics',
      long_description=readme(),
      classifiers=[],
      url='http://github.com/bkvarda/capycityplanr',
      author='Brandon Kvarda',
      author_email='bkvarda@cloudera.com',
      license='MIT',
      packages=['capycityplanr'],
      install_requires=[],
      test_suite='pytest',
      test_require=['pytest'],
      zip_safe=False)
      
