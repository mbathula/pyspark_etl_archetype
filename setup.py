from setuptools import setup, find_packages

setup(
    name='pyspark_etl_archetype',
    version='1.0',
    packages=find_packages(),
    install_requires=[
       ## 'pyspark==3.1.2',
       ## 'delta-spark'
    ],
    entry_points={
        'console_scripts': [
            'run_etl = main:main',
        ],
    },
)