from setuptools import setup, find_packages

setup(
    name='pyrawcore',
    version='0.0.0',
    description='',
    author='Miguel Branco',
    author_email='miguel.branco@epfl.ch',
    packages=find_packages(),
    install_requires = [
        'cloud',
        'enum34',
        'pandas',
        'prettytable',
        'psycopg2>=2.5.2',
        'xlrd',
    ]
)
