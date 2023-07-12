from setuptools import setup,find_packages
from typing import List



def get_requirements_list()->List[str]:

    with open('requirements.txt') as requirement_file:
        requirement_file.readlines().remove('-e .')
setup(
    name='housing predictor',
    version='0.0.1',
    author='pratik',
    packages=find_packages(),
    install_requires=get_requirements_list()
)


if __name__ == 'main':
    print(get_requirements_list())
