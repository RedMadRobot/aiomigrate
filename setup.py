"""Setuptools script."""

import codecs

import setuptools

with codecs.open('README.md', encoding='utf-8') as readme:
    LONG_DESCRIPTION = readme.read()

setuptools.setup(
    name='aiomigrate',
    version='0.0.1',
    author='RedMadRobot, LLC',
    author_email='contact-backend@redmadrobot.ru',
    description='asyncio raw sql migration tool',
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license='MIT',
    url='https://github.com/RedMadRobot/aiomigrate',
    packages=setuptools.find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"],
    ),
    entry_points={
        'console_scripts': [
            'migrate = aiomigrate.run:main',
        ],
    },
    package_data={
        'aiomigrate': [],
    },
    zip_safe=True,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Database',
        'Topic :: Utilities',
        'Typing :: Typed',
    ],
)
