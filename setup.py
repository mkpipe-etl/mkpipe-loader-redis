from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-redis',
    version='0.2.0',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe', 'redis'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'redis = mkpipe_loader_redis:RedisLoader',
        ],
    },
    description='Redis loader for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
