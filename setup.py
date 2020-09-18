import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysparkdq",
    version="0.0.3",
    author="Oliver Meyer",
    author_email="meyer.oliver93@gmail.com",
    description="Lightweight PySpark DataFrame validation framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/olivermeyer/pyspark-dq",
    install_requires=[
        "pyspark>=2.1.2"
    ],
    python_requires='>=3.7',
)
