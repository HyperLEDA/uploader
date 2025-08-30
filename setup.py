from setuptools import setup, find_packages

setup(
    name="hyperleda-uploader",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click",
        "hyperleda",
        "structlog",
    ],
    entry_points={
        "console_scripts": [
            "hyperleda-upload=cli:cli",
        ],
    },
    python_requires=">=3.8",
    author="HyperLEDA Team",
    description="A tool for uploading data to HyperLEDA",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
