from setuptools import setup, find_packages

setup(
    name="ads_nasa_parser",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "ads_nasa_parser=main:main",
        ],
    },
)