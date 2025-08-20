from setuptools import setup, find_packages

setup(
    name="phd_agent",
    version="0.1.0",
    description="PhD Research Agent - ADS Analysis Toolkit for astronomical literature analysis",
    author="PhD Researcher",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.25.1",
        "python-dotenv>=1.0.0",
        "numpy>=2.3.0",
        "pandas>=2.3.0",
        "pytest>=6.2.4",
        "wordcloud>=1.9.0",
        "matplotlib>=3.7.0",
        "jupyter>=1.0.0",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "phd_agent=ads_parser:main",
        ],
    },
)