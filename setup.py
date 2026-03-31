"""Data Contract Enforcer - Package configuration."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="data-contract-enforcer",
    version="1.0.0",
    author="Tsegay",
    author_email="tsegayassefa27@gmail.com",
    description="Enterprise-grade data contract enforcement with schema integrity and lineage attribution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TsegayIS122123/data-contract-enforcer",
    packages=find_packages(exclude=["tests", "docs"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Data Engineers",
        "Topic :: Software Development :: Testing",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "contract-generator=contracts.generator:main",
            "contract-runner=contracts.runner:main",
            "contract-attributor=contracts.attributor:main",
            "contract-analyzer=contracts.schema_analyzer:main",
            "contract-ai=contracts.ai_extensions:main",
            "contract-report=contracts.report_generator:main",
        ],
    },
)
