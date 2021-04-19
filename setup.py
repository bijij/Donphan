from setuptools import setup
import re

requirements = []
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

version = ""
with open("donphan/__init__.py") as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError("version is not set")

readme = ""
with open("README.rst") as f:
    readme = f.read()

setup(
    name="donphan",
    author="bijij",
    url="https://github.com/bijij/donphan",
    project_urls={
        "Documentation": "https://donphan.readthedocs.io/",
        "Issue tracker": "https://github.com/bijij/donphan/issues",
    },
    version=version,
    packages=["donphan"],
    license="MIT",
    description="Asyncronous Database ORM for Postgres",
    long_description=readme,
    long_description_content_type="text/x-rst",
    include_package_data=True,
    install_requires=requirements,
    extras_require={
        "docs": [
            "sphinx==3.2.1",
            "sphinxcontrib_trio==1.1.2",
            "sphinxcontrib-websupport",
        ],
        "test": ["flake8>=3.9.0", "pytest>=6.2.0", "mypy"],
    },
    python_requires=">=3.7.2",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
)
