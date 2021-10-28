from setuptools import find_packages, setup

setup(
    name="courier",
    packages=find_packages(exclude=["courier.scripts"]),
    version="0.1.2",
    description="Courier Runner Library",
    author="Jacob Rodgers [yudjinn]",
    python_requires=">=3.8",
    install_requires=[
        "aiobotocore==1.4.2",
        "aiohttp==3.7.4.post0",
        "aioitertools==0.8.0",
        "anyio==3.3.4",
        "async-timeout==3.0.1",
        "atomicwrites==1.4.0",
        "attrs==21.2.0",
        "boto3==1.17.106",
        "botocore==1.20.106",
        "certifi==2021.10.8",
        "cffi==1.15.0",
        "chardet==4.0.0",
        "charset-normalizer==2.0.7",
        "click==8.0.3",
        "colorama==0.4.4",
        "cryptography==35.0.0",
        "fastapi==0.70.0",
        "fsspec==2021.8.1",
        "greenlet==1.1.2",
        "idna==3.3",
        "iniconfig==1.1.1",
        "jmespath==0.10.0",
        "multidict==5.2.0",
        "mypy-extensions==0.4.3",
        "mysql==0.0.3",
        "mysqlclient==2.0.3",
        "numpy==1.21.2",
        "packaging==21.0",
        "pandas==1.3.2",
        "pathspec==0.9.0",
        "phonenumbers==8.12.31",
        "platformdirs==2.4.0",
        "pluggy==1.0.0",
        "py==1.10.0",
        "pycparser==2.20",
        "pydantic==1.8.2",
        "pyodbc==4.0.30",
        "pyparsing==3.0.2",
        "python-dateutil==2.8.1",
        "python-dotenv==0.19.0",
        "pytz==2021.1",
        "regex==2021.10.23",
        "requests==2.26.0",
        "responses==0.14.0",
        "s3fs==2021.8.1",
        "s3transfer==0.4.2",
        "six==1.15.0",
        "sniffio==1.2.0",
        "SQLAlchemy==1.4.26",
        "sqlalchemy2-stubs==0.0.2a18",
        "sqlmodel==0.0.4",
        "starlette==0.16.0",
        "toml==0.10.2",
        "tomli==1.2.2",
        "typing==3.7.4.3",
        "typing-extensions==3.10.0.2",
        "urllib3==1.26.7",
        "wrapt==1.13.2",
        "yarl==1.7.0",
    ],
)