FROM python:3.9-slim

WORKDIR /usr/src/app

RUN apt-get update && \
  apt-get install -y default-libmysqlclient-dev && \
  apt-get install -y gcc && \
  apt-get install -y --reinstall build-essential && \
  apt-get install -y unixodbc-dev

# optional - if not installed already, libraries will be installed on setup.py step
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -U pip wheel setuptools egg

COPY . .

# install from setup.py
RUN python setup.py bdist_wheel
RUN pip install dist/*whl

# test invocation based on README.md file
CMD [ "python", "-c", "from courier import process" ]

