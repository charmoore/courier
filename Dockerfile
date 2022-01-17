FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y python-devel \
  mysql-devel \
  gcc-c++ \
  unixODBC-devel

RUN pip install -U pip wheel setuptools egg

WORKDIR /build_dir

COPY . .

RUN python setup.py bdist_wheel

FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y python-devel \
  mysql-devel \
  gcc-c++ \
  unixODBC-devel

WORKDIR "${LAMBDA_TASK_ROOT}"

COPY --from=0 /build_dir/dist/*whl ./

RUN pip install -U pip wheel setuptools egg
RUN pip install *whl


