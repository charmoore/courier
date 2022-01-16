FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y python-devel mysql-devel
RUN yum install -y gcc-c++
RUN yum install -y yum install unixODBC-devel

WORKDIR "${LAMBDA_TASK_ROOT}"

COPY ./ "${LAMBDA_TASK_ROOT}"/


RUN pip install -U pip wheel setuptools egg
RUN pip install  --no-cache-dir -r requirements.txt

RUN ls -la ${LAMBDA_TASK_ROOT}

CMD [ "courier.main.main" ]

