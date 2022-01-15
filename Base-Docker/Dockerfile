# Dockerfile for building an AWS lambda base layer for Courier
FROM scratch
LABEL base.image="scratch"
LABEL maintainer="Will Cassada <will@cassidys.biz>"
LABEL version="1.0.0"
LABEL software="python, aws, courier"
LABEL software.version="1.0.0"
LABEL description="A container built for Code of Entry's Courier service."
LABEL license="https://www.gnu.org/licenses/gpl-3.0.en.html"
MAINTAINER Will Cassada <will@cassidys.biz>

ADD aws-lambda-base-images/x86_64/6477e2622fc9cfc61ad61cfa8be73065304c7a2cec656459e7ca50d761ff0dbd.tar.xz /
ADD aws-lambda-base-images/x86_64/affa51231f66758474a27ce54d753269d8b2781a13bb4bfa88521080d61c725d.tar.xz /
ADD aws-lambda-base-images/x86_64/da24311780c6f8793f350ca69b2f4293f3b19fea40ca94c89da6a0ce281d40f5.tar.xz /
ADD aws-lambda-base-images/x86_64/df1328a2b32e4671b19ca197caff84425b8923f2c431eddbb28b3dc20f97e36f.tar.xz /
ADD aws-lambda-base-images/x86_64/df7c0f6cee35a7a49f4390a3e65664398ae88799ecc33f5ad333069589cd3be9.tar.xz /
ADD aws-lambda-base-images/x86_64/e1f0289596ce2fb146cc64e271424dc86474c620e6a4232af219d749ba247e21.tar.xz /

ENV LANG=en_US.UTF-8
ENV TZ=:/etc/localtime
ENV PATH=/var/lang/bin:/usr/local/bin:/usr/bin/:/bin:/opt/bin
ENV LD_LIBRARY_PATH=/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib
ENV LAMBDA_TASK_ROOT=/var/task
ENV LAMBDA_RUNTIME_DIR=/var/runtime

WORKDIR /var/task

ENTRYPOINT ["/lambda-entrypoint.sh"]
