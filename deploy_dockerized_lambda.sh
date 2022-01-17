#!/bin/bash

region=us-east-2

# create repository
aws --profile priv ecr create-repository \
    --region $region \
    --repository-name upwork/docker_lambda

# extract repository url
reg_url=****

# login to docker repository using aws profile
aws --profile priv \
  ecr get-login-password --region $region | docker login --username AWS --password-stdin $reg_url

#build docker base layer images
tag="0.3-aws"
image_name=courier
docker build . -t $image_name:tag

#extract image id to push
img_id=*****

docker tag $img_id $reg_url:$tag
docker push $reg_url:$tag

#build docker base layer images

cd second_layer_exmple

tag_second="0.1"
image_name_second=second_layer
docker build . -t $image_name_second:$tag_second

#extract image id to push
img_id_second=*****

docker tag $img_id_second $reg_url:$tag_second
docker push $reg_url:$tag_second

# create role for lambda
aws --profile priv iam create-role --role-name docker-lambda-execution-role \
  --assume-role-policy-document file://docker-lambda-execution-policy.json

# create access policy and attach it lambda role
aws --profile priv iam put-role-policy --role-name docker-lambda-execution-role \
  --policy-name docker-lambda-execution-access-policy \
  --policy-document file://docker-access-policy.json

# create lambda
aws --profile priv lambda create-function --region us-east-2 --function-name docker-lambda \
    --package-type Image  \
    --code ImageUri=*****/image_name_second:$tag_second   \
    --role arn:aws:iam::******:role/docker-lambda-execution-role
