# example docker file created from courier base image

FROM courier:0.4-aws

WORKDIR "${LAMBDA_TASK_ROOT}"

# copy code
COPY . .

# set lambda handler (as a command to aws entrypoint script from aws base python image)
CMD [ "sec_layer_app.handler" ]

