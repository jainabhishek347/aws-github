FROM public.ecr.aws/lambda/python:3.8

# Copy function code

ADD utils ${LAMBDA_TASK_ROOT}/utils
ADD etl_scripts ${LAMBDA_TASK_ROOT}/etl_scripts
ADD configs ${LAMBDA_TASK_ROOT}/configs

RUN ls -la ${LAMBDA_TASK_ROOT}/

COPY app.py ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .

COPY chesterfield.csv .

RUN yum install -y gcc python27 python27-devel postgresql-devel

RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]
