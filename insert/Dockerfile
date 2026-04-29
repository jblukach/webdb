FROM public.ecr.aws/lambda/python:3.13

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy handler code
COPY insert.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler
CMD ["insert.handler"]
