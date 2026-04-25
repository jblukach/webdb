FROM public.ecr.aws/lambda/python:3.13

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    boto3

COPY insert.py ${LAMBDA_TASK_ROOT}

CMD ["insert.handler"]