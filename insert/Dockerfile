FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir \
    "pyiceberg[s3tables]>=0.8.0,<1.0.0" \
    pandas \
    pyarrow \
    boto3

COPY insert.py ${LAMBDA_TASK_ROOT}

CMD ["insert.handler"]