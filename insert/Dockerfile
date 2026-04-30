FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY insert.py "${LAMBDA_TASK_ROOT}/insert.py"

CMD ["insert.handler"]