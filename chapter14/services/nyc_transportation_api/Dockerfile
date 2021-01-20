FROM python:3.7-alpine
RUN apk update && \
    apk add postgresql-dev gcc python3-dev musl-dev

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY app.py /app/
COPY templates /app/templates
WORKDIR /app
EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["app.py"]
