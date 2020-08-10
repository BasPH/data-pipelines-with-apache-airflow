FROM python:3.7-alpine
RUN apk update && \
    apk add postgresql-dev gcc python3-dev musl-dev

COPY app.py requirements.txt /app/
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["app.py"]
