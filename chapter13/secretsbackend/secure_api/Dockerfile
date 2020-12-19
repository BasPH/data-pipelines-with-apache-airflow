FROM python:3.8.2-slim

RUN pip install Flask~=1.1.2
COPY app.py /app/app.py
RUN chmod +x /app/app.py

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["/app/app.py"]
