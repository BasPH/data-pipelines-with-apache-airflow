# Tell docker which image to use as base.
FROM python:3.8-slim

# Copy requirements file and run pip to install requirements.
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy our script and make sure it's executable.
COPY scripts/fetch_weather.py /usr/local/bin/fetch-weather
RUN chmod +x /usr/local/bin/fetch-weather

# Tell docker which command to run when starting the container.
ENTRYPOINT [ "/usr/local/bin/fetch-weather" ]

# Tell docker which default arguments to include with the command.
CMD [ "--help" ]
