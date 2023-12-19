FROM python:alpine

WORKDIR /app

COPY silapiimporter.py .
COPY progress_bible.py .
COPY joshua_project.py .
COPY main.py .
COPY requirements.txt .
ADD https://truststore.pki.rds.amazonaws.com/us-west-2/us-west-2-bundle.pem ./aws-ssl-certs/

# Install requirements
# Disable caching, to keep Docker image lean
RUN pip install --no-cache-dir -r requirements.txt

# Run as non-root user
ARG user_id=3046
RUN addgroup -g ${user_id} -S data_tracker && adduser -u ${user_id} -S -G data_tracker data_tracker
# data_tracker user needs access to the AWS CA certificate
RUN chown -R data_tracker:data_tracker ./aws-ssl-certs

USER data_tracker

CMD [ "python", "/app/main.py" ]
