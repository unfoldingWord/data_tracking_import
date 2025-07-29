FROM python:bullseye

WORKDIR /app

# Install requirements
# Disable caching, to keep Docker image lean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the rest of the scripts
COPY silapiimporter.py .
COPY progress_bible.py .
COPY joshua_project.py .
COPY main.py .
ADD https://truststore.pki.rds.amazonaws.com/us-west-2/us-west-2-bundle.pem ./aws-ssl-certs/

# Run as non-root user
ARG user_id=3046
RUN groupadd -g ${user_id} data_tracker && useradd -u ${user_id} -g data_tracker data_tracker
# data_tracker user needs access to the AWS CA certificate
RUN chown -R data_tracker:data_tracker ./aws-ssl-certs

USER data_tracker

CMD [ "python", "/app/main.py" ]
