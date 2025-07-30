# Using Chainguard WolfiOS as base
FROM cgr.dev/chainguard/wolfi-base

# Set Python version
ARG version=3.12

WORKDIR /app

# Install required libraries
RUN apk update && apk add --no-cache \
    python-${version} \
    py${version}-pip \
    py${version}-setuptools \
    ca-certificates \
    curl \
    openssl

# Fix certificate issue with AAA Certificate Services (Comodo/Sectigo)
# Download the AAA Certificate Services root CA
RUN curl -o aaa_cert_services.der "http://crt.comodoca.com/AAACertificateServices.crt" && \
    # Convert from DER to PEM format
    openssl x509 -inform DER -in aaa_cert_services.der -out aaa_cert_services.crt && \
    # Remove old DER certificate
    rm aaa_cert_services.der && \
    # Copy crt file to the certs directory
    mv aaa_cert_services.crt /etc/ssl/certs/ && \
    # Update the certificate hash links
    c_rehash /etc/ssl/certs/

# Install requirements
# Disable caching, to keep Docker image lean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the rest of the scripts
ADD https://truststore.pki.rds.amazonaws.com/us-west-2/us-west-2-bundle.pem ./aws-ssl-certs/
COPY silapiimporter.py .
COPY progress_bible.py .
COPY joshua_project.py .
COPY main.py .

# Run as non-root user
RUN chown -R nonroot:nonroot /app/

USER nonroot

CMD [ "python", "/app/main.py" ]
