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
    ca-certificates

# Install requirements
# Disable caching, to keep Docker image lean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the rest of the scripts
ADD https://truststore.pki.rds.amazonaws.com/us-west-2/us-west-2-bundle.pem ./aws-ssl-certs/
COPY functions.py .
COPY silapiimporter.py .
COPY progress_bible.py .
COPY joshua_project.py .
COPY main.py .
COPY FRED_scraper.py .
COPY github_scraper.py .
COPY google_sheets_scraper.py .
COPY white_pages_scraper.py .
COPY impact_metrics_scraper.py .
COPY imports_positive_pr.py .


# Run as non-root user
RUN chown -R nonroot:nonroot /app/

USER nonroot

CMD [ "python", "/app/main.py" ]
