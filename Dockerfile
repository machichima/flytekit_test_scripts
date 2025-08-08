# Use a minimal base image
FROM python:3.10-slim

# Set environment variables for clean installation
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install OS dependencies (for pandas, pyarrow, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    requests \ 
    flytekit \
    pydantic_settings \
    pandas \
    pyarrow \
    boto3 \
    "polars[s3]" \
    s3fs

# Optional: Add your code (or leave empty if this is a base image)
# COPY . /app
