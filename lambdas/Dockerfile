FROM public.ecr.aws/lambda/python:3.13

WORKDIR /app

# Install zip using dnf since the container is Amazon Linux 2023 based
RUN dnf install -y zip

COPY requirements.txt .
COPY main.py .

# Install dependencies directly into /app (same as deployment package root)
RUN pip install -r requirements.txt -t .

# Exclude pycache files from deployment package to reduce bloat
RUN zip -r deployment_package.zip . -x "*__pycache__*" "*.pyc"
