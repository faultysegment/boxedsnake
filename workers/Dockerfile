FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY config.py executor.py main.py ./

# Run the worker daemon
ENTRYPOINT ["python", "-u", "main.py"]
