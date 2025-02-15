FROM python:3.9-slim

# Install system dependencies (gcc) needed to compile certain Python packages
RUN apt-get update && apt-get install -y gcc

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the remaining application files into the container
COPY . .

# By default, the container will start by running the producer script
# (this can be overridden in docker-compose if needed)
CMD ["python", "producer.py"]
