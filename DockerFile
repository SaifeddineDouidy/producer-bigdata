# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir azure-identity azure-eventhub requests python-dotenv

# Run the producer script when the container launches
CMD ["python", "producer.py"]