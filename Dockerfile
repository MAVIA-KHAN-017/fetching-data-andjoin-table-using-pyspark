# # Use an official Python runtime as the base image
# FROM python:3.8

# # Set the working directory in the container
# WORKDIR /app

# # Copy the requirements file
# COPY requirements.txt .

# # Install the required Python packages
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy the code into the container
# COPY . .

# # Set environment variables if needed
# ENV REDIS_HOST=localhost
# ENV REDIS_PORT=6379

# # Run the Python script
# CMD ["python", "rfc.py"]

# FROM python:3.8

# # Install dependencies
# RUN pip install redis requests pyspark

# # Copy the script to the Docker image
# COPY rfc.py /

# EXPOSE 8000

# # Run the script
# CMD ["python", "/rfc.py"]


# Use an official Python runtime as the base image
# Base image with Java and Spark
# Base image with Java and Spark
FROM bitnami/spark:3.1.2

# Install Python and required libraries
USER root
RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory in the container
WORKDIR /app

# Copy the PySpark script to the container
COPY rfc.py /app/rfc.py

# Install required Python packages
COPY requirements.txt /app/requirements.txt
RUN pip3 install --user -r requirements.txt

# Set the entry point of the container to run the PySpark script
CMD spark-submit --master local[2] --py-files /app/rfc.py /app/rfc.py
