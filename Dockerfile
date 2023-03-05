# Use the official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requests.txt
RUN pip install --trusted-host pypi.python.org -r requests.txt

# Expose port 80 for FastAPI
EXPOSE 80

# Run the application using uvicorn and FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
