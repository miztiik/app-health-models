# Use the official Python base image
FROM python:3.9-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the content
COPY miztiik-producer-app /app

# Set the environment variables
ENV FLASK_APP=app.py

# Expose the port
EXPOSE 80

# Run the command to start uWSGI
# CMD ["gunicorn", "--bind", "0.0.0.0:80", "app:app"]
CMD ["gunicorn", "--bind", "0.0.0.0:80", "--access-logfile", "-", "--error-logfile", "-", "app:app"]