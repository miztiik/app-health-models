# Use the official Python base image
FROM python:3.9-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the content
COPY . /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the environment variables
ENV FLASK_APP=miztiik-app/app.py

# Expose the port
EXPOSE 80

# Run the command to start uWSGI
CMD ["gunicorn", "--bind", "0.0.0.0:80", "miztiik-app.app:app"]