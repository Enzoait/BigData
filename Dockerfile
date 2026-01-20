FROM --platform=linux/amd64 python:3.9-slim

# Set working directory
WORKDIR /app
ENV PYTHONPATH=/app
# Copy application files
COPY . /app
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Expose port
EXPOSE 5000
# Command to run the application
CMD ["python", "api/app.py"]
