# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and source code
COPY pyproject.toml .
COPY my_env ./my_env

# Install the package in editable mode so Uvicorn can find it
# Added --default-timeout=1000 to prevent network timeouts during large downloads
RUN pip install --default-timeout=1000 --no-cache-dir -e .
RUN pip install --default-timeout=1000 --no-cache-dir uvicorn fastapi

# Define environment variables used by OpenEnv/FastAPI
ENV PORT=7860
ENV HOST=0.0.0.0
ENV WORKERS=2
ENV MAX_CONCURRENT_ENVS=100

# Expose the port the app runs on
EXPOSE 7860

# Command to run the application using Uvicorn
CMD ["uvicorn", "my_env.server.app:app", "--host", "0.0.0.0", "--port", "7860"]