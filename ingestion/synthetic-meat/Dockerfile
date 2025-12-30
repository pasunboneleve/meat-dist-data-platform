# ---- Builder ----
# Use the official Python image as a parent image
FROM python:3.11-slim as builder

# Install uv, the package installer
RUN pip install uv

# Set the working directory
WORKDIR /app

# Create a virtual environment and install dependencies from the lock file
COPY pyproject.toml uv.lock ./
RUN uv venv && \
    uv pip sync --no-dev --python .venv/bin/python

# ---- Final Image ----
# Use a slim Python image for the final container
FROM python:3.11-slim

WORKDIR /app

# Copy the virtual environment and the application source code from the builder stage
COPY --from=builder /app/.venv ./.venv
COPY src ./src

# Add the virtual environment to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Set the entry point for the container using functions-framework.
# Cloud Run provides the PORT environment variable, which functions-framework uses by default.
CMD ["functions-framework", "--target=generate_and_upload", "--source=src/synthesise.py"]
