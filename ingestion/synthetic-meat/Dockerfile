# ---- Builder ----
# Use the official Python image as a parent image
FROM python:3.11-slim as builder

# Install uv, the package installer
RUN pip install uv

# Set the working directory
WORKDIR /app

# Copy project files to build the package
COPY pyproject.toml uv.lock ./
COPY src ./src

# Install the project and its dependencies as a tool.
# This creates a self-contained virtual environment.
# `uv` will name the tool 'synthetic-meat' based on pyproject.toml.
RUN uv tool install .

# ---- Final Image ----
# Use a slim Python image for the final container
FROM python:3.11-slim

WORKDIR /app

# Copy the installed tool environment from the builder stage.
COPY --from=builder /root/.uv/tools/synthetic-meat /app/.venv

# Add the virtual environment to the PATH. This makes `functions-framework` available.
ENV PATH="/app/.venv/bin:$PATH"

# Set the entry point for the container using functions-framework.
# The target function is specified by its full import path.
# No --source flag is needed because the package is installed in the environment.
CMD ["functions-framework", "--target=src.synthesise.generate_and_upload"]
