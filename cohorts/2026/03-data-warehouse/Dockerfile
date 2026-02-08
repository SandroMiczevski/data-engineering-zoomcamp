FROM python:latest

WORKDIR /code

# Install uv tool for dependency management and set virtual environment path
RUN pip install uv
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files and install dependencies
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked

