FROM python:3.14-slim

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

RUN pip install uv

# Copy dependency files and install dependencies
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked

# Copy ingestion scripts into the Docker image
COPY ./ingestion/run.sh ./ 
COPY ./ingestion/ingest_green_tripdata.py ./
COPY ./ingestion/ingest_taxi_zone_lookup.py ./

# Make the script executable by changing its permissions
RUN chmod +x run.sh

# Run the ingestion script when the container starts
CMD ["./run.sh"]
