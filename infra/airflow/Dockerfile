FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl tar git && \
    rm -rf /var/lib/apt/lists/*

USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install airflow providers system-wide
COPY requirements.txt .

# Create venv for dbt and install
RUN python -m venv /opt/dbt_venv && \
    chown -R airflow:0 /opt/dbt_venv
COPY requirements-dbt.txt .
RUN VIRTUAL_ENV=/opt/dbt_venv uv pip install --no-cache -r requirements-dbt.txt

# Switch back to airflow user
USER airflow

# Ensure uv can install to airflow's local site-packages without poisoning dbt venv
RUN uv pip install --target /home/airflow/.local/lib/python3.11/site-packages --no-cache -r requirements.txt
