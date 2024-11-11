FROM quay.io/astronomer/astro-runtime:12.3.0

# Define AIRFLOW_HOME
ENV AIRFLOW_HOME="/usr/local/airflow"

# Create and activate the virtual environment, install dbt-snowflake
RUN python -m venv $AIRFLOW_HOME/dbt_venv && \
    $AIRFLOW_HOME/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake

# Install dependencies from requirements.txt


# Set PATH to prioritize the virtual environment
ENV PATH="$AIRFLOW_HOME/dbt_venv/bin:$PATH"

