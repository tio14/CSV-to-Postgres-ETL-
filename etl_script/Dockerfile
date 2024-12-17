FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install numpy pandas psycopg2

# Install PostgreSQL command-line tools
RUN apt-get update && apt-get install -y postgresql-client-15

# Copy the ELT script 
COPY etl_script.py .
COPY movies.csv .

# Set the default command to run the ELT script
CMD ["python", "etl_script.py"]