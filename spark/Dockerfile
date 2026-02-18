FROM apache/spark:3.5.1

USER root

# Install faker
RUN pip install faker

# Download PostgreSQL JDBC driver directly
RUN curl -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER spark