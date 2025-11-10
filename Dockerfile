# Dockerfile
FROM postgres:15

# Label for clarity
LABEL maintainer="Nomba Data Engineering Assessment"
LABEL description="Local analytics warehouse for CDC + dbt testing"

# Environment variables
ENV POSTGRES_USER=analytics
ENV POSTGRES_PASSWORD=analytics
ENV POSTGRES_DB=nomba_warehouse

# Copy init scripts (schema + tables)
COPY init_scripts/ /docker-entrypoint-initdb.d/

# Expose default port
EXPOSE 5432

