# PostgreSQL and pgAdmin Setup with Docker

This guide provides step-by-step instructions to set up a PostgreSQL database, pgAdmin, and data ingestion using Docker containers.

## 1. Create a Docker Network

We create a dedicated Docker network to ensure seamless communication between the PostgreSQL database and pgAdmin.

```sh
docker network create -d bridge pg-network2
```

## 2. Run PostgreSQL Container

This command starts a PostgreSQL container with the necessary environment variables and volume mapping.

```sh
docker run -it \
      -e POSTGRES_USER="root" \
      -e POSTGRES_PASSWORD="root" \
      -e POSTGRES_DB="ny_taxi" \
      -v "/home/vivek/de_zoomcamp_2025/ny_taxi_prostgres_data:/var/lib/postgresql/data:rw" \
      -p 5432:5432 \
      --network=pg-network2 \ 
      --name pg-database \
      postgres:13
```

## 3. Run pgAdmin Container

We set up pgAdmin to manage the PostgreSQL database via a web-based interface.

```sh
docker run \
      -e PGADMIN_DEFAULT_EMAIL='admin@admin.com' \
      -e PGADMIN_DEFAULT_PASSWORD='root' \
      -v "/home/vivek/de_zoomcamp_2025/pg_admin:/var/lib/pgadmin:rw" \
      -p "8080:80" \
      --network=pg-network2 \
      --name pg-admin \
      dpage/pgadmin4
```

Access pgAdmin by opening `http://localhost:8080` in your browser and logging in with the provided credentials.

## 4. Run `ingest_data.py` Script Locally

Download the dataset and ingest it into the PostgreSQL database.

```sh
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--database=ny_taxi \
--table=yellow_taxi \
--url=${URL}
```

## 5. Build a Docker Image for Data Ingestion

To run `ingest_data.py` inside a container, first build the image:

```sh
docker build -t taxi_ingest:v001 .
```

## 6. Run the Data Ingestion Container

Execute the ingestion script inside a Docker container within the same network.

```sh
docker run --name taxi_ingest_test \
--network=pg-network2 \
taxi_ingest:v001 \
--user=root \
--password=root \
--host=pg-database \
--port=5432 \
--database=ny_taxi \
--table=yellow_taxi \
--url=${URL}
```

## Notes:

- Ensure that Docker is installed and running before executing these commands.
- Use `docker ps` to verify that the containers are running.
- You can inspect logs using `docker logs <container_name>` if any issues arise.

This setup ensures a smooth workflow for PostgreSQL management and data ingestion using Docker.
