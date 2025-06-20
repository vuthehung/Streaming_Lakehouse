# Mini Project - Data Engineering - VDT 2025
## Project introduction
<ul>
  <li>Project: Demo Streaming Lakehouse</li>
  <li>Project objective:
    <ul>
      <li>Understand the data lakehouse architecture</li>
      <li>Apply technologies used in the data lakehouse architecture</li>
      <li>Build a streaming project using the data lakehouse architecture</li>
    </ul>
  </li>
</ul>

## Architecture
<img src="./pictures/architecture.png">

## Deploy system
### 1. Build Images
```sh
docker build ./flask -t flask
```
```sh
docker build ./airflow -t airflow
```
```sh
docker build ./superset -t superset
```
### 2. Run system
```sh
docker-compose up -d
```
### 3. Run DAG on Airflow webserver
#### Create Connection: Admin -> Connections -> Create
Create Spark Connection
<img src="./pictures/spark_conn.png">

Create Trino Connection
<img src="./pictures/trino_conn.png">

#### Run DAG: stream_dag -> batch_dag

### 4. Run Superset
#### Create environment
```sh
./superset/bootstrap-superset.sh
```
#### Create Trino Connction
Create connection: Settings -> Database Connections
<img src="./pictures/superset_trino_conn.png">

Output
<img src="./pictures/dashboard.png">






