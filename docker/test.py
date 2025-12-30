# docker run -it --name pg-database --network pg-network `
#      -e POSTGRES_USER=root `
#      -e POSTGRES_PASSWORD=root `
#      -e POSTGRES_DB=ny_taxi `
#      -v ${PWD}/ny_taxi_postgres_data:/var/lib/postgresql/data `
#      -p 5432:5432 `
#      postgres:13

# docker run -it --name pgadmin --network pg-network `
# -e PGADMIN_DEFAULT_EMAIL=admin@admin.com `
# -e PGADMIN_DEFAULT_PASSWORD=root `
# -p 8080:80 `
# dpage/pgadmin4

# docker run -it `
#     --name ingest_data_container `
#     --network pg-network `
#     taxi_ingest:v001 `
#     --user=root `
#     --password=root `
#     --host=localhost `
#     --port=5432 `
#     --db=ny_taxi `
#     --table_name=yellow_taxi_trips `
#     --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
     
# # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

# import pandas as pd

# df = pd.read_parquet("yellow_tripdata_2021-01.parquet")

# print(df.columns)
# print(df.head(5))


list = [1, 1, 1, 1, 1]
m = 1

