# BigMartSales_DataEngineering_project
Processing BigMartSales data set using Apache spark

## How to run this project using Docker?

### Build the docker image:
`docker build -t <app-name>`

### Run docker container:

Mount local output dir 'Metrics' in the container to fetch the metrics created by our spark job. 

This is creating a shared directory between local and docekr container. Any changes in folder will be reflected in both places.

`docker run -v $(pwd)/Metrics:/app/Metrics <app-name>`
