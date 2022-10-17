### Interacting with Minio User Interface
###### 1. Create Minio bucket name "lab"
###### 2. Upload the given CSV file (lab/data/computer_games.csv) into the created bucket
###### 3. Try delete the bucket 

### Interacting with Minio CLI
###### 1. Install the Minio CLI according to the "miniocli.sh"  
###### 2. Try establish a connection between your Minio and CLI using the following command:
    mc alias set demo-minio http://localhost:9000 minio minio123 
###### 3. Try verify if the bucket has been created using the following command:
    mc ls [ your alias]
###### 4. Try verify if the prior uploaded CSV file is existed
###### 5. Please visit the following [url](https://min.io/docs/minio/linux/reference/minio-mc.html) for more information on Minio CLI


[comment]: <> (mc alias set demo-minio http://localhost:9000 minio minio123)
[comment]: <> (mc mb demo-minio/example-data-pipeline)
[comment]: <> (mc rm --recursive --force demo-minio/example-data-pipeline)