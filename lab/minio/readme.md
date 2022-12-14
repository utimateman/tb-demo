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

[comment]: <> (ssh -i tb_id_rsa root@167.71.20.174)
[comment]: <> (ssh -i tb_id_rsa root@137.184.79.204)
[comment]: <> (ssh -i tb_id_rsa root@167.71.25.125)
[comment]: <> (ssh -i tb_id_rsa root@167.71.25.36)
[comment]: <> (ssh -i tb_id_rsa root@167.71.30.49)
[comment]: <> (ssh -i tb_id_rsa root@167.71.25.143)


[comment]: <> (
-----BEGIN OPENSSH PRIVATE KEY-----
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
-----END OPENSSH PRIVATE KEY-----
)
