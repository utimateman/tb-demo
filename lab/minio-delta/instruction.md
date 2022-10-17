# try connect mc
mc alias set demo-minio http://localhost:9000 minio minio123 
mc mb demo-minio/example-data-pipeline
mc rm --recursive --force demo-minio/example-data-pipeline