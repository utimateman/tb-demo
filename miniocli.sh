#!/bin/bash

# setup minio cli
curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/
mc --help



# [comment]: <> (mc alias set demo-minio http://localhost:9000 minio minio123)
# [comment]: <> (mc mb demo-minio/example-data-pipeline)
# [comment]: <> (mc rm --recursive --force demo-minio/example-data-pipeline)


