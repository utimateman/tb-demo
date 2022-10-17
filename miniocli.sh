#!/bin/bash

# setup minio cli
curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/
 mc --help

# try connect mc
mc alias set demo-minio http://localhost:9000 minio minio123 

