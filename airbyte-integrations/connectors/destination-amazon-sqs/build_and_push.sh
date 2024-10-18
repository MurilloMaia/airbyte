#!/bin/bash

# Verifica se o argumento foi fornecido
if [ $# -ne 1 ]; then
    echo "Uso: $0 <tag>"
    exit 1
fi

tag=$1

# Executa os comandos
airbyte-ci connectors --name=destination-amazon-sqs build --architecture=linux/amd64 --tag=$tag

docker tag airbyte/destination-amazon-sqs:$tag murillosmaia/destination-amazon-sqs:$tag
docker push murillosmaia/destination-amazon-sqs:$tag

# Mensagem de lembrete
echo "Não se esqueça de alterar também no arquivo k8s/values/env/values.yaml"



