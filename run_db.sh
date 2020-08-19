#!/bin/bash

MYSQL_DATABASE=${MYSQL_DATABASE:=suicord}
MYSQL_USER=${MYSQL_USER:=suicord}
MYSQL_PASS=${MYSQL_PASS:=suicord}

docker run --rm -d \
    -e MYSQL_RANDOM_ROOT_PASSWORD=yes \
    -e MYSQL_USER=${MYSQL_USER} \
    -e MYSQL_PASSWORD=${MYSQL_PASS} \
    -e MYSQL_DATABASE=${MYSQL_DATABASE} \
    -p 3306:3306 \
    --name=suicord \
    mariadb:10
