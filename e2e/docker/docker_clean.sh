#!/bin/bash

# A simple script to clean docker

# Kill running instances
docker stop $(docker ps -a -q)

# Delete all containers
docker rm $(docker ps -a -q)

# Delete all images
docker rmi -f $(docker images -q)
