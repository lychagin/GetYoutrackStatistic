#!/bin/sh

sudo docker ps -a | awk '/yt-metrics/{print $1}' | xargs sudo docker rm && sudo docker image rm yt-metrics
