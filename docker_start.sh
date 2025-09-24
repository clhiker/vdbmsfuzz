#!/usr/bin/env bash
docker start 66f6eaa2c6f7
bash milvus/standalone_embed.sh start
docker start 2218d217a853
docker start df7bd3be82c3