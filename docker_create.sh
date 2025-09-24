#!/usr/bin/env bash
docker run -d   --name chroma   -p 8000:8000   -v chroma_data:/chroma/db   chromadb/chroma:latest
bash milvus/standalone_embed.sh start
docker run -d   --name qdrant   -p 6333:6333   -v qdrant_storage:/qdrant/storage   qdrant/qdrant:latest
docker run -d   --name weaviate   -p 8080:8080   -e QUERY_DEFAULTS_LIMIT=20   -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true   -v weaviate_data:/var/lib/weaviate   semitechnologies/weaviate:latest