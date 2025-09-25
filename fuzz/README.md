
# 基于查分测试的 VDBMS 测试框架
对于四种常见的VDBMS: milvusdb、chroma、qdrant、weaviate
我希望对他们进行模糊测试，为了减少第三方工具的干扰，我希望直接使用API进行测试，而不是使用第三方编程语言提供的sdk
这四个数据库疫情成功启动了docker，详细信息如下
```shell
f0488514985c   milvusdb/milvus:latest             "/tini -- milvus run…"   11 seconds ago   Up 10 seconds (healthy)   0.0.0.0:2379->2379/tcp, [::]:2379->2379/tcp, 0.0.0.0:9091->9091/tcp, [::]:9091->9091/tcp, 0.0.0.0:19530->19530/tcp, [::]:19530->19530/tcp   milvus-standalone
47af68927415   chromadb/chroma:latest             "dumb-init -- chroma…"   54 minutes ago   Up About a minute         0.0.0.0:8000->8000/tcp, [::]:8000->8000/tcp                                                                                                 chroma
bd98d5a3c37a   semitechnologies/weaviate:latest   "/bin/weaviate --hos…"   22 hours ago     Up About a minute         0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp                                                                                                 weaviate
7164bd3892f1   qdrant/qdrant:latest               "./entrypoint.sh"        22 hours ago     Up 55 seconds             0.0.0.0:6333->6333/tcp, [::]:6333->6333/tcp, 6334/tcp 
```

接下来我希望你实现一个差分测试框架对他们进行测试，开发语言选择Python