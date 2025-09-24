#!/usr/bin/env python3
"""
Vector Database Connection Example
连接四个向量数据库的简单示例: Milvus, Pinecone, Qdrant, Weaviate
"""

import numpy as np
import time
from typing import List, Dict, Any

try:
    from pymilvus import MilvusClient, Collection, FieldSchema, CollectionSchema, DataType, Index
    from pinecone import Pinecone, ServerlessSpec
    from qdrant_client import QdrantClient, models
    import weaviate
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Please install: pip install -r requirements.txt")
    exit(1)


class VectorDatabaseConnector:
    def __init__(self):
        self.clients = {
            'milvus': None,
            'pinecone': None,
            'qdrant': None,
            'weaviate': None
        }

    def connect_milvus(self, host='localhost', port=19530):
        """连接Milvus数据库"""
        try:
            uri = f"http://{host}:{port}"
            self.clients['milvus'] = MilvusClient(uri=uri)
            print(f"✓ Milvus connected successfully at {uri}")
            return True
        except Exception as e:
            print(f"✗ Milvus connection failed: {e}")
            return False

    def connect_pinecone(self, api_key='your-api-key', environment='us-west1-gcp'):
        """连接Pinecone数据库"""
        try:
            self.clients['pinecone'] = Pinecone(api_key=api_key)
            print(f"✓ Pinecone connected successfully")
            return True
        except Exception as e:
            print(f"✗ Pinecone connection failed: {e}")
            return False

    def connect_qdrant(self, host='localhost', port=6333):
        """连接Qdrant数据库"""
        try:
            self.clients['qdrant'] = QdrantClient(host=host, port=port)
            print(f"✓ Qdrant connected successfully at {host}:{port}")
            return True
        except Exception as e:
            print(f"✗ Qdrant connection failed: {e}")
            return False

    def connect_weaviate(self, host='localhost', port=8080):
        """连接Weaviate数据库"""
        try:
            self.clients['weaviate'] = weaviate.connect_to_local(
                host=host,
                port=port,
                skip_init_checks=True
            )
            print(f"✓ Weaviate connected successfully at {host}:{port}")
            return True
        except Exception as e:
            print(f"✗ Weaviate connection failed: {e}")
            return False

    def connect_all(self):
        """连接所有数据库"""
        print("=== 连接所有向量数据库 ===")

        connections = []
        connections.append(self.connect_milvus())
        connections.append(self.connect_pinecone())
        connections.append(self.connect_qdrant())
        connections.append(self.connect_weaviate())

        success_count = sum(connections)
        print(f"\n连接结果: {success_count}/4 成功")

        return success_count > 0

    def generate_sample_vectors(self, count=5, dim=128):
        """生成示例向量数据"""
        return np.random.rand(count, dim).astype(np.float32)

    def milvus_operations(self):
        """Milvus基本操作"""
        if not self.clients['milvus']:
            return

        print("\n=== Milvus 基本操作 ===")
        client = self.clients['milvus']

        try:
            # 创建集合
            collection_name = "test_collection"
            if client.has_collection(collection_name):
                client.drop_collection(collection_name)

            schema = client.create_schema(
                auto_id=True,
                enable_dynamic_field=True,
            )
            schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
            schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=128)

            client.create_collection(
                collection_name=collection_name,
                schema=schema,
            )
            print("✓ Milvus 集合创建成功")

            # 插入数据
            vectors = self.generate_sample_vectors(5, 128)
            data = [{"vector": vector.tolist()} for vector in vectors]

            client.insert(collection_name=collection_name, data=data)
            print("✓ Milvus 数据插入成功")

            # 搜索数据
            query_vector = self.generate_sample_vectors(1, 128)[0].tolist()

            # 等待索引创建完成 (Flat索引自动创建)
            time.sleep(2)

            # 加载集合以确保可搜索
            client.load_collection(collection_name)

            results = client.search(
                collection_name=collection_name,
                data=[query_vector],
                limit=3,
                output_fields=["id"]
            )
            print(f"✓ Milvus 搜索完成，找到 {len(results[0])} 个结果")

        except Exception as e:
            print(f"✗ Milvus 操作失败: {e}")

    def pinecone_operations(self):
        """Pinecone基本操作"""
        if not self.clients['pinecone']:
            return

        print("\n=== Pinecone 基本操作 ===")
        client = self.clients['pinecone']

        try:
            # 创建索引
            index_name = "test-index"

            # 检查并删除现有索引
            if index_name in client.list_indexes().names():
                client.delete_index(index_name)

            # 创建新索引
            client.create_index(
                name=index_name,
                dimension=128,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            print("✓ Pinecone 索引创建成功")

            # 等待索引就绪
            while not client.describe_index(index_name).status.ready:
                time.sleep(1)

            # 连接到索引
            index = client.Index(index_name)

            # 插入数据
            vectors = self.generate_sample_vectors(5, 128)
            vectors_to_upsert = [
                (str(i), vector.tolist(), {"text": f"sample_text_{i}"})
                for i, vector in enumerate(vectors)
            ]

            index.upsert(vectors_to_upsert)
            print("✓ Pinecone 数据插入成功")

            # 搜索数据
            query_vector = self.generate_sample_vectors(1, 128)[0].tolist()
            results = index.query(
                vector=query_vector,
                top_k=3,
                include_values=True
            )
            print(f"✓ Pinecone 搜索完成，找到 {len(results.matches)} 个结果")

        except Exception as e:
            print(f"✗ Pinecone 操作失败: {e}")

    def qdrant_operations(self):
        """Qdrant基本操作"""
        if not self.clients['qdrant']:
            return

        print("\n=== Qdrant 基本操作 ===")
        client = self.clients['qdrant']

        try:
            # 创建集合
            collection_name = "test_collection"

            # 检查并删除现有集合
            if client.collection_exists(collection_name):
                client.delete_collection(collection_name)

            # 创建新集合
            client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(size=128, distance=models.Distance.COSINE)
            )
            print("✓ Qdrant 集合创建成功")

            # 插入数据
            vectors = self.generate_sample_vectors(5, 128)
            points = [
                models.PointStruct(
                    id=i,
                    vector=vector.tolist(),
                    payload={"text": f"sample_text_{i}"}
                )
                for i, vector in enumerate(vectors)
            ]

            client.upsert(
                collection_name=collection_name,
                points=points
            )
            print("✓ Qdrant 数据插入成功")

            # 搜索数据
            query_vector = self.generate_sample_vectors(1, 128)[0].tolist()
            results = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=3
            )
            print(f"✓ Qdrant 搜索完成，找到 {len(results.points)} 个结果")

        except Exception as e:
            print(f"✗ Qdrant 操作失败: {e}")

    def weaviate_operations(self):
        """Weaviate基本操作"""
        if not self.clients['weaviate']:
            return

        print("\n=== Weaviate 基本操作 ===")
        client = self.clients['weaviate']

        try:
            # 创建类
            class_name = "TestItem"

            # 删除现有类
            if client.collections.exists(class_name):
                client.collections.delete(class_name)

            # 创建新类
            class_obj = {
                "class": class_name,
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "text",
                        "dataType": ["text"]
                    }
                ]
            }

            client.collections.create_from_dict(class_obj)
            print("✓ Weaviate 类创建成功")

            # 获取集合引用
            collection = client.collections.get(class_name)

            # 插入数据
            vectors = self.generate_sample_vectors(5, 128)

            for i, vector in enumerate(vectors):
                data_object = {
                    "text": f"sample_text_{i}"
                }

                collection.data.insert(
                    properties=data_object,
                    vector=vector.tolist()
                )

            print("✓ Weaviate 数据插入成功")

            # 搜索数据 (跳过如果gRPC不可用)
            try:
                query_vector = self.generate_sample_vectors(1, 128)[0].tolist()

                results = collection.query.near_vector(
                    near_vector=query_vector,
                    limit=3,
                    return_properties=["text"]
                )

                result_count = len(results.objects)
                print(f"✓ Weaviate 搜索完成，找到 {result_count} 个结果")
            except Exception as search_e:
                print(f"✓ Weaviate 数据插入成功，搜索跳过 (gRPC不可用): {search_e}")

        except Exception as e:
            print(f"✗ Weaviate 操作失败: {e}")

    def run_all_operations(self):
        """运行所有数据库的基本操作"""
        print("\n=== 运行所有数据库基本操作 ===")

        operations = [
            self.milvus_operations,
            self.pinecone_operations,
            self.qdrant_operations,
            self.weaviate_operations
        ]

        for operation in operations:
            try:
                operation()
            except Exception as e:
                print(f"操作失败: {e}")

    def cleanup(self):
        """清理连接"""
        for db_name, client in self.clients.items():
            if client:
                try:
                    if db_name == 'milvus':
                        # Milvus客户端会自动清理
                        pass
                    elif db_name == 'pinecone':
                        # Pinecone客户端会自动清理
                        pass
                    elif db_name == 'qdrant':
                        # Qdrant客户端会自动清理
                        pass
                    elif db_name == 'weaviate':
                        # Weaviate客户端需要显式关闭
                        if hasattr(client, 'close'):
                            client.close()
                    print(f"✓ {db_name} 连接已清理")
                except Exception as e:
                    print(f"✗ {db_name} 清理失败: {e}")


def main():
    """主函数"""
    connector = VectorDatabaseConnector()

    try:
        # 连接所有数据库
        if not connector.connect_all():
            print("没有成功连接到任何数据库")
            return

        # 运行基本操作
        connector.run_all_operations()

        print("\n=== 所有操作完成 ===")

    except Exception as e:
        print(f"程序执行失败: {e}")

    finally:
        # 清理资源
        connector.cleanup()


if __name__ == "__main__":
    main()