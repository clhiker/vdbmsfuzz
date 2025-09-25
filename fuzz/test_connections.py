# """
# Database connection testing script for VDBMS fuzzing framework
# Tests connections to Milvus, Chroma, Qdrant, and Weaviate databases
# """

# import asyncio
# import aiohttp
# import json
# import logging
# from typing import Dict, List

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class ConnectionTester:
#     """Enhanced database connection tester with proper endpoint handling"""

#     def __init__(self):
#         self.results = {}

#     async def test_database_connection(self, name: str, url: str, endpoints: List[Dict]) -> Dict[str, bool]:
#         """Test database connection with multiple endpoints"""
#         logger.info(f"Testing connection to {name} at {url}")
#         results = {}

#         async with aiohttp.ClientSession() as session:
#             for endpoint_info in endpoints:
#                 endpoint = endpoint_info["path"]
#                 method = endpoint_info.get("method", "GET")
#                 headers = endpoint_info.get("headers", {})
#                 expected_status = endpoint_info.get("expected_status", 200)

#                 try:
#                     full_url = f"{url}{endpoint}"
#                     logger.info(f"  Testing: {method} {full_url}")

#                     if method == "GET":
#                         async with session.get(full_url, timeout=10, headers=headers) as response:
#                             if isinstance(expected_status, list):
#                                 success = response.status in expected_status
#                             else:
#                                 success = response.status == expected_status
#                             results[endpoint] = {
#                                 "success": success,
#                                 "status": response.status,
#                                 "method": method
#                             }
#                             if success:
#                                 try:
#                                     data = await response.json()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {str(data)[:100]}...")
#                                 except json.JSONDecodeError:
#                                     text = await response.text()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {text[:100]}...")
#                             else:
#                                 if isinstance(expected_status, list):
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected one of {expected_status})")
#                                 else:
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected {expected_status})")

#                     elif method == "POST":
#                         payload = endpoint_info.get("payload", {})
#                         async with session.post(full_url, json=payload, timeout=10, headers=headers) as response:
#                             if isinstance(expected_status, list):
#                                 success = response.status in expected_status
#                             else:
#                                 success = response.status == expected_status
#                             results[endpoint] = {
#                                 "success": success,
#                                 "status": response.status,
#                                 "method": method
#                             }
#                             if success:
#                                 try:
#                                     data = await response.json()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {str(data)[:100]}...")
#                                 except json.JSONDecodeError:
#                                     text = await response.text()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {text[:100]}...")
#                             else:
#                                 if isinstance(expected_status, list):
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected one of {expected_status})")
#                                 else:
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected {expected_status})")

#                     elif method == "PUT":
#                         payload = endpoint_info.get("payload", {})
#                         async with session.put(full_url, json=payload, timeout=10, headers=headers) as response:
#                             if isinstance(expected_status, list):
#                                 success = response.status in expected_status
#                             else:
#                                 success = response.status == expected_status
#                             results[endpoint] = {
#                                 "success": success,
#                                 "status": response.status,
#                                 "method": method
#                             }
#                             if success:
#                                 try:
#                                     data = await response.json()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {str(data)[:100]}...")
#                                 except json.JSONDecodeError:
#                                     text = await response.text()
#                                     logger.info(f"    ✓ {name}: {endpoint} - Status {response.status}")
#                                     logger.info(f"      Response: {text[:100]}...")
#                             else:
#                                 if isinstance(expected_status, list):
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected one of {expected_status})")
#                                 else:
#                                     logger.warning(f"    ⚠️  {name}: {endpoint} - Status {response.status} (expected {expected_status})")

#                 except asyncio.TimeoutError:
#                     logger.warning(f"    ⚠️  {name}: {endpoint} - Timeout")
#                     results[endpoint] = {"success": False, "status": "timeout", "method": method}
#                 except aiohttp.ClientError as e:
#                     logger.warning(f"    ⚠️  {name}: {endpoint} - Client Error: {e}")
#                     results[endpoint] = {"success": False, "status": f"client_error: {e}", "method": method}
#                 except Exception as e:
#                     logger.warning(f"    ⚠️  {name}: {endpoint} - Error: {e}")
#                     results[endpoint] = {"success": False, "status": f"error: {e}", "method": method}

#         # Calculate overall success
#         any_successful = any(result["success"] for result in results.values())
#         logger.info(f"  {name}: {'✓ Connected' if any_successful else '✗ Failed'} ({sum(1 for r in results.values() if r['success'])}/{len(results)} endpoints working)")

#         return results

#     async def main(self):
#         """Main testing function"""
#         # Enhanced database configuration with proper endpoint details
#         databases = {
#             "milvus": {
#                 "url": "http://localhost:19530",
#                 "endpoints": [
#                     {"path": "/", "method": "GET", "expected_status": 404},
#                     {"path": "/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v1/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v2/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v1/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v2/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v1/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v2/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v1/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v2/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v2/vectordb/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v2/vectordb/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v2/vectordb/databases", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v2/vectordb/databases", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/api/v1/databases", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/v1/databases", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/healthz", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/info", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/metrics", "method": "GET", "expected_status": [200, 404]}
#                 ]
#             },
#             "milvus_9091": {
#                 "url": "http://localhost:9091",
#                 "endpoints": [
#                     {"path": "/", "method": "GET", "expected_status": 404},
#                     {"path": "/health", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/metrics", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/info", "method": "GET", "expected_status": [200, 404]}
#                 ]
#             },
#             "chroma": {
#                 "url": "http://localhost:8000",
#                 "endpoints": [
#                     {"path": "/api/v1/heartbeat", "method": "GET", "expected_status": 200},
#                     {"path": "/api/v1", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/health", "method": "GET"},
#                     {"path": "/api/v1/health", "method": "GET", "expected_status": [200, 410]},
#                     {"path": "/api/v2/heartbeat", "method": "GET", "expected_status": 200},
#                     {"path": "/api/v2/collections", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/", "method": "GET", "expected_status": [200, 404]}
#                 ]
#             },
#             "qdrant": {
#                 "url": "http://localhost:6333",
#                 "endpoints": [
#                     {"path": "/", "method": "GET", "expected_status": 200},
#                     {"path": "/health", "method": "GET"},
#                     {"path": "/metrics", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/collections", "method": "GET", "expected_status": 200}
#                 ]
#             },
#             "weaviate": {
#                 "url": "http://localhost:8080",
#                 "endpoints": [
#                     {"path": "/.well-known/ready", "method": "GET"},
#                     {"path": "/v1/meta", "method": "GET", "expected_status": 200},
#                     {"path": "/v1/schema", "method": "GET", "expected_status": 200},
#                     {"path": "/", "method": "GET", "expected_status": [200, 404]},
#                     {"path": "/health", "method": "GET"}
#                 ]
#             }
#         }

#         # Test all databases concurrently
#         tasks = []
#         for name, config in databases.items():
#             task = self.test_database_connection(name, config["url"], config["endpoints"])
#             tasks.append(task)

#         self.results = await asyncio.gather(*tasks, return_exceptions=True)

#         # Print summary
#         logger.info("\n" + "="*60)
#         logger.info("CONNECTION TEST SUMMARY")
#         logger.info("="*60)

#         for i, result in enumerate(self.results):
#             if isinstance(result, Exception):
#                 logger.error(f"{'Milvus' if i == 0 else 'Chroma' if i == 1 else 'Qdrant' if i == 2 else 'Weaviate'}: Exception - {result}")
#             else:
#                 db_name = list(databases.keys())[i]
#                 successful_endpoints = sum(1 for r in result.values() if r["success"])
#                 total_endpoints = len(result)
#                 logger.info(f"{db_name.capitalize()}: {successful_endpoints}/{total_endpoints} endpoints working")

#                 if successful_endpoints > 0:
#                     logger.info(f"  ✓ {db_name} is accessible")
#                 else:
#                     logger.warning(f"  ✗ {db_name} is not accessible")

#         logger.info("="*60)

# async def main():
#     """Main entry point"""
#     tester = ConnectionTester()
#     await tester.main()

# if __name__ == "__main__":
#     asyncio.run(main())


from pymilvus import connections, utility, CollectionSchema, FieldSchema, DataType, Collection

# 1. 连接
connections.connect(alias="default", host="127.0.0.1", port="19530")
# 或者如果 Milvus 有 token 或者配置特殊的地址/域名

# 2. 查看服务器版本
ver = utility.get_server_version()
print("Milvus server version:", ver)

# 3. 查看当前是否已有某个 collection
print("Collections currently:", utility.list_collections())

# 4. 创建一个 test collection（如果不存在）
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128)
]
schema = CollectionSchema(fields, description="test collection for API")

collection_name = "test_collection_api"
if not utility.has_collection(collection_name):
    collection = Collection(name=collection_name, schema=schema)
else:
    collection = Collection(name=collection_name)

# 5. 插入示例数据
import random
vectors = [[random.random() for _ in range(128)] for _ in range(10)]
collection.insert([vectors])  # 如果 auto_id=True，那么不指定 id，也可以自己提供 ids

# 6. 创建索引
index_params = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 64}
}
collection.create_index(field_name="vector", index_params=index_params)

# 7. 加载 collection
collection.load()

# 8. 搜索
query_vec = [[random.random() for _ in range(128)]]
search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
res = collection.search(
    data=query_vec,
    anns_field="vector",
    param=search_params,
    limit=5
)

print("Search results:", res)
