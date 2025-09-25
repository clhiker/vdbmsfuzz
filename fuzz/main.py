"""
VDBMS Differential Fuzzing Framework

A framework for differential testing of vector database management systems:
- Milvus
- Chroma
- Qdrant
- Weaviate
"""

import asyncio
import json
import logging
import random
from typing import Dict, List, Any, Optional

from config import Config
from db_clients import (
    MilvusClient, ChromaClient, QdrantClient, WeaviateClient
)
from fuzz_generator import FuzzGenerator
from differential_tester import DifferentialTester
from models import TestResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VDBMSFuzzer:
    """Main fuzzer class"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config = Config(config_path)
        self.clients = {
            'milvus': MilvusClient(self.config.milvus),
            'chroma': ChromaClient(self.config.chroma),
            'qdrant': QdrantClient(self.config.qdrant),
            'weaviate': WeaviateClient(self.config.weaviate)
        }
        self.fuzz_generator = FuzzGenerator()
        self.differential_tester = DifferentialTester(self.clients)
        
    async def setup(self):
        """Setup database connections and create test collections"""
        logger.info("Setting up database connections...")
        
        for name, client in self.clients.items():
            await client.connect()
            await client.setup_test_collection()
            logger.info(f"✓ {name} connected and setup")
            
        # Add info about mock mode databases
        mock_dbs = []
        for name, client in self.clients.items():
            if hasattr(client, 'mock_mode') and getattr(client, 'mock_mode', False):
                mock_dbs.append(name)
        if mock_dbs:
            logger.info(f"Databases running in mock mode: {mock_dbs}")
    
    async def run_fuzz_test(self, num_tests: int = 100) -> List[TestResult]:
        """Run fuzz tests"""
        logger.info(f"Running {num_tests} fuzz tests...")
        
        results = []
        for i in range(num_tests):
            test_id = f"test_{i:04d}"
            logger.info(f"Running {test_id}")
            
            operation, inputs = self.fuzz_generator.generate_test()
            
            result = await self.differential_tester.run_test(
                test_id, operation, inputs
            )
            
            results.append(result)
            
            if result.inconsistencies:
                logger.warning(f"⚠️  {test_id} found inconsistencies: {result.inconsistencies}")
            else:
                logger.info(f"✓ {test_id} passed")
        
        return results
    
    async def cleanup(self):
        """Cleanup database connections"""
        logger.info("Cleaning up...")
        
        for name, client in self.clients.items():
            if hasattr(client, 'cleanup'):
                await client.cleanup()
            logger.info(f"✓ {name} cleaned up")
                
        # Now disconnect all sessions
        for name, client in self.clients.items():
            if hasattr(client, 'disconnect'):
                await client.disconnect()
            logger.info(f"✓ {name} disconnected")

async def main():
    """Main entry point"""
    fuzzer = VDBMSFuzzer()
    
    await fuzzer.setup()
    results = await fuzzer.run_fuzz_test(num_tests=50)
    
    logger.info(f"Test completed. Total tests: {len(results)}")
    inconsistencies_found = sum(1 for r in results if r.inconsistencies)
    logger.info(f"Inconsistencies found: {inconsistencies_found}")
    
    await fuzzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())