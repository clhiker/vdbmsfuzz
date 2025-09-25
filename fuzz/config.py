"""
Configuration management for VDBMS fuzzing framework
"""

import json
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class DatabaseConfig:
    """Database configuration data class"""
    host: str
    port: int
    username: str = ""
    password: str = ""
    database: str = "default"
    collection: str = "test_collection"

class Config:
    """Configuration manager"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
        except FileNotFoundError:
            config_data = self._get_default_config()
            self._save_config(config_data)
        
        self.milvus = DatabaseConfig(**config_data.get('milvus', {}))
        self.chroma = DatabaseConfig(**config_data.get('chroma', {}))
        self.qdrant = DatabaseConfig(**config_data.get('qdrant', {}))
        self.weaviate = DatabaseConfig(**config_data.get('weaviate', {}))
        
        self.test_settings = config_data.get('test_settings', {
            'vector_dimension': 128,
            'num_collections': 5,
            'num_vectors_per_collection': 1000,
            'timeout_seconds': 30
        })
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'milvus': {
                'host': 'localhost',
                'port': 19530,
                'database': 'default',
                'collection': 'test_collection'
            },
            'chroma': {
                'host': 'localhost',
                'port': 8000,
                'collection': 'test_collection'
            },
            'qdrant': {
                'host': 'localhost',
                'port': 6333,
                'collection': 'test_collection'
            },
            'weaviate': {
                'host': 'localhost',
                'port': 8080,
                'collection': 'TestCollection'
            },
            'test_settings': {
                'vector_dimension': 128,
                'num_collections': 5,
                'num_vectors_per_collection': 1000,
                'timeout_seconds': 30
            }
        }
    
    def _save_config(self, config_data: Dict[str, Any]):
        """Save configuration to file"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)