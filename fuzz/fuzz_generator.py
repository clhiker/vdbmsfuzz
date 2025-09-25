"""
Fuzz test generator for VDBMS differential testing
"""

import random
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
import string

@dataclass
class FuzzConfig:
    """Fuzzing configuration"""
    vector_dimension: int = 128
    max_vectors_per_batch: int = 100
    max_metadata_size: int = 10
    probability_invalid_vector: float = 0.1
    probability_large_vector: float = 0.05
    probability_negative_floats: float = 0.1
    probability_special_chars: float = 0.05

class FuzzGenerator:
    """Generate fuzz test cases for vector databases"""
    
    def __init__(self, config: Optional[FuzzConfig] = None):
        self.config = config or FuzzConfig()
        self.operations = [
            'insert',
            'search', 
            'delete',
            'batch_insert',
            'batch_search',
            'mixed_operations'
        ]
        
    def generate_test(self) -> Tuple[str, Dict[str, Any]]:
        """Generate a random test case"""
        operation = random.choice(self.operations)
        
        if operation == 'insert':
            return 'insert', self._generate_insert_params()
        elif operation == 'search':
            return 'search', self._generate_search_params()
        elif operation == 'delete':
            return 'delete', self._generate_delete_params()
        elif operation == 'batch_insert':
            return 'batch_insert', self._generate_batch_insert_params()
        elif operation == 'batch_search':
            return 'batch_search', self._generate_batch_search_params()
        elif operation == 'mixed_operations':
            return 'mixed_operations', self._generate_mixed_operations_params()
        else:
            return 'unknown', {}
            
    def _generate_vector(self, dimension: Optional[int] = None) -> List[float]:
        """Generate a fuzzed vector"""
        if dimension is None:
            dimension = self.config.vector_dimension
            
        if random.random() < self.config.probability_invalid_vector:
            # Invalid vector dimensions
            if random.random() < 0.5:
                # Empty vector
                return []
            else:
                # Wrong dimension
                return [random.uniform(-1, 1) for _ in range(random.randint(1, 256))]
                
        if random.random() < self.config.probability_large_vector:
            # Large dimension vector
            dimension = random.randint(256, 1000)
            
        vector = []
        for i in range(dimension):
            if random.random() < self.config.probability_negative_floats:
                # Include negative values
                vector.append(random.uniform(-10, 10))
            else:
                # Normal case
                vector.append(random.uniform(-1, 1))
                
        # Add some special float values
        if random.random() < 0.01:
            vector.extend([float('inf'), float('-inf')])
        if random.random() < 0.01:
            vector.append(float('nan'))
            
        return vector
        
    def _generate_metadata(self) -> Dict[str, Any]:
        """Generate fuzzed metadata"""
        metadata = {}
        
        # Random number of metadata fields
        num_fields = random.randint(0, self.config.max_metadata_size)
        
        for i in range(num_fields):
            field_type = random.choice(['string', 'number', 'boolean', 'list', 'nested'])
            
            if field_type == 'string':
                if random.random() < self.config.probability_special_chars:
                    # String with special characters
                    metadata[f'field_{i}'] = ''.join(
                        random.choices(string.ascii_letters + string.digits + '!@#$%^&*()', 
                                     k=random.randint(1, 50))
                    )
                else:
                    # Normal string
                    metadata[f'field_{i}'] = ''.join(
                        random.choices(string.ascii_letters + string.digits, 
                                     k=random.randint(1, 20))
                    )
            elif field_type == 'number':
                metadata[f'field_{i}'] = random.randint(-1000000, 1000000)
            elif field_type == 'boolean':
                metadata[f'field_{i}'] = random.choice([True, False])
            elif field_type == 'list':
                metadata[f'field_{i}'] = [
                    random.randint(0, 100) for _ in range(random.randint(1, 10))
                ]
            elif field_type == 'nested':
                metadata[f'field_{i}'] = {
                    'nested_value': random.choice(['nested_string', 42, True])
                }
                
        return metadata
        
    def _generate_insert_params(self) -> Dict[str, Any]:
        """Generate insert operation parameters"""
        num_vectors = random.randint(1, self.config.max_vectors_per_batch)
        vectors = [self._generate_vector() for _ in range(num_vectors)]
        ids = [f"id_{random.randint(0, 1000000)}" for _ in range(num_vectors)]
        
        # Generate metadata for some vectors
        metadata = []
        for i in range(num_vectors):
            if random.random() < 0.7:
                metadata.append(self._generate_metadata())
            else:
                metadata.append(None)
                
        return {
            'vectors': vectors,
            'ids': ids,
            'metadata': metadata,
            'collection_name': self._generate_collection_name()
        }
        
    def _generate_search_params(self) -> Dict[str, Any]:
        """Generate search operation parameters"""
        query_vector = self._generate_vector()
        limit = random.randint(1, 100)
        metric_type = random.choice(['L2', 'cosine', 'ip'])
        
        return {
            'query_vector': query_vector,
            'limit': limit,
            'metric_type': metric_type,
            'collection_name': self._generate_collection_name()
        }
        
    def _generate_delete_params(self) -> Dict[str, Any]:
        """Generate delete operation parameters"""
        num_ids = random.randint(1, 50)
        ids = [f"id_{random.randint(0, 1000000)}" for _ in range(num_ids)]
        
        # Add some invalid IDs
        if random.random() < 0.2:
            ids.extend(['invalid_id_1', 'nonexistent_id', ''])
            
        return {
            'ids': ids,
            'collection_name': self._generate_collection_name()
        }
        
    def _generate_batch_insert_params(self) -> Dict[str, Any]:
        """Generate batch insert operation parameters"""
        return self._generate_insert_params()
        
    def _generate_batch_search_params(self) -> Dict[str, Any]:
        """Generate batch search operation parameters"""
        num_queries = random.randint(1, 10)
        query_vectors = [self._generate_vector() for _ in range(num_queries)]
        limit = random.randint(1, 50)
        metric_type = random.choice(['L2', 'cosine', 'ip'])
        
        return {
            'query_vectors': query_vectors,
            'limit': limit,
            'metric_type': metric_type,
            'collection_name': self._generate_collection_name()
        }
        
    def _generate_mixed_operations_params(self) -> Dict[str, Any]:
        """Generate mixed operations parameters"""
        operations = []
        num_operations = random.randint(2, 10)
        
        for i in range(num_operations):
            op_type = random.choice(['insert', 'search', 'delete'])
            
            if op_type == 'insert':
                op_params = {
                    'type': 'insert',
                    'vectors': [self._generate_vector()],
                    'id': f"id_{random.randint(0, 1000000)}"
                }
            elif op_type == 'search':
                op_params = {
                    'type': 'search',
                    'query_vector': self._generate_vector(),
                    'limit': random.randint(1, 20)
                }
            elif op_type == 'delete':
                op_params = {
                    'type': 'delete',
                    'ids': [f"id_{random.randint(0, 1000000)}"]
                }
                
            operations.append(op_params)
            
        return {
            'operations': operations,
            'collection_name': self._generate_collection_name()
        }
        
    def _generate_collection_name(self) -> str:
        """Generate a collection name"""
        if random.random() < 0.1:
            # Invalid collection name
            invalid_names = ['', 'invalid-name', '123', 'name with spaces', '!@#$%']
            return random.choice(invalid_names)
        else:
            # Valid collection name
            return f"test_collection_{random.randint(1, 1000)}"
            
    def generate_edge_case_test(self) -> Tuple[str, Dict[str, Any]]:
        """Generate edge case test"""
        edge_case = random.choice([
            'empty_vector',
            'very_large_vector', 
            'nan_values',
            'inf_values',
            'very_large_batch',
            'empty_metadata',
            'malformed_id',
            'nonexistent_collection'
        ])
        
        if edge_case == 'empty_vector':
            return 'insert', {
                'vectors': [[]],
                'ids': ['empty_id'],
                'metadata': [{}],
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'very_large_vector':
            return 'insert', {
                'vectors': [[random.uniform(-1, 1) for _ in range(10000)]],
                'ids': ['large_vector_id'],
                'metadata': [{}],
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'nan_values':
            vector = [float('nan') if i % 10 == 0 else random.uniform(-1, 1) 
                     for i in range(self.config.vector_dimension)]
            return 'search', {
                'query_vector': vector,
                'limit': 10,
                'metric_type': 'L2',
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'inf_values':
            vector = [float('inf') if i % 10 == 0 else random.uniform(-1, 1) 
                     for i in range(self.config.vector_dimension)]
            return 'search', {
                'query_vector': vector,
                'limit': 10,
                'metric_type': 'L2',
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'very_large_batch':
            num_vectors = 1000
            vectors = [[random.uniform(-1, 1) for _ in range(self.config.vector_dimension)] 
                      for _ in range(num_vectors)]
            ids = [f"id_{i}" for i in range(num_vectors)]
            return 'batch_insert', {
                'vectors': vectors,
                'ids': ids,
                'metadata': [{} for _ in range(num_vectors)],
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'empty_metadata':
            return 'insert', {
                'vectors': [self._generate_vector()],
                'ids': ['empty_metadata_id'],
                'metadata': [{}],
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'malformed_id':
            return 'delete', {
                'ids': ['', 'invalid@id', 'id with spaces'],
                'collection_name': self._generate_collection_name()
            }
        elif edge_case == 'nonexistent_collection':
            return 'search', {
                'query_vector': self._generate_vector(),
                'limit': 10,
                'metric_type': 'L2',
                'collection_name': 'nonexistent_collection'
            }
        else:
            return 'unknown', {}