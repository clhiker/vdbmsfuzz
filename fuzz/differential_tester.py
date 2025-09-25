"""
Differential testing core logic for VDBMS fuzzing framework
"""

import asyncio
import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import json

from models import TestResult

logger = logging.getLogger(__name__)

@dataclass
class DatabaseResult:
    """Individual database operation result"""
    database: str
    success: bool
    data: Any
    error: Optional[str] = None
    execution_time: float = 0.0

class DifferentialTester:
    """Core differential testing logic"""
    
    def __init__(self, clients: Dict[str, Any]):
        self.clients = clients
        self.result_comparators = {
            'insert': self._compare_insert_results,
            'search': self._compare_search_results,
            'delete': self._compare_delete_results,
            'batch_insert': self._compare_insert_results,
            'batch_search': self._compare_batch_search_results,
            'mixed_operations': self._compare_mixed_operation_results
        }
        
    async def run_test(self, test_id: str, operation: str, inputs: Dict[str, Any]) -> TestResult:
        """Run a differential test across all databases"""
        start_time = time.time()
        
        # Execute operation on all databases
        results = await self._execute_on_all_databases(operation, inputs)
        
        # Compare results
        inconsistencies = self._compare_results(operation, results)
        
        execution_time = {name: time.time() - start_time for name in results.keys()}
        
        return TestResult(
            test_id=test_id,
            operation=operation,
            inputs=inputs,
            results={name: result.data for name, result in results.items()},
            inconsistencies=inconsistencies,
            execution_time=execution_time
        )
        
    async def _execute_on_all_databases(self, operation: str, inputs: Dict[str, Any]) -> Dict[str, DatabaseResult]:
        """Execute operation on all databases concurrently"""
        tasks = {}
        
        for db_name, client in self.clients.items():
            task = asyncio.create_task(
                self._safe_execute(db_name, client, operation, inputs)
            )
            tasks[db_name] = task
        
        try:
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            
            db_results = {}
            for i, (db_name, task) in enumerate(tasks.items()):
                result = results[i]
                if isinstance(result, Exception):
                    db_results[db_name] = DatabaseResult(
                        database=db_name,
                        success=False,
                        data=None,
                        error=str(result)
                    )
                else:
                    db_results[db_name] = result
                    
            return db_results
        except Exception as e:
            logger.error(f"Error gathering results: {e}")
            return {db_name: DatabaseResult(
                database=db_name,
                success=False,
                data=None,
                error=f"Gather error: {str(e)}"
            ) for db_name in self.clients.keys()}
        
    async def _safe_execute(self, db_name: str, client: Any, operation: str, inputs: Dict[str, Any]) -> DatabaseResult:
        """Safely execute operation with error handling and timing"""
        start_time = time.time()
        
        try:
            collection_name = inputs.get('collection_name', 'test_collection')
            
            if operation == 'insert':
                vectors = inputs['vectors']
                ids = inputs.get('ids')
                metadata = inputs.get('metadata')
                
                try:
                    data = await client.insert_vectors(
                        collection_name, vectors, ids, metadata
                    )
                except Exception as e:
                    raise e
                
            elif operation == 'search':
                query_vector = inputs['query_vector']
                limit = inputs.get('limit', 10)
                metric_type = inputs.get('metric_type', 'L2')
                
                data = await client.search_vectors(
                    collection_name, query_vector, limit, metric_type
                )
                
            elif operation == 'delete':
                ids = inputs['ids']
                
                data = await client.delete_vectors(collection_name, ids)
                
            elif operation == 'batch_insert':
                vectors = inputs['vectors']
                ids = inputs.get('ids')
                metadata = inputs.get('metadata')
                
                try:
                    data = await client.insert_vectors(
                        collection_name, vectors, ids, metadata
                    )
                except Exception as e:
                    raise e
                
            elif operation == 'batch_search':
                query_vectors = inputs['query_vectors']
                limit = inputs.get('limit', 10)
                metric_type = inputs.get('metric_type', 'L2')
                
                # Execute multiple searches
                data = []
                for query_vector in query_vectors:
                    result = await client.search_vectors(
                        collection_name, query_vector, limit, metric_type
                    )
                    data.append(result)
                    
            elif operation == 'mixed_operations':
                operations = inputs['operations']
                data = []
                
                for op in operations:
                    if op['type'] == 'insert':
                        result = await client.insert_vectors(
                            collection_name, [op['vectors']], [op['id']]
                        )
                    elif op['type'] == 'search':
                        result = await client.search_vectors(
                            collection_name, op['query_vector'], op['limit']
                        )
                    elif op['type'] == 'delete':
                        result = await client.delete_vectors(collection_name, op['ids'])
                    else:
                        continue
                        
                    data.append({'operation': op['type'], 'result': result})
                    
            else:
                raise Exception(f"Unknown operation: {operation}")
                
            execution_time = time.time() - start_time
            
            return DatabaseResult(
                database=db_name,
                success=True,
                data=data,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error executing {operation} on {db_name}: {e}")
            
            return DatabaseResult(
                database=db_name,
                success=False,
                data=None,
                error=str(e),
                execution_time=execution_time
            )
            
    def _compare_results(self, operation: str, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare results across databases"""
        inconsistencies = []
        
        # Check if some databases failed while others succeeded
        successful_results = {k: v for k, v in results.items() if v.success}
        failed_results = {k: v for k, v in results.items() if not v.success}
        
        if successful_results and failed_results:
            inconsistencies.append(
                f"Some databases succeeded while others failed. "
                f"Success: {list(successful_results.keys())}, "
                f"Failed: {list(failed_results.keys())}"
            )
            
        # Only compare successful results
        if len(successful_results) < 2:
            return inconsistencies
            
        # Use appropriate comparator
        comparator = self.result_comparators.get(operation, self._compare_generic_results)
        operation_inconsistencies = comparator(successful_results)
        inconsistencies.extend(operation_inconsistencies)
        
        return inconsistencies
        
    def _compare_insert_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare insert operation results"""
        inconsistencies = []
        
        # Check if all databases accepted the same number of vectors
        success_counts = {}
        for db_name, result in results.items():
            if hasattr(result.data, 'get'):
                # Milvus, Qdrant style response
                if 'insert_count' in result.data:
                    success_counts[db_name] = result.data['insert_count']
                elif 'status' in result.data:
                    success_counts[db_name] = len(result.data.get('insert_ids', []))
            elif isinstance(result.data, dict):
                # Generic dict response
                success_counts[db_name] = len(result.data.get('ids', []))
            else:
                success_counts[db_name] = 1  # Default assumption
                
        if len(set(success_counts.values())) > 1:
            inconsistencies.append(
                f"Insert count mismatch: {success_counts}"
            )
            
        return inconsistencies
        
    def _compare_search_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare search operation results"""
        inconsistencies = []
        
        # Extract result IDs from each database response
        search_results = {}
        for db_name, result in results.items():
            try:
                result_ids = self._extract_search_result_ids(result.data)
                search_results[db_name] = set(result_ids)
            except Exception as e:
                logger.warning(f"Could not extract search results from {db_name}: {e}")
                search_results[db_name] = set()
                
        # Check if all databases returned same top results (at least some overlap)
        if search_results:
            reference_db = next(iter(search_results))
            reference_ids = search_results[reference_db]
            
            for db_name, ids in search_results.items():
                if db_name != reference_db:
                    intersection = reference_ids.intersection(ids)
                    max_len = max(len(reference_ids), len(ids))
                    if max_len == 0:
                        overlap_percent = 0
                    else:
                        overlap_percent = len(intersection) / max_len * 100
                    
                    if overlap_percent < 50 and len(reference_ids) > 0 and len(ids) > 0:
                        inconsistencies.append(
                            f"Search results differ significantly between {reference_db} and {db_name}. "
                            f"Overlap: {overlap_percent:.1f}%"
                        )
                        
        return inconsistencies
        
    def _compare_batch_search_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare batch search operation results"""
        inconsistencies = []
        
        # Compare each query result
        if not results:
            return inconsistencies
            
        reference_db = next(iter(results))
        reference_data = results[reference_db].data
        
        if not isinstance(reference_data, list):
            return inconsistencies
            
        for query_idx in range(len(reference_data)):
            query_results = {}
            for db_name, result in results.items():
                try:
                    if isinstance(result.data, list) and query_idx < len(result.data):
                        result_ids = self._extract_search_result_ids(result.data[query_idx])
                        query_results[db_name] = set(result_ids)
                    else:
                        query_results[db_name] = set()
                except Exception as e:
                    logger.warning(f"Could not extract batch search results from {db_name}: {e}")
                    query_results[db_name] = set()
                    
            # Compare query results
            if len(query_results) > 1:
                reference_ids = query_results[reference_db]
                for db_name, ids in query_results.items():
                    if db_name != reference_db:
                        intersection = reference_ids.intersection(ids)
                        max_len = max(len(reference_ids), len(ids))
                        if max_len == 0:
                            overlap_percent = 0
                        else:
                            overlap_percent = len(intersection) / max_len * 100
                        
                        if overlap_percent < 50 and len(reference_ids) > 0 and len(ids) > 0:
                            inconsistencies.append(
                                f"Batch search results differ at query {query_idx} between {reference_db} and {db_name}. "
                                f"Overlap: {overlap_percent:.1f}%"
                            )
                            
        return inconsistencies
        
    def _compare_delete_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare delete operation results"""
        inconsistencies = []
        
        # Check if all databases reported success
        success_status = {}
        for db_name, result in results.items():
            if hasattr(result.data, 'get'):
                # Milvus, Qdrant style response
                success = result.data.get('status', '').lower() in ['success', 'ok', 'completed']
            elif isinstance(result.data, dict):
                success = result.data.get('success', True)
            else:
                success = True  # Default assumption
                
            success_status[db_name] = success
            
        if not all(success_status.values()):
            inconsistencies.append(
                f"Delete operation status mismatch: {success_status}"
            )
            
        return inconsistencies
        
    def _compare_mixed_operation_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Compare mixed operation results"""
        inconsistencies = []
        
        # Check if all databases executed all operations successfully
        operation_counts = {}
        for db_name, result in results.items():
            if isinstance(result.data, list):
                operation_counts[db_name] = len(result.data)
            else:
                operation_counts[db_name] = 0
                
        if len(set(operation_counts.values())) > 1:
            inconsistencies.append(
                f"Mixed operations execution count mismatch: {operation_counts}"
            )
            
        return inconsistencies
        
    def _compare_generic_results(self, results: Dict[str, DatabaseResult]) -> List[str]:
        """Generic result comparison for unknown operations"""
        inconsistencies = []
        
        # Simple success/failure comparison
        success_status = {k: v.success for k, v in results.items()}
        
        if not all(success_status.values()):
            inconsistencies.append(
                f"Operation success status mismatch: {success_status}"
            )
            
        return inconsistencies
        
    def _extract_search_result_ids(self, data: Any) -> List[str]:
        """Extract result IDs from search response"""
        result_ids = []
        
        if isinstance(data, dict):
            # Handle different response formats
            if 'data' in data and isinstance(data['data'], list):
                # Milvus format
                for item in data['data']:
                    if isinstance(item, dict) and 'id' in item:
                        result_ids.append(str(item['id']))
            elif 'ids' in data and isinstance(data['ids'], list):
                # Simple ids array
                result_ids.extend([str(id) for id in data['ids']])
            elif 'result' in data:
                # Nested result format
                result_ids.extend(self._extract_search_result_ids(data['result']))
            elif 'Get' in data:
                # Weaviate GraphQL format
                get_data = data.get('Get', {})
                for collection_name in get_data.values():
                    if isinstance(collection_name, list):
                        for item in collection_name:
                            if isinstance(item, dict) and '_additional' in item:
                                additional = item['_additional']
                                if 'id' in additional:
                                    result_ids.append(str(additional['id']))
            elif 'points' in data and isinstance(data['points'], list):
                # Qdrant format
                for point in data['points']:
                    if 'id' in point:
                        result_ids.append(str(point['id']))
                        
        elif isinstance(data, list):
            # Handle list responses (Chroma format)
            if len(data) > 0 and isinstance(data[0], list):
                # Chroma returns nested lists
                for sublist in data:
                    if isinstance(sublist, list):
                        result_ids.extend([str(id) for id in sublist])
            else:
                # Single list of results
                result_ids.extend([str(id) for id in data])
                
        return result_ids