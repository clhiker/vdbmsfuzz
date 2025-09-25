"""
Utility functions for VDBMS fuzzing framework
"""

import json
import csv
import os
from typing import List, Dict, Any
from datetime import datetime
import logging

from models import TestResult

logger = logging.getLogger(__name__)

class ResultAnalyzer:
    """Analyze and report test results"""
    
    def __init__(self, output_dir: str = "results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def save_results(self, results: List[TestResult], filename: str = None):
        """Save test results to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fuzz_results_{timestamp}.json"
            
        output_path = os.path.join(self.output_dir, filename)
        
        # Convert results to serializable format
        serializable_results = []
        for result in results:
            serializable_result = {
                "test_id": result.test_id,
                "operation": result.operation,
                "inputs": self._make_serializable(result.inputs),
                "results": self._make_serializable(result.results),
                "inconsistencies": result.inconsistencies,
                "execution_time": result.execution_time
            }
            serializable_results.append(serializable_result)
            
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Results saved to {output_path}")
        return output_path
        
    def _make_serializable(self, obj: Any) -> Any:
        """Convert object to JSON serializable format"""
        if isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._make_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        else:
            return str(obj)
            
    def generate_report(self, results: List[TestResult]) -> str:
        """Generate comprehensive test report"""
        total_tests = len(results)
        inconsistencies_found = sum(1 for r in results if r.inconsistencies)
        success_rate = (total_tests - inconsistencies_found) / total_tests * 100 if total_tests > 0 else 0
        
        # Database success rates
        database_stats = {}
        for result in results:
            for db_name, db_result in result.results.items():
                if db_name not in database_stats:
                    database_stats[db_name] = {"success": 0, "total": 0}
                database_stats[db_name]["total"] += 1
                if db_result is not None:
                    database_stats[db_name]["success"] += 1
                    
        # Operation statistics
        operation_stats = {}
        for result in results:
            op_type = result.operation
            if op_type not in operation_stats:
                operation_stats[op_type] = {"count": 0, "inconsistencies": 0}
            operation_stats[op_type]["count"] += 1
            if result.inconsistencies:
                operation_stats[op_type]["inconsistencies"] += 1
                
        report = f"""
=== VDBMS Differential Fuzzing Test Report ===

Summary:
- Total Tests: {total_tests}
- Inconsistencies Found: {inconsistencies_found}
- Consistency Rate: {success_rate:.1f}%

Database Success Rates:
"""
        
        for db_name, stats in database_stats.items():
            success_rate = stats["success"] / stats["total"] * 100 if stats["total"] > 0 else 0
            report += f"- {db_name}: {stats['success']}/{stats['total']} ({success_rate:.1f}% success)\n"
            
        report += "\nOperation Statistics:\n"
        for op_type, stats in operation_stats.items():
            consistency_rate = (stats["count"] - stats["inconsistencies"]) / stats["count"] * 100 if stats["count"] > 0 else 0
            report += f"- {op_type}: {stats['count']} tests, {stats['inconsistencies']} inconsistencies ({consistency_rate:.1f}% consistency)\n"
            
        # Top inconsistencies
        all_inconsistencies = []
        for result in results:
            if result.inconsistencies:
                all_inconsistencies.extend([
                    f"{result.test_id} ({result.operation}): {inc}"
                    for inc in result.inconsistencies
                ])
                
        if all_inconsistencies:
            report += "\nTop Inconsistencies:\n"
            for i, inconsistency in enumerate(all_inconsistencies[:10], 1):
                report += f"{i}. {inconsistency}\n"
                
        return report
        
    def save_report(self, report: str, filename: str = None):
        """Save report to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fuzz_report_{timestamp}.txt"
            
        output_path = os.path.join(self.output_dir, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
            
        logger.info(f"Report saved to {output_path}")
        return output_path

class HealthChecker:
    """Check database health status"""
    
    def __init__(self, clients: Dict[str, Any]):
        self.clients = clients
        
    async def check_all_health(self) -> Dict[str, bool]:
        """Check health of all databases"""
        health_status = {}
        
        for db_name, client in self.clients.items():
            try:
                await client._check_health()
                health_status[db_name] = True
                logger.info(f"✓ {db_name} is healthy")
            except Exception as e:
                health_status[db_name] = False
                logger.error(f"✗ {db_name} is unhealthy: {e}")
                
        return health_status
        
    def print_health_status(self, health_status: Dict[str, bool]):
        """Print health status summary"""
        print("\n=== Database Health Status ===")
        healthy_count = sum(1 for status in health_status.values() if status)
        total_count = len(health_status)
        
        for db_name, is_healthy in health_status.items():
            status = "✓ Healthy" if is_healthy else "✗ Unhealthy"
            print(f"{db_name}: {status}")
            
        print(f"\nOverall: {healthy_count}/{total_count} databases healthy")
        
        if healthy_count < total_count:
            print("⚠️  Some databases are unhealthy. Test results may be affected.")

class ConfigValidator:
    """Validate configuration files"""
    
    @staticmethod
    def validate_config(config_data: Dict[str, Any]) -> List[str]:
        """Validate configuration data and return list of issues"""
        issues = []
        
        required_databases = ['milvus', 'chroma', 'qdrant', 'weaviate']
        
        for db_name in required_databases:
            if db_name not in config_data:
                issues.append(f"Missing configuration for {db_name}")
                continue
                
            db_config = config_data[db_name]
            
            # Check required fields
            if 'host' not in db_config or not db_config['host']:
                issues.append(f"Missing or empty host for {db_name}")
                
            if 'port' not in db_config or not isinstance(db_config['port'], int):
                issues.append(f"Missing or invalid port for {db_name}")
            elif not (1 <= db_config['port'] <= 65535):
                issues.append(f"Invalid port range for {db_name}: {db_config['port']}")
                
        # Validate test settings
        if 'test_settings' not in config_data:
            issues.append("Missing test_settings configuration")
        else:
            test_settings = config_data['test_settings']
            
            if 'vector_dimension' in test_settings:
                if not isinstance(test_settings['vector_dimension'], int) or test_settings['vector_dimension'] <= 0:
                    issues.append("Invalid vector_dimension in test_settings")
                    
            if 'timeout_seconds' in test_settings:
                if not isinstance(test_settings['timeout_seconds'], int) or test_settings['timeout_seconds'] <= 0:
                    issues.append("Invalid timeout_seconds in test_settings")
                    
        return issues
        
    @staticmethod
    def fix_common_issues(config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Fix common configuration issues"""
        fixed_config = config_data.copy()
        
        # Ensure required databases exist with defaults
        default_configs = {
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
            }
        }
        
        for db_name, default_config in default_configs.items():
            if db_name not in fixed_config:
                fixed_config[db_name] = default_config.copy()
            else:
                # Fill missing fields with defaults
                for key, value in default_config.items():
                    if key not in fixed_config[db_name]:
                        fixed_config[db_name][key] = value
                        
        # Ensure test_settings exist
        if 'test_settings' not in fixed_config:
            fixed_config['test_settings'] = {
                'vector_dimension': 128,
                'num_collections': 5,
                'num_vectors_per_collection': 1000,
                'timeout_seconds': 30
            }
        else:
            # Fill missing test settings with defaults
            default_test_settings = {
                'vector_dimension': 128,
                'num_collections': 5,
                'num_vectors_per_collection': 1000,
                'timeout_seconds': 30
            }
            
            for key, value in default_test_settings.items():
                if key not in fixed_config['test_settings']:
                    fixed_config['test_settings'][key] = value
                    
        return fixed_config