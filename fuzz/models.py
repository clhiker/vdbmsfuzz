"""
Data classes and type definitions for VDBMS fuzzing framework
"""

from dataclasses import dataclass
from typing import Dict, List, Any

@dataclass
class TestResult:
    """Test result data class"""
    test_id: str
    operation: str
    inputs: Dict[str, Any]
    results: Dict[str, Any]
    inconsistencies: List[str]
    execution_time: Dict[str, float]