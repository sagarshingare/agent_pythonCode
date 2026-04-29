"""
Base agent class for all agents in the system
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum
import time
from utils.logger import get_logger


logger = get_logger(__name__)


class AgentStatus(Enum):
    """Agent execution status"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class AgentResult:
    """Result from agent execution"""
    agent_name: str
    status: AgentStatus
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'agent_name': self.agent_name,
            'status': self.status.value,
            'output': self.output,
            'error': self.error,
            'execution_time_ms': self.execution_time_ms,
        }


@dataclass
class AgentContext:
    """Context passed between agents"""
    xml_data: Optional[Dict[str, Any]] = None
    canonical_models: Optional[List[Any]] = None
    generated_code: Optional[str] = None
    validation_queries: Optional[str] = None
    optimized_code: Optional[str] = None
    airflow_dag: Optional[str] = None
    documentation: Optional[str] = None
    input_dataset: Optional[str] = None
    output_dataset: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'xml_data_keys': list(self.xml_data.keys()) if self.xml_data else None,
            'canonical_models_count': len(self.canonical_models) if self.canonical_models else 0,
            'generated_code_lines': len(self.generated_code.split('\n')) if self.generated_code else 0,
            'validation_queries_count': len(self.validation_queries.split('\n')) if self.validation_queries else 0,
            'metadata': self.metadata,
        }


class BaseAgent(ABC):
    """Base class for all agents"""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize agent"""
        self.name = name
        self.config = config or {}
        self.logger = get_logger(f"Agent.{name}")
        
    @abstractmethod
    def execute(self, context: AgentContext) -> AgentResult:
        """Execute agent logic - must be implemented by subclasses"""
        pass
    
    def run(self, context: AgentContext) -> AgentResult:
        """Run agent with timing and error handling"""
        self.logger.info(f"Starting agent: {self.name}")
        start_time = time.time()
        
        try:
            result = self.execute(context)
            execution_time_ms = int((time.time() - start_time) * 1000)
            result.execution_time_ms = execution_time_ms
            
            self.logger.info(
                f"Agent {self.name} completed in {execution_time_ms}ms with status {result.status.value}"
            )
            return result
            
        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            self.logger.error(f"Agent {self.name} failed: {str(e)}", exc_info=True)
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.FAILED,
                error=str(e),
                execution_time_ms=execution_time_ms,
            )
    
    def log_step(self, step: str, details: Optional[str] = None) -> None:
        """Log an execution step"""
        if details:
            self.logger.info(f"  • {step}: {details}")
        else:
            self.logger.info(f"  • {step}")
    
    def validate_context(self, context: AgentContext, required_fields: List[str]) -> None:
        """Validate that context has required fields"""
        for field in required_fields:
            if not hasattr(context, field) or getattr(context, field) is None:
                raise ValueError(f"Context missing required field: {field}")
