# Informatica to PySpark Migration Agent

A production-grade, enterprise-level Agentic AI system that migrates Informatica ETL mappings (XML) into PySpark code with comprehensive validation, optimization, orchestration, documentation, and knowledge learning capabilities.

## 🎯 Features

- **XML Parsing & Conversion**: Parses Informatica PowerCenter XML exports with full schema extraction
- **Canonical Model Building**: Normalizes Informatica structures into a deterministic canonical format
- **Multi-Agent Architecture**: Orchestrated agents for planning, specification, code generation, validation, optimization, and documentation
- **PySpark Code Generation**: Auto-generates production-grade PySpark code with type hints and documentation
- **SQL Validation**: Creates validation queries for row counts, EXCEPT queries, and hash comparisons
- **Performance Optimization**: Applies repartitioning, caching, and broadcast join strategies
- **Airflow DAG Generation**: Creates Apache Airflow DAGs for job orchestration
- **Knowledge Graph**: Builds transformation lineage graph for impact analysis
- **RAG System**: Maintains knowledge base and memory store for learning and pattern reuse
- **Sample Data Generation**: Creates input/output datasets aligned with transformation logic
- **Comprehensive Documentation**: Auto-generates Markdown documentation from mappings

## 🏗️ Architecture

### Multi-Agent System

```
┌─────────────────────────────────────────────────────────────────┐
│                    PlannerAgent                                 │
│        Orchestrates entire migration flow                       │
└──────────────┬────────────────────────────────────────────────────┘
               │
               ├──► SpecAgent
               │    (XML → JSON → Canonical)
               │
               ├──► CodeGenAgent
               │    (Canonical → PySpark Code)
               │
               ├──► ValidationAgent
               │    (Generate Validation Queries)
               │
               ├──► OptimizationAgent
               │    (Apply Performance Optimizations)
               │
               ├──► OrchestrationAgent
               │    (Generate Airflow DAG)
               │
               └──► DocumentationAgent
                    (Generate Documentation)
```

### Knowledge Systems

- **Knowledge Base**: Stores transformation rules, function mappings, patterns, and best practices
- **Memory Store**: Persists execution history, learned patterns, errors, and suggestions
- **Knowledge Graph**: NetworkX-based lineage graph for impact analysis and traceability

### Transformers

- **Expression Transformer**: Converts Informatica expressions (IIF, NVL, DECODE, etc.) to PySpark functions
- **Join Transformer**: Handles join transformations with optimization
- **Aggregator Transformer**: Processes aggregations and groupings
- **Maplet Handler**: Expands reusable transformations (maplets)

## 📋 Project Structure

```
migration-agent/
│
├── config/
│   ├── settings.yaml           # System and agent configuration
│   ├── agent_config.yaml       # Agent-specific settings
│   └── mappings.yaml           # Transformation function mappings
│
├── data/
│   ├── sample_informatic_export.xml  # Input Informatica XML
│   ├── generated_input.csv      # Generated sample input data
│   └── generated_output.csv     # Generated sample output data
│
├── ingestion/
│   └── xml_parser.py            # Informatica XML parser
│
├── canonical/
│   ├── model_builder.py         # Canonical model builder
│   └── xml_to_json.py           # XML to JSON converter
│
├── agents/
│   ├── base_agent.py            # Base agent class
│   ├── planner_agent.py         # Planner agent
│   ├── spec_agent.py            # Specification agent
│   ├── codegen_agent.py         # Code generation agent
│   ├── validation_agent.py      # Validation agent
│   ├── optimization_agent.py    # Optimization agent
│   ├── orchestration_agent.py   # Orchestration agent
│   └── documentation_agent.py   # Documentation agent
│
├── rag/
│   ├── knowledge_base.py        # Knowledge base for patterns
│   ├── retriever.py             # Semantic search retriever (optional)
│   └── memory_store.py          # Memory store for learning
│
├── knowledge_graph/
│   └── graph_builder.py         # Knowledge graph builder
│
├── transformers/
│   ├── expression.py            # Expression transformer
│   ├── join.py                  # Join transformer
│   ├── aggregator.py            # Aggregator transformer
│   └── maplet_handler.py        # Maplet handler
│
├── output/
│   ├── pyspark/                 # Generated PySpark code
│   ├── sql/                     # Generated SQL validation queries
│   ├── dags/                    # Generated Airflow DAGs
│   └── docs/                    # Generated documentation
│
├── utils/
│   ├── logger.py                # Centralized logging
│   └── helpers.py               # Utility functions
│
├── main.py                      # Main orchestrator
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🚀 Installation

### Prerequisites

- Python 3.8+
- pip

### Setup

1. **Clone/Extract the project**:
```bash
cd migration-agent
```

2. **Create virtual environment**:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Verify installation**:
```bash
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
```

## 📖 Usage

### Basic Execution

```bash
python main.py \
  --xml data/sample_informatic_export.xml \
  --output output \
  --data data
```


```bash
python main.py \
  --xml data/informatic_export.xml \
  --output output2 \
  --data data2
```

### With Custom Configuration

```bash
python main.py \
  --xml path/to/your/mapping.xml \
  --output /custom/output/path \
  --data /custom/data/path
```

### Output Files

After execution, check:

- **PySpark Code**: `output/pyspark/job.py` - Main PySpark job
- **SQL Validation**: `output/sql/validation.sql` - Validation queries
- **Airflow DAG**: `output/dags/informatica_migration_dag.py` - Orchestration
- **Documentation**: `output/docs/README.md` - Comprehensive documentation
- **Sample Data**: `data/generated_input.csv` & `data/generated_output.csv`
- **Execution Log**: `output/migration_*.log` - Detailed execution log

## 🔧 Configuration

### settings.yaml

```yaml
system:
  name: "Informatica Migration Agent"
  version: "1.0.0"
  environment: "production"

xml_config:
  input_file: "data/sample_informatic_export.xml"
  parse_maplets: true

codegen:
  pyspark_version: "3.3.0"
  repartition_default: 8
  enable_broadcast_joins: true
  broadcast_threshold_mb: 100

data_generation:
  enable_input_generation: true
  input_sample_rows: 100

optimization:
  enable_repartitioning: true
  enable_caching: true
  enable_broadcast_optimization: true
```

### agent_config.yaml

Configure individual agent behavior:

```yaml
agents:
  planner:
    enabled: true
    timeout_seconds: 300
    
  codegen:
    enabled: true
    optimization_level: 2
    include_documentation: true
```

### mappings.yaml

Function and transformation type mappings for conversion rules.

## 🧠 Agent Execution Flow

### 1. PlannerAgent
- Creates execution plan with 6-7 sequential steps
- Validates dependencies between agents
- Returns plan to orchestrator

### 2. SpecAgent
- Parses Informatica XML file using lxml
- Converts to JSON structure
- Builds canonical model with normalization
- Outputs: JSON file, canonical models

### 3. CodeGenAgent
- Generates PySpark code from canonical model
- Applies expression transformer for Informatica functions
- Creates modular transformation classes
- Outputs: job.py, mapping-specific files

### 4. ValidationAgent
- Generates SQL validation queries
- Creates row count checks
- Generates EXCEPT queries for reconciliation
- Outputs: validation.sql

### 5. OptimizationAgent
- Adds Spark configuration for optimization
- Enables adaptive query execution
- Configures broadcast joins
- Outputs: job_optimized.py

### 6. OrchestrationAgent
- Generates Apache Airflow DAG
- Creates task dependencies
- Adds alerts and SLA monitoring
- Outputs: informatica_migration_dag.py

### 7. DocumentationAgent
- Generates Markdown documentation
- Creates data lineage diagrams
- Documents transformation logic
- Outputs: mapping_doc.md, README.md

## 📊 Supported Informatica Features

### Transformations
- ✅ Expression
- ✅ Filter
- ✅ Aggregator
- ✅ Lookup Procedure
- ✅ Source Qualifier
- ⚠️ Join (partial)
- ⚠️ Router (basic)
- 🔄 Maplet (reusable transformations)

### Functions (Informatica → PySpark)
- `IIF` → `F.when().otherwise()`
- `NVL` → `F.coalesce()`
- `DECODE` → `F.when().otherwise()`
- `TO_CHAR`, `TO_DATE`, `TO_TIMESTAMP` → Type casting
- `SUBSTR`, `LPAD`, `RPAD` → String operations
- `UPPER`, `LOWER`, `TRIM` → String functions
- `ROUND`, `ABS`, `SIGN` → Numeric functions
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` → Aggregations
- `IS_NULL`, `IS_NOT_NULL`, `IS_NUMBER` → Conditionals

## 🧪 Testing

### Unit Tests

```bash
pytest tests/ -v
```

### Sample Data Validation

Generated sample datasets can be used for testing:

```bash
# Load and validate generated data
python -c "
import pandas as pd
df_input = pd.read_csv('data/generated_input.csv')
print(df_input.head())
"
```

## 📈 Monitoring & Logging

### Log Levels
- **DEBUG**: Detailed execution info
- **INFO**: Agent execution steps
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures

### Log Files
- Main log: `output/migration_YYYYMMDD_HHMMSS.log`
- Contains full execution trace with timestamps
- Aggregated summary in `output/execution_summary.json`

## 🔍 Knowledge Systems

### Knowledge Base (`rag/knowledge_base.json`)
- Transformation rules by category
- Function mappings (Informatica → PySpark)
- Design patterns and best practices
- Reusable snippets

### Memory Store (`rag/memory_store.json`)
- Execution history with results
- Learned patterns from past runs
- Error logs for debugging
- Optimization suggestions

### Knowledge Graph (`output/transformation_graph.graphml`)
- Nodes: Sources, transformations, targets
- Edges: Data flows between nodes
- Metadata: Field mappings, lineage
- Format: GraphML (compatible with Gephi, yEd)

## 🎓 Knowledge Learning & Reuse

The system learns from each execution:

1. **Pattern Learning**: Common transformation patterns are captured
2. **Error Handling**: Failed transformations are logged with context
3. **Optimization Tips**: Performance bottlenecks generate suggestions
4. **Mapping Cache**: Successful mappings are cached for reuse

## 🔐 Production Considerations

### Security
- No credentials stored in code
- Use environment variables for secrets
- Validate all user inputs
- Sanitize file paths

### Performance
- Adaptive Query Execution enabled
- Broadcast joins for small tables
- Repartitioning for skewed data
- Columnar storage optimization

### Reliability
- Retry logic with exponential backoff
- Comprehensive error handling
- Transaction safety checks
- Data validation at each step

## 📝 Supported Informatica Mapping Features

### ✅ Fully Supported
- Source Qualifiers with filters
- Expression transformations
- Filter transformations
- Aggregator with grouping
- Lookup procedures
- Mapping variables

### ⚠️ Partially Supported
- Stored procedures (basic)
- Union transformations
- Complex joins
- Router transformations

### 🚧 Future Enhancements
- Incremental loading
- CDC (Change Data Capture)
- SCD (Slowly Changing Dimensions)
- Complex hierarchies
- Informatica native functions

## 🐛 Troubleshooting

### Common Issues

**Issue**: `Import "lxml" could not be resolved`
```bash
pip install lxml
```

**Issue**: `NetworkX not found`
```bash
pip install networkx
```

**Issue**: XML parsing errors
- Ensure XML file is valid (well-formed)
- Check XML encoding (should be UTF-8 or ISO-8859-1)
- Validate against PowerCenter DTD

**Issue**: PySpark configuration errors
- Check Spark version compatibility
- Verify Java installation
- Review Spark configuration

## 📚 Examples

### Example 1: Simple Expression Transformation

**Informatica XML**:
```xml
<TRANSFORMFIELD NAME="v_PP_NUM">
  <EXPRESSION>IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))</EXPRESSION>
</TRANSFORMFIELD>
```

**Generated PySpark**:
```python
df_transformed = df.selectExpr(
    "F.when(PP_NUM < 10, F.lpad(F.cast(PP_NUM, StringType()), 2, '0')).otherwise(F.cast(PP_NUM, StringType())) as v_PP_NUM"
)
```

### Example 2: Filter + Aggregation

**Informatica XML**:
```xml
<TRANSFORMATION TYPE="Filter">
  <TABLEATTRIBUTE NAME="Filter Condition" VALUE="RECORD_TYPE_FLAG = 'D'"/>
</TRANSFORMATION>
<TRANSFORMATION TYPE="Aggregator">
  <TRANSFORMFIELD EXPRESSION="COUNT(SSN)" NAME="o_DETAIL_RECORD_COUNT"/>
</TRANSFORMATION>
```

**Generated PySpark**:
```python
df_filtered = df.filter(RECORD_TYPE_FLAG == 'D')
df_agg = df_filtered.agg(F.count("SSN").alias("DETAIL_RECORD_COUNT"))
```

## 🤝 Contributing

Contributions are welcome! Areas for enhancement:

1. Additional Informatica transformation types
2. More PySpark optimization strategies
3. Extended validation rules
4. Enhanced documentation generation
5. Support for more data sources

## 📄 License

This project is provided as-is for enterprise data integration purposes.

## 📞 Support

For issues or questions:

1. Check the log files in `output/`
2. Review the generated documentation in `output/docs/`
3. Verify XML structure against Informatica schema
4. Check configuration in `config/`

## 🎯 Roadmap

### v1.1 (Planned)
- [ ] Support for Informatica workflows
- [ ] Delta Lake integration
- [ ] Real-time change data capture
- [ ] Multi-source federation

### v2.0 (Planned)
- [ ] LLM-based code review and optimization
- [ ] Interactive UI for migration planning
- [ ] Performance profiling and recommendations
- [ ] Automated testing framework

## 📖 Additional Resources

- [Informatica Documentation](https://docs.informatica.com)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

---

**Version**: 1.0.0  
**Last Updated**: 2024-01-01  
**Author**: Data Engineering Team  
**Status**: Production-Ready ✅
