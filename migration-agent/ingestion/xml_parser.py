"""
Informatica XML parser module
Parses Informatica PowerCenter XML export files
"""
from typing import Dict, List, Any, Optional, Tuple
from lxml import etree
from pathlib import Path
import json
from utils.logger import get_logger


logger = get_logger(__name__)


class InformaticaXMLParser:
    """Parse Informatica PowerCenter XML export files"""
    
    def __init__(self, xml_path: str):
        """Initialize parser with XML file path"""
        self.xml_path = xml_path
        self.tree = None
        self.root = None
        self.namespaces = {}
        
    def parse(self) -> Dict[str, Any]:
        """Parse XML file and return structured data"""
        try:
            logger.info(f"Parsing XML file: {self.xml_path}")
            
            with open(self.xml_path, 'rb') as f:
                self.tree = etree.parse(f)
                self.root = self.tree.getroot()
            
            logger.info("XML file loaded successfully")
            
            result = {
                'repository': self._parse_repository(),
                'mappings': self._parse_mappings(),
                'sources': self._parse_sources(),
                'targets': self._parse_targets(),
                'transformations': self._parse_transformations_global(),
                'maplets': self._parse_maplets(),
            }
            
            logger.info(f"Parsed {len(result['mappings'])} mappings")
            logger.info(f"Parsed {len(result['sources'])} sources")
            logger.info(f"Parsed {len(result['targets'])} targets")
            
            return result
        except Exception as e:
            logger.error(f"Error parsing XML: {str(e)}", exc_info=True)
            raise
    
    def _parse_repository(self) -> Dict[str, Any]:
        """Parse repository metadata"""
        repo = self.root.find('.//REPOSITORY')
        if repo is None:
            return {}
        
        return {
            'name': repo.get('NAME', ''),
            'version': repo.get('VERSION', ''),
            'codepage': repo.get('CODEPAGE', ''),
            'databasetype': repo.get('DATABASETYPE', ''),
        }
    
    def _parse_sources(self) -> Dict[str, Dict[str, Any]]:
        """Parse all SOURCE definitions"""
        sources = {}
        for source in self.root.findall('.//SOURCE'):
            source_name = source.get('NAME', '')
            sources[source_name] = {
                'name': source_name,
                'businessname': source.get('BUSINESSNAME', ''),
                'databasetype': source.get('DATABASETYPE', ''),
                'dbdname': source.get('DBDNAME', ''),
                'description': source.get('DESCRIPTION', ''),
                'fields': self._parse_source_fields(source),
                'attributes': self._parse_attributes(source),
            }
        logger.info(f"Parsed {len(sources)} sources")
        return sources
    
    def _parse_targets(self) -> Dict[str, Dict[str, Any]]:
        """Parse all TARGET definitions"""
        targets = {}
        for target in self.root.findall('.//TARGET'):
            target_name = target.get('NAME', '')
            targets[target_name] = {
                'name': target_name,
                'businessname': target.get('BUSINESSNAME', ''),
                'databasetype': target.get('DATABASETYPE', ''),
                'description': target.get('DESCRIPTION', ''),
                'fields': self._parse_target_fields(target),
                'attributes': self._parse_attributes(target),
            }
        logger.info(f"Parsed {len(targets)} targets")
        return targets
    
    def _parse_source_fields(self, source_elem) -> List[Dict[str, Any]]:
        """Parse SOURCEFIELD elements"""
        fields = []
        for field in source_elem.findall('SOURCEFIELD'):
            fields.append({
                'name': field.get('NAME', ''),
                'datatype': field.get('DATATYPE', ''),
                'fieldnumber': field.get('FIELDNUMBER', ''),
                'length': field.get('LENGTH', ''),
                'precision': field.get('PRECISION', ''),
                'scale': field.get('SCALE', ''),
                'keytype': field.get('KEYTYPE', 'NOT A KEY'),
                'nullable': field.get('NULLABLE', ''),
                'description': field.get('DESCRIPTION', ''),
            })
        return fields
    
    def _parse_target_fields(self, target_elem) -> List[Dict[str, Any]]:
        """Parse TARGETFIELD elements"""
        fields = []
        for field in target_elem.findall('TARGETFIELD'):
            fields.append({
                'name': field.get('NAME', ''),
                'datatype': field.get('DATATYPE', ''),
                'fieldnumber': field.get('FIELDNUMBER', ''),
                'precision': field.get('PRECISION', ''),
                'scale': field.get('SCALE', ''),
                'keytype': field.get('KEYTYPE', 'NOT A KEY'),
                'nullable': field.get('NULLABLE', ''),
                'description': field.get('DESCRIPTION', ''),
            })
        return fields
    
    def _parse_attributes(self, elem) -> Dict[str, str]:
        """Parse TABLEATTRIBUTE elements"""
        attributes = {}
        for attr in elem.findall('TABLEATTRIBUTE'):
            attributes[attr.get('NAME', '')] = attr.get('VALUE', '')
        return attributes
    
    def _parse_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Parse MAPPING elements"""
        mappings = {}
        for mapping in self.root.findall('.//MAPPING'):
            mapping_name = mapping.get('NAME', '')
            mappings[mapping_name] = {
                'name': mapping_name,
                'description': mapping.get('DESCRIPTION', ''),
                'isvalid': mapping.get('ISVALID', ''),
                'transformations': self._parse_transformations(mapping),
                'instances': self._parse_instances(mapping),
                'connectors': self._parse_connectors(mapping),
                'targetloadorder': self._parse_target_load_order(mapping),
                'variables': self._parse_mapping_variables(mapping),
            }
        logger.info(f"Parsed {len(mappings)} mappings with connections")
        return mappings
    
    def _parse_transformations(self, mapping_elem) -> Dict[str, Dict[str, Any]]:
        """Parse TRANSFORMATION elements within a mapping"""
        transformations = {}
        for trans in mapping_elem.findall('TRANSFORMATION'):
            trans_name = trans.get('NAME', '')
            trans_type = trans.get('TYPE', '')
            transformations[trans_name] = {
                'name': trans_name,
                'type': trans_type,
                'description': trans.get('DESCRIPTION', ''),
                'reusable': trans.get('REUSABLE', 'NO'),
                'fields': self._parse_transform_fields(trans),
                'attributes': self._parse_attributes(trans),
                'sql_query': self._extract_sql_query(trans),
                'expression_logic': self._extract_expression_logic(trans),
                'filter_condition': self._extract_filter_condition(trans),
            }
        return transformations
    
    def _parse_transformations_global(self) -> Dict[str, Dict[str, Any]]:
        """Parse all TRANSFORMATION elements globally (for reusable ones)"""
        transformations = {}
        # Note: In the structure, reusable transformations are defined within FOLDER or at root
        for trans in self.root.findall('.//TRANSFORMATION[@REUSABLE="YES"]'):
            trans_name = trans.get('NAME', '')
            transformations[trans_name] = {
                'name': trans_name,
                'type': trans.get('TYPE', ''),
                'description': trans.get('DESCRIPTION', ''),
                'reusable': True,
                'fields': self._parse_transform_fields(trans),
                'attributes': self._parse_attributes(trans),
            }
        return transformations
    
    def _parse_transform_fields(self, trans_elem) -> List[Dict[str, Any]]:
        """Parse TRANSFORMFIELD elements"""
        fields = []
        for field in trans_elem.findall('TRANSFORMFIELD'):
            fields.append({
                'name': field.get('NAME', ''),
                'datatype': field.get('DATATYPE', ''),
                'porttype': field.get('PORTTYPE', ''),
                'precision': field.get('PRECISION', ''),
                'scale': field.get('SCALE', ''),
                'expression': field.get('EXPRESSION', ''),
                'expressiontype': field.get('EXPRESSIONTYPE', ''),
                'defaultvalue': field.get('DEFAULTVALUE', ''),
                'description': field.get('DESCRIPTION', ''),
            })
        return fields
    
    def _parse_instances(self, mapping_elem) -> List[Dict[str, Any]]:
        """Parse INSTANCE elements (transformation/source/target instances)"""
        instances = []
        for instance in mapping_elem.findall('INSTANCE'):
            inst_data = {
                'name': instance.get('NAME', ''),
                'type': instance.get('TYPE', ''),
                'transformation_name': instance.get('TRANSFORMATION_NAME', ''),
                'transformation_type': instance.get('TRANSFORMATION_TYPE', ''),
                'description': instance.get('DESCRIPTION', ''),
                'reusable': instance.get('REUSABLE', 'NO'),
                'dbdname': instance.get('DBDNAME', ''),
                'associated_sources': [
                    src.get('NAME', '') for src in instance.findall('ASSOCIATED_SOURCE_INSTANCE')
                ],
            }
            instances.append(inst_data)
        return instances
    
    def _parse_connectors(self, mapping_elem) -> List[Dict[str, Any]]:
        """Parse CONNECTOR elements (data flow connections)"""
        connectors = []
        for connector in mapping_elem.findall('CONNECTOR'):
            connectors.append({
                'fromfield': connector.get('FROMFIELD', ''),
                'frominstance': connector.get('FROMINSTANCE', ''),
                'frominstancetype': connector.get('FROMINSTANCETYPE', ''),
                'tofield': connector.get('TOFIELD', ''),
                'toinstance': connector.get('TOINSTANCE', ''),
                'toinstancetype': connector.get('TOINSTANCETYPE', ''),
            })
        return connectors
    
    def _parse_target_load_order(self, mapping_elem) -> List[Dict[str, Any]]:
        """Parse TARGETLOADORDER elements"""
        order = []
        for target_order in mapping_elem.findall('TARGETLOADORDER'):
            order.append({
                'order': target_order.get('ORDER', ''),
                'targetinstance': target_order.get('TARGETINSTANCE', ''),
            })
        return order
    
    def _parse_mapping_variables(self, mapping_elem) -> Dict[str, Dict[str, Any]]:
        """Parse MAPPINGVARIABLE elements"""
        variables = {}
        for var in mapping_elem.findall('MAPPINGVARIABLE'):
            var_name = var.get('NAME', '')
            variables[var_name] = {
                'name': var_name,
                'datatype': var.get('DATATYPE', ''),
                'defaultvalue': var.get('DEFAULTVALUE', ''),
                'description': var.get('DESCRIPTION', ''),
                'userdefined': var.get('USERDEFINED', 'NO'),
            }
        return variables
    
    def _parse_maplets(self) -> Dict[str, Dict[str, Any]]:
        """Parse MAPLET elements"""
        maplets = {}
        for maplet in self.root.findall('.//MAPLET'):
            maplet_name = maplet.get('NAME', '')
            maplets[maplet_name] = {
                'name': maplet_name,
                'description': maplet.get('DESCRIPTION', ''),
                'transformations': self._parse_transformations(maplet),
                'instances': self._parse_instances(maplet),
                'connectors': self._parse_connectors(maplet),
            }
        logger.info(f"Parsed {len(maplets)} maplets")
        return maplets
    
    def _extract_sql_query(self, trans_elem) -> Optional[str]:
        """Extract SQL query from Source Qualifier"""
        sql_attr = trans_elem.find('.//TABLEATTRIBUTE[@NAME="Sql Query"]')
        if sql_attr is not None:
            return sql_attr.get('VALUE', '')
        return None
    
    def _extract_filter_condition(self, trans_elem) -> Optional[str]:
        """Extract filter condition from Filter transformation"""
        filter_attr = trans_elem.find('.//TABLEATTRIBUTE[@NAME="Filter Condition"]')
        if filter_attr is not None:
            return filter_attr.get('VALUE', '')
        return None
    
    def _extract_expression_logic(self, trans_elem) -> List[Dict[str, str]]:
        """Extract expression logic from Expression fields"""
        expressions = []
        for field in trans_elem.findall('.//TRANSFORMFIELD'):
            expr = field.get('EXPRESSION', '')
            if expr:
                expressions.append({
                    'field': field.get('NAME', ''),
                    'expression': expr,
                    'type': field.get('EXPRESSIONTYPE', ''),
                })
        return expressions


def parse_informatica_xml(xml_path: str) -> Dict[str, Any]:
    """Convenience function to parse Informatica XML"""
    parser = InformaticaXMLParser(xml_path)
    return parser.parse()
