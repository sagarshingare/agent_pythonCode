"""
Expression transformer - handles Informatica expressions to PySpark conversion
"""
from typing import Dict, Optional
import re
from utils.logger import get_logger


logger = get_logger(__name__)


class ExpressionTransformer:
    """Convert Informatica expressions to PySpark expressions"""
    
    def __init__(self):
        """Initialize transformer"""
        self.variables = {}
    
    def transform(self, expression: str) -> str:
        """Transform Informatica expression to PySpark"""
        if not expression:
            return expression
        
        # Unescape HTML entities
        expr = self._unescape_html(expression)
        
        # Apply simple replacements
        expr = self._simple_replacements(expr)
        
        return expr
    
    def _unescape_html(self, text: str) -> str:
        """Unescape HTML entities"""
        replacements = {
            '&lt;': '<',
            '&gt;': '>',
            '&amp;': '&',
            '&quot;': '"',
            '&#xD;': '\r',
            '&#xA;': '\n',
            '&#x9;': '\t',
        }
        for entity, char in replacements.items():
            text = text.replace(entity, char)
        return text
    
    def _simple_replacements(self, expr: str) -> str:
        """Apply simple replacements to avoid regex issues"""
        # Logical operators (case-insensitive)
        expr = re.sub(r'\bAND\b', 'and', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bOR\b', 'or', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bNOT\b', 'not', expr, flags=re.IGNORECASE)
        
        # Comparison operators
        expr = expr.replace('<>', '!=')
        
        # Function replacements (simple case-insensitive)
        expr = re.sub(r'\bTO_CHAR\b', 'cast_str', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bTO_DATE\b', 'to_date', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bUPPER\b', 'upper', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bLOWER\b', 'lower', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bTRIM\b', 'trim', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSUBSTR\b', 'substring', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bLENGTH\b', 'length', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bROUND\b', 'round', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bABS\b', 'abs', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSIGN\b', 'signum', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bCOUNT\b', 'count', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSUM\b', 'sum', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bAVG\b', 'avg', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMIN\b', 'min', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMAX\b', 'max', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bCONCAT\b', 'concat', expr, flags=re.IGNORECASE)
        
        # Special functions
        expr = re.sub(r'\bNVL\b', 'coalesce', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bIIF\b', 'when', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bDECODE\b', 'case_when', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bIS_NUMBER\b', 'is_number', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bIS_NULL\b', 'isnull', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bIS_NOT_NULL\b', 'isnotnull', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSESSTARTTIME\b', 'current_timestamp', expr, flags=re.IGNORECASE)
        
        return expr
    
    def _convert_iif(self, expr: str) -> str:
        """Convert IIF(condition, true_val, false_val) to when()"""
        # This is a simplified version - a full parser would be needed for complex expressions
        pattern = r'IIF\s*\('
        
        def replace_iif(match):
            # Basic replacement - for complex nested IIFs, would need full parser
            return expr.replace('IIF(', 'F.when(').replace(')', ').otherwise(', 1)
        
        return re.sub(pattern, replace_iif, expr, flags=re.IGNORECASE)
    
    def _convert_nvl(self, expr: str) -> str:
        """Convert NVL(col, default) to coalesce()"""
        pattern = r'NVL\\s*\\((.*?)\\s*,\\s*(.*?)\\)'
        
        def replace_nvl(match):
            col = match.group(1).strip()
            default = match.group(2).strip()
            return f'F.coalesce({col}, {default})'
        
        return re.sub(pattern, replace_nvl, expr, flags=re.IGNORECASE)
    
    def _convert_decode(self, expr: str) -> str:
        """Convert DECODE(expr, val1, result1, ...) to case when"""
        # Simplified conversion
        pattern = r'DECODE\\s*\\((.*?)\\)'
        return re.sub(pattern, lambda m: f'F.when({m.group(1)})', expr, flags=re.IGNORECASE)
    
    def _convert_to_char(self, expr: str) -> str:
        """Convert TO_CHAR() to cast()"""
        pattern = r'TO_CHAR\\s*\\((.*?)\\)'
        
        def replace_to_char(match):
            col = match.group(1).strip()
            return f'F.cast({col}, StringType())'
        
        return re.sub(pattern, replace_to_char, expr, flags=re.IGNORECASE)
    
    def _convert_to_date(self, expr: str) -> str:
        """Convert TO_DATE() to to_date()"""
        pattern = r'TO_DATE\\s*\\((.*?)\\)'
        
        def replace_to_date(match):
            args = match.group(1).strip()
            return f'F.to_date({args})'
        
        return re.sub(pattern, replace_to_date, expr, flags=re.IGNORECASE)
    
    def _convert_to_timestamp(self, expr: str) -> str:
        """Convert TO_TIMESTAMP() to to_timestamp()"""
        pattern = r'TO_TIMESTAMP\\s*\\((.*?)\\)'
        
        def replace_to_timestamp(match):
            args = match.group(1).strip()
            return f'F.to_timestamp({args})'
        
        return re.sub(pattern, replace_to_timestamp, expr, flags=re.IGNORECASE)
    
    def _convert_lpad(self, expr: str) -> str:
        """Convert LPAD() to lpad()"""
        pattern = r'LPAD\\s*\\((.*?)\\)'
        
        def replace_lpad(match):
            args = match.group(1).strip()
            return f'F.lpad({args})'
        
        return re.sub(pattern, replace_lpad, expr, flags=re.IGNORECASE)
    
    def _convert_rpad(self, expr: str) -> str:
        """Convert RPAD() to rpad()"""
        pattern = r'RPAD\\s*\\((.*?)\\)'
        
        def replace_rpad(match):
            args = match.group(1).strip()
            return f'F.rpad({args})'
        
        return re.sub(pattern, replace_rpad, expr, flags=re.IGNORECASE)
    
    def _convert_substr(self, expr: str) -> str:
        """Convert SUBSTR() to substring()"""
        pattern = r'SUBSTR\\s*\\((.*?)\\)'
        
        def replace_substr(match):
            args = match.group(1).strip()
            return f'F.substring({args})'
        
        return re.sub(pattern, replace_substr, expr, flags=re.IGNORECASE)
    
    def _convert_length(self, expr: str) -> str:
        """Convert LENGTH() to length()"""
        pattern = r'LENGTH\\s*\\((.*?)\\)'
        
        def replace_length(match):
            col = match.group(1).strip()
            return f'F.length({col})'
        
        return re.sub(pattern, replace_length, expr, flags=re.IGNORECASE)
    
    def _convert_upper(self, expr: str) -> str:
        """Convert UPPER() to upper()"""
        pattern = r'UPPER\\s*\\((.*?)\\)'
        
        def replace_upper(match):
            col = match.group(1).strip()
            return f'F.upper({col})'
        
        return re.sub(pattern, replace_upper, expr, flags=re.IGNORECASE)
    
    def _convert_lower(self, expr: str) -> str:
        """Convert LOWER() to lower()"""
        pattern = r'LOWER\\s*\\((.*?)\\)'
        
        def replace_lower(match):
            col = match.group(1).strip()
            return f'F.lower({col})'
        
        return re.sub(pattern, replace_lower, expr, flags=re.IGNORECASE)
    
    def _convert_trim(self, expr: str) -> str:
        """Convert TRIM() to trim()"""
        pattern = r'TRIM\\s*\\((.*?)\\)'
        
        def replace_trim(match):
            col = match.group(1).strip()
            return f'F.trim({col})'
        
        return re.sub(pattern, replace_trim, expr, flags=re.IGNORECASE)
    
    def _convert_round(self, expr: str) -> str:
        """Convert ROUND() to round()"""
        pattern = r'ROUND\\s*\\((.*?)\\)'
        
        def replace_round(match):
            args = match.group(1).strip()
            return f'F.round({args})'
        
        return re.sub(pattern, replace_round, expr, flags=re.IGNORECASE)
    
    def _convert_trunc(self, expr: str) -> str:
        """Convert TRUNC() to floor()"""
        pattern = r'TRUNC\\s*\\((.*?)\\)'
        
        def replace_trunc(match):
            col = match.group(1).strip()
            return f'F.floor({col})'
        
        return re.sub(pattern, replace_trunc, expr, flags=re.IGNORECASE)
    
    def _convert_abs(self, expr: str) -> str:
        """Convert ABS() to abs()"""
        pattern = r'ABS\\s*\\((.*?)\\)'
        
        def replace_abs(match):
            col = match.group(1).strip()
            return f'F.abs({col})'
        
        return re.sub(pattern, replace_abs, expr, flags=re.IGNORECASE)
    
    def _convert_sign(self, expr: str) -> str:
        """Convert SIGN() to signum()"""
        pattern = r'SIGN\\s*\\((.*?)\\)'
        
        def replace_sign(match):
            col = match.group(1).strip()
            return f'F.signum({col})'
        
        return re.sub(pattern, replace_sign, expr, flags=re.IGNORECASE)
    
    def _convert_greatest(self, expr: str) -> str:
        """Convert GREATEST() to greatest()"""
        pattern = r'GREATEST\\s*\\((.*?)\\)'
        
        def replace_greatest(match):
            args = match.group(1).strip()
            return f'F.greatest({args})'
        
        return re.sub(pattern, replace_greatest, expr, flags=re.IGNORECASE)
    
    def _convert_least(self, expr: str) -> str:
        """Convert LEAST() to least()"""
        pattern = r'LEAST\\s*\\((.*?)\\)'
        
        def replace_least(match):
            args = match.group(1).strip()
            return f'F.least({args})'
        
        return re.sub(pattern, replace_least, expr, flags=re.IGNORECASE)
    
    def _convert_concat(self, expr: str) -> str:
        """Convert CONCAT() to concat()"""
        pattern = r'CONCAT\\s*\\((.*?)\\)'
        
        def replace_concat(match):
            args = match.group(1).strip()
            return f'F.concat({args})'
        
        return re.sub(pattern, replace_concat, expr, flags=re.IGNORECASE)
    
    def _convert_is_number(self, expr: str) -> str:
        """Convert IS_NUMBER() to regex check"""
        pattern = r'IS_NUMBER\\s*\\((.*?)\\)'
        
        def replace_is_number(match):
            col = match.group(1).strip()
            return f'F.regexp_like({col}, r"[0-9]+")'
        
        return re.sub(pattern, replace_is_number, expr, flags=re.IGNORECASE)
    
    def _convert_is_null(self, expr: str) -> str:
        """Convert IS_NULL() to isnull()"""
        pattern = r'IS_NULL\\s*\\((.*?)\\)'
        
        def replace_is_null(match):
            col = match.group(1).strip()
            return f'F.isnull({col})'
        
        return re.sub(pattern, replace_is_null, expr, flags=re.IGNORECASE)
    
    def _convert_is_not_null(self, expr: str) -> str:
        """Convert IS_NOT_NULL() to isnotnull()"""
        pattern = r'IS_NOT_NULL\\s*\\((.*?)\\)'
        
        def replace_is_not_null(match):
            col = match.group(1).strip()
            return f'F.isnotnull({col})'
        
        return re.sub(pattern, replace_is_not_null, expr, flags=re.IGNORECASE)
    
    def _convert_sessstarttime(self, expr: str) -> str:
        """Convert SESSSTARTTIME to current_timestamp()"""
        return expr.replace('SESSSTARTTIME', 'F.current_timestamp()')
    
    def _convert_setvariable(self, expr: str) -> str:
        """Handle SETVARIABLE - store in memory"""
        # This requires special handling - will be processed separately
        return f'# VARIABLE_SET: {expr}'
