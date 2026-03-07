"""
Response formatting module
Converts query results into various output formats (tabular, narrative, JSON, etc.)
"""
from typing import List, Dict, Any, Optional
import json
from datetime import datetime, date
from decimal import Decimal
from tabulate import tabulate
import logging

logger = logging.getLogger(__name__)


class ResponseFormatter:
    """
    Formats query results for different output types
    """
    
    def __init__(self):
        """Initialize ResponseFormatter"""
        logger.info("ResponseFormatter initialized")
    
    def format_tabular(
        self,
        data: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
        table_format: str = "grid",
        max_col_width: Optional[int] = None
    ) -> str:
        """
        Format results as a text table
        
        Args:
            data: List of row dictionaries
            columns: Column order (if None, uses first row keys)
            table_format: Table style (grid, simple, pretty, html, etc.)
            max_col_width: Maximum column width for truncation
            
        Returns:
            Formatted table string
        """
        if not data:
            return "No results found."
        
        # Determine columns
        if columns is None:
            columns = list(data[0].keys())
        
        # Extract rows in column order
        rows = []
        for row in data:
            formatted_row = []
            for col in columns:
                value = row.get(col)
                formatted_value = self._format_value(value)
                
                # Truncate if needed
                if max_col_width and len(str(formatted_value)) > max_col_width:
                    formatted_value = str(formatted_value)[:max_col_width-3] + "..."
                
                formatted_row.append(formatted_value)
            rows.append(formatted_row)
        
        # Create table
        table = tabulate(rows, headers=columns, tablefmt=table_format)
        
        return table
    
    def format_json(
        self,
        data: List[Dict[str, Any]],
        pretty: bool = True
    ) -> str:
        """
        Format results as JSON
        
        Args:
            data: List of row dictionaries
            pretty: If True, use indentation
            
        Returns:
            JSON string
        """
        # Convert non-serializable types
        serializable_data = [
            {k: self._make_serializable(v) for k, v in row.items()}
            for row in data
        ]
        
        if pretty:
            return json.dumps(serializable_data, indent=2, default=str)
        else:
            return json.dumps(serializable_data, default=str)
    
    def format_csv(
        self,
        data: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
        delimiter: str = ","
    ) -> str:
        """
        Format results as CSV
        
        Args:
            data: List of row dictionaries
            columns: Column order (if None, uses first row keys)
            delimiter: CSV delimiter
            
        Returns:
            CSV string
        """
        if not data:
            return ""
        
        # Determine columns
        if columns is None:
            columns = list(data[0].keys())
        
        # Build CSV
        lines = []
        
        # Header
        lines.append(delimiter.join(columns))
        
        # Rows
        for row in data:
            csv_row = []
            for col in columns:
                value = row.get(col, "")
                # Escape quotes and wrap in quotes if contains delimiter
                str_value = str(self._format_value(value))
                if delimiter in str_value or '"' in str_value or '\n' in str_value:
                    str_value = '"' + str_value.replace('"', '""') + '"'
                csv_row.append(str_value)
            lines.append(delimiter.join(csv_row))
        
        return "\n".join(lines)
    
    def format_summary(
        self,
        data: List[Dict[str, Any]],
        key_metrics: Optional[List[str]] = None
    ) -> str:
        """
        Format results as a summary with key metrics highlighted
        
        Args:
            data: List of row dictionaries
            key_metrics: List of column names to highlight
            
        Returns:
            Summary string
        """
        if not data:
            return "No results found."
        
        lines = [
            f"Results Summary",
            f"Total Rows: {len(data)}",
            ""
        ]
        
        # If single row with aggregations, show as key-value pairs
        if len(data) == 1:
            lines.append("Metrics:")
            for key, value in data[0].items():
                formatted_value = self._format_value(value)
                lines.append(f"  {key}: {formatted_value}")
        else:
            # Show first few rows and summary stats
            if key_metrics:
                lines.append("Key Metrics Across All Rows:")
                for metric in key_metrics:
                    values = [row.get(metric) for row in data if metric in row]
                    numeric_values = [v for v in values if isinstance(v, (int, float, Decimal))]
                    
                    if numeric_values:
                        lines.append(f"  {metric}:")
                        lines.append(f"    Total: {self._format_value(sum(numeric_values))}")
                        lines.append(f"    Average: {self._format_value(sum(numeric_values) / len(numeric_values))}")
                        lines.append(f"    Min: {self._format_value(min(numeric_values))}")
                        lines.append(f"    Max: {self._format_value(max(numeric_values))}")
                lines.append("")
            
            # Show sample rows
            lines.append(f"Sample Rows (showing first {min(5, len(data))}):")
            lines.append(self.format_tabular(data[:5]))
        
        return "\n".join(lines)
    
    def format_markdown(
        self,
        data: List[Dict[str, Any]],
        title: Optional[str] = None
    ) -> str:
        """
        Format results as Markdown
        
        Args:
            data: List of row dictionaries
            title: Optional title for the table
            
        Returns:
            Markdown string
        """
        lines = []
        
        if title:
            lines.append(f"## {title}")
            lines.append("")
        
        if not data:
            lines.append("*No results found.*")
            return "\n".join(lines)
        
        # Add row count
        lines.append(f"*{len(data)} rows returned*")
        lines.append("")
        
        # Create markdown table
        table = self.format_tabular(data, table_format="github")
        lines.append(table)
        
        return "\n".join(lines)
    
    def format_html(
        self,
        data: List[Dict[str, Any]],
        title: Optional[str] = None,
        include_css: bool = True
    ) -> str:
        """
        Format results as HTML table
        
        Args:
            data: List of row dictionaries
            title: Optional title
            include_css: If True, include basic CSS styling
            
        Returns:
            HTML string
        """
        html_parts = []
        
        if include_css:
            html_parts.append("""
<style>
    .results-table {
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    .results-table th {
        background-color: #4CAF50;
        color: white;
        padding: 12px;
        text-align: left;
        border: 1px solid #ddd;
    }
    .results-table td {
        padding: 10px;
        border: 1px solid #ddd;
    }
    .results-table tr:nth-child(even) {
        background-color: #f2f2f2;
    }
    .results-table tr:hover {
        background-color: #ddd;
    }
</style>
""")
        
        if title:
            html_parts.append(f"<h2>{title}</h2>")
        
        if not data:
            html_parts.append("<p><em>No results found.</em></p>")
            return "\n".join(html_parts)
        
        html_parts.append(f"<p><em>{len(data)} rows returned</em></p>")
        
        # Use tabulate to generate HTML table
        table = tabulate(
            [[self._format_value(row.get(col)) for col in data[0].keys()] for row in data],
            headers=list(data[0].keys()),
            tablefmt="html"
        )
        
        # Add CSS class
        table = table.replace('<table>', '<table class="results-table">')
        
        html_parts.append(table)
        
        return "\n".join(html_parts)
    
    def _format_value(self, value: Any) -> str:
        """Format a single value for display"""
        if value is None:
            return "NULL"
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return f"{float(value):.2f}"
        elif isinstance(value, float):
            return f"{value:.2f}"
        elif isinstance(value, bool):
            return "true" if value else "false"
        else:
            return str(value)
    
    def _make_serializable(self, value: Any) -> Any:
        """Convert value to JSON-serializable type"""
        if value is None:
            return None
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='ignore')
        else:
            return value


class AnalysisResponse:
    """
    Complete response structure for analysis queries
    """
    
    def __init__(
        self,
        question: str,
        query: str,
        results: List[Dict[str, Any]],
        narrative: Optional[str] = None,
        execution_time_ms: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize AnalysisResponse
        
        Args:
            question: Original user question
            query: SQL query executed
            results: Query results
            narrative: Optional narrative explanation
            execution_time_ms: Query execution time
            metadata: Additional metadata
        """
        self.question = question
        self.query = query
        self.results = results
        self.narrative = narrative
        self.execution_time_ms = execution_time_ms
        self.metadata = metadata or {}
        self.timestamp = datetime.now()
        
        self.formatter = ResponseFormatter()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'question': self.question,
            'query': self.query,
            'results': self.results,
            'result_count': len(self.results),
            'narrative': self.narrative,
            'execution_time_ms': self.execution_time_ms,
            'metadata': self.metadata
        }
    
    def to_json(self, pretty: bool = True) -> str:
        """Convert to JSON string"""
        data = self.to_dict()
        if pretty:
            return json.dumps(data, indent=2, default=str)
        else:
            return json.dumps(data, default=str)
    
    def to_tabular(self, format: str = "grid") -> str:
        """Get tabular representation of results"""
        return self.formatter.format_tabular(self.results, table_format=format)
    
    def to_markdown(self) -> str:
        """Get markdown representation"""
        lines = [
            f"# Analysis Results",
            f"",
            f"**Question:** {self.question}",
            f"",
            f"**Query:**",
            f"```sql",
            self.query,
            f"```",
            f""
        ]
        
        if self.narrative:
            lines.extend([
                f"## Summary",
                f"",
                self.narrative,
                f""
            ])
        
        lines.extend([
            f"## Results",
            f"",
            self.formatter.format_markdown(self.results),
            f"",
            f"*Query executed in {self.execution_time_ms}ms*"
        ])
        
        return "\n".join(lines)
    
    def to_html(self) -> str:
        """Get HTML representation"""
        html_parts = [
            "<div class='analysis-response'>",
            f"<h1>Analysis Results</h1>",
            f"<p><strong>Question:</strong> {self.question}</p>",
            f"<p><strong>Query:</strong></p>",
            f"<pre><code>{self.query}</code></pre>"
        ]
        
        if self.narrative:
            html_parts.extend([
                "<h2>Summary</h2>",
                f"<p>{self.narrative}</p>"
            ])
        
        html_parts.extend([
            "<h2>Results</h2>",
            self.formatter.format_html(self.results, include_css=False),
            f"<p><em>Query executed in {self.execution_time_ms}ms</em></p>",
            "</div>"
        ])
        
        return "\n".join(html_parts)

