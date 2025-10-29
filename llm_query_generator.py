"""
LLM-powered query generation module using Azure OpenAI
Converts natural language questions into Spark SQL queries
"""
from typing import Optional, Dict, List, Any
from openai import AzureOpenAI
import json
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from schemas import get_schema_context

logger = logging.getLogger(__name__)


class QueryGenerator:
    """
    Generates Spark SQL queries from natural language using Azure OpenAI
    """
    
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        deployment_name: str = "gpt-4",
        api_version: str = "2024-02-15-preview"
    ):
        """
        Initialize QueryGenerator
        
        Args:
            endpoint: Azure OpenAI endpoint URL
            api_key: Azure OpenAI API key
            deployment_name: Deployment name for the model
            api_version: API version to use
        """
        self.client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version
        )
        self.deployment_name = deployment_name
        self.schema_context = get_schema_context()
        
        logger.info(f"QueryGenerator initialized with deployment: {deployment_name}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def generate_query(
        self,
        user_question: str,
        include_explanation: bool = True,
        max_rows: Optional[int] = 1000
    ) -> Dict[str, Any]:
        """
        Generate a Spark SQL query from a natural language question
        
        Args:
            user_question: Natural language question from user
            include_explanation: Whether to include query explanation
            max_rows: Maximum number of rows to return (adds LIMIT clause)
            
        Returns:
            Dictionary containing:
                - query: Generated SQL query
                - explanation: Human-readable explanation (if requested)
                - query_type: Type of query (aggregation, filter, join, etc.)
                - tables_used: List of tables referenced in the query
                - is_financial: Whether query involves financial analysis
        """
        logger.info(f"Generating query for question: {user_question}")
        
        system_prompt = self._build_system_prompt(max_rows)
        user_prompt = self._build_user_prompt(user_question, include_explanation)
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for more consistent SQL generation
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            
            logger.info(f"Successfully generated query: {result.get('query', '')[:100]}...")
            return result
            
        except Exception as e:
            logger.error(f"Error generating query: {e}")
            raise
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """
        Validate a SQL query for safety and correctness
        
        Args:
            query: SQL query to validate
            
        Returns:
            Dictionary with validation results:
                - is_valid: Whether query is valid
                - issues: List of validation issues found
                - warnings: List of warnings
        """
        issues = []
        warnings = []
        
        query_upper = query.upper()
        
        # Check for dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                issues.append(f"Query contains potentially dangerous keyword: {keyword}")
        
        # Check for common issues
        if 'SELECT *' in query_upper and 'LIMIT' not in query_upper:
            warnings.append("Query uses SELECT * without LIMIT - may return large results")
        
        # Check if query references known tables
        known_tables = ['yellow_taxi', 'green_taxi', 'fhv', 'fhvhv', 'taxi_zones']
        references_known_table = any(table in query.lower() for table in known_tables)
        
        if not references_known_table:
            issues.append("Query does not reference any known tables")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'warnings': warnings
        }
    
    def refine_query(
        self,
        original_question: str,
        original_query: str,
        user_feedback: str
    ) -> Dict[str, Any]:
        """
        Refine a query based on user feedback
        
        Args:
            original_question: Original user question
            original_query: Previously generated query
            user_feedback: User feedback on the query
            
        Returns:
            Dictionary with refined query and explanation
        """
        logger.info(f"Refining query based on feedback: {user_feedback}")
        
        system_prompt = self._build_system_prompt()
        
        user_prompt = f"""
Original Question: {original_question}

Previous Query:
```sql
{original_query}
```

User Feedback: {user_feedback}

Please generate an improved query that addresses the user's feedback.
Return your response as JSON with the following structure:
{{
    "query": "refined SQL query here",
    "explanation": "explanation of changes made",
    "changes_made": ["list", "of", "specific", "changes"]
}}
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            
            logger.info("Successfully refined query")
            return result
            
        except Exception as e:
            logger.error(f"Error refining query: {e}")
            raise
    
    def suggest_related_queries(
        self,
        user_question: str,
        num_suggestions: int = 3
    ) -> List[str]:
        """
        Suggest related questions that user might be interested in
        
        Args:
            user_question: Original user question
            num_suggestions: Number of suggestions to generate
            
        Returns:
            List of suggested questions
        """
        prompt = f"""
Based on this question about NYC taxi data: "{user_question}"

Suggest {num_suggestions} related questions that would provide additional insights.
Return your response as JSON:
{{
    "suggestions": ["question 1", "question 2", "question 3"]
}}
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are a data analysis assistant specializing in NYC taxi and ride-share data."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            
            return result.get('suggestions', [])
            
        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            return []
    
    def _build_system_prompt(self, max_rows: Optional[int] = None) -> str:
        """Build the system prompt for query generation"""
        
        limit_guidance = ""
        if max_rows:
            limit_guidance = f"\n- Add 'LIMIT {max_rows}' to queries that might return many rows"
        
        return f"""You are an expert SQL query generator specializing in financial analysis of NYC taxi and for-hire vehicle data using Spark SQL.

{self.schema_context}

## Query Generation Guidelines:

1. **Table Names**: Use the exact table/view names: yellow_taxi, green_taxi, fhv, fhvhv, taxi_zones
2. **Column Names**: Use exact column names from the schema above (case-sensitive in Spark SQL)
3. **Joins**: When analyzing by location, join with taxi_zones using PULocationID or DOLocationID
4. **Financial Analysis**: 
   - For revenue calculations, use total_amount (yellow/green), base_passenger_fare + tips (fhvhv)
   - Consider all fee components: tolls, taxes, surcharges, tips
   - Calculate profit margins: compare passenger fares to driver_pay (fhvhv only)
5. **Date Handling**: 
   - Use appropriate datetime columns for each table type
   - Extract month/day/hour using Spark SQL functions: month(), dayofweek(), hour()
6. **Aggregations**: Use appropriate aggregate functions: SUM, AVG, COUNT, MIN, MAX
7. **Performance**: 
   - Use WHERE clauses to filter data when possible
   - Avoid SELECT * in production queries{limit_guidance}
8. **Safety**: Never generate queries with DROP, DELETE, INSERT, UPDATE, or other modifying operations

## Response Format:
Return ONLY valid JSON with this exact structure:
{{
    "query": "complete Spark SQL query here",
    "explanation": "brief explanation of what the query does and how it answers the question",
    "query_type": "aggregation|filter|join|time_series|ranking",
    "tables_used": ["list", "of", "tables"],
    "is_financial": true/false,
    "expected_columns": ["list", "of", "output", "column", "names"]
}}
"""
    
    def _build_user_prompt(
        self,
        user_question: str,
        include_explanation: bool
    ) -> str:
        """Build the user prompt for query generation"""
        
        explanation_note = ""
        if include_explanation:
            explanation_note = "\nInclude a clear explanation of the query logic."
        
        return f"""Generate a Spark SQL query to answer this question:

"{user_question}"
{explanation_note}

Remember to return your response as valid JSON following the specified format.
"""


class NarrativeGenerator:
    """
    Generates narrative explanations of query results
    """
    
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        deployment_name: str = "gpt-4",
        api_version: str = "2024-02-15-preview"
    ):
        """Initialize NarrativeGenerator with Azure OpenAI client"""
        self.client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version
        )
        self.deployment_name = deployment_name
        
        logger.info("NarrativeGenerator initialized")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def generate_narrative(
        self,
        user_question: str,
        query_results: List[Dict[str, Any]],
        query_used: str,
        max_length: str = "medium"
    ) -> str:
        """
        Generate a narrative explanation of query results
        
        Args:
            user_question: Original user question
            query_results: List of result rows (as dictionaries)
            query_used: The SQL query that was executed
            max_length: Desired length ("short", "medium", "long")
            
        Returns:
            Narrative text explaining the results
        """
        logger.info(f"Generating narrative for {len(query_results)} result rows")
        
        # Limit results shown in prompt to avoid token limits
        results_preview = query_results[:20] if len(query_results) > 20 else query_results
        
        length_guidance = {
            "short": "2-3 sentences",
            "medium": "1-2 paragraphs",
            "long": "2-3 paragraphs with detailed analysis"
        }
        
        prompt = f"""You are a financial analyst providing insights on NYC taxi and ride-share data.

User Question: {user_question}

Query Executed:
```sql
{query_used}
```

Results (showing {len(results_preview)} of {len(query_results)} rows):
{json.dumps(results_preview, indent=2, default=str)}

Generate a clear, professional narrative explanation of these results in {length_guidance.get(max_length, 'medium')}.
Include:
1. Direct answer to the user's question
2. Key insights from the data
3. Notable patterns or trends
4. Financial implications if relevant

Write in a professional but accessible tone. Use specific numbers from the results.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": "You are a data analyst specializing in transportation and financial analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1000
            )
            
            narrative = response.choices[0].message.content
            logger.info("Successfully generated narrative")
            return narrative
            
        except Exception as e:
            logger.error(f"Error generating narrative: {e}")
            raise

