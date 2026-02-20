"""
LLM-powered query generation module using Crusoe Cloud
Converts natural language questions into Spark SQL queries
"""
from typing import Optional, Dict, List, Any, Literal
from openai import OpenAI
import json
import logging
import re
from tenacity import retry, stop_after_attempt, wait_exponential
from schemas import get_schema_context

logger = logging.getLogger(__name__)


class QueryGenerator:
    """
    Generates Spark SQL queries from natural language using Crusoe Cloud
    """
    
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        deployment_name: str = "gpt-5.2-chat",
        api_version: str = "2024-12-01-preview"
    ):
        """
        Initialize QueryGenerator
        
        Args:
            endpoint: Crusoe Cloud inference endpoint URL (not used, kept for compatibility)
            api_key: Crusoe Cloud API key
            deployment_name: Deployment name for the model (kept for compatibility)
            api_version: API version (kept for compatibility, not used)
        """
        # Note: endpoint and api_version are kept for compatibility but not used
        # Crusoe uses a fixed endpoint and does not require version specification
        
        logger.info("Using Crusoe Cloud inference endpoint")
        logger.info(f"Deployment name: {deployment_name}")
        
        self.client = OpenAI(
            base_url="https://inference.api.crusoecloud.com/v1",
            api_key=api_key
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
            # Crusoe uses max_completion_tokens instead of max_tokens
            response = self.client.chat.completions.create(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_completion_tokens=2000  # Limit response length for SQL queries
            )
            
            result_text = response.choices[0].message.content
            
            # Try to extract JSON from the response (might be wrapped in markdown code blocks)
            result_text = result_text.strip()
            if result_text.startswith("