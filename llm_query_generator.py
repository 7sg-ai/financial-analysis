"""
LLM-powered query generation module using AWS Bedrock
Converts natural language questions into Spark SQL queries
"""
from typing import Optional, Dict, List, Any, Literal
import boto3
import json
import logging
import re
from tenacity import retry, stop_after_attempt, wait_exponential
from schemas import get_schema_context

logger = logging.getLogger(__name__)


class QueryGenerator:
    """
    Generates Spark SQL queries from natural language using AWS Bedrock
    """
    
    def __init__(
        self,
        aws_region: str = "us-east-1",
        model_id: str = "meta.llama3-1-8b-instruct-v1:0"
    ):
        """
        Initialize QueryGenerator
        
        Args:
            aws_region: AWS region for Bedrock (e.g., 'us-east-1')
            model_id: Bedrock model ID (e.g., 'meta.llama3-1-8b-instruct-v1:0')
        """
        session = boto3.Session()
        self.client = session.client('bedrock-runtime', region_name=aws_region)
        self.model_id = model_id
        self.schema_context = get_schema_context()
        
        logger.info(f"QueryGenerator initialized with model: {model_id}")
    
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
            response = self.client.converse(
                modelId=self.model_id,
                messages=[
                    {"role": "system", "content": [{"text": system_prompt}]},
                    {"role": "user", "content": [{"text": user_prompt}]}
                ],
                inferenceConfig={
                    "maxTokens": 2000,
                    "temperature": 0.7,
                    "topP": 0.9
                }
            )
            
            result_text = response["output"]["message"]["content"][0]["text"]
            
            # Try to extract JSON from the response (might be wrapped in markdown code blocks)
            result_text = result_text.strip()
            if result_text.startswith("
class NarrativeGenerator:
    """
    Generates narrative explanations of query results
    """
    
    def __init__(
        self,
        aws_region: str,
        deployment_name: str = "gpt-5.2-chat",
        bedrock_model_id: str = "meta.llama3-1-8b-instruct-v1:0"
    ):
        """Initialize NarrativeGenerator with AWS Bedrock client"""
        logger.info(f"Using AWS region: {aws_region}")
        logger.info(f"Bedrock model ID: {bedrock_model_id}")
        
        session = boto3.Session()
        self.client = session.client('bedrock-runtime', region_name=aws_region)
        self.bedrock_model_id = bedrock_model_id
        
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