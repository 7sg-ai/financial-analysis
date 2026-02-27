"""
LLM-powered query generation module using an OpenAI-compatible endpoint.
Converts natural language questions into SQL queries for DuckDB.
"""
from typing import Optional, Dict, List, Any
from openai import OpenAI
import json
import logging
import re
from tenacity import retry, stop_after_attempt, wait_exponential
from schemas import get_schema_context

logger = logging.getLogger(__name__)


def _build_cf_headers(
    client_id: Optional[str], client_secret: Optional[str]
) -> Dict[str, str]:
    """Build Cloudflare Access headers if credentials are provided."""
    headers: Dict[str, str] = {}
    if client_id and client_secret:
        headers["CF-Access-Client-Id"] = client_id
        headers["CF-Access-Client-Secret"] = client_secret
        logger.info("Cloudflare Access headers configured")
    return headers


class QueryGenerator:
    """
    Generates SQL queries from natural language using an OpenAI-compatible LLM.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        model_name: str = "qwen",
        cf_access_client_id: Optional[str] = None,
        cf_access_client_secret: Optional[str] = None,
    ):
        extra_headers = _build_cf_headers(cf_access_client_id, cf_access_client_secret)
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key or "not-needed",
            default_headers=extra_headers,
        )
        self.model_name = model_name
        self.schema_context = get_schema_context()
        logger.info("QueryGenerator initialized (model=%s, base_url=%s)", model_name, base_url)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def generate_query(
        self,
        user_question: str,
        include_explanation: bool = True,
        max_rows: Optional[int] = 1000,
    ) -> Dict[str, Any]:
        """
        Generate a SQL query from a natural language question.

        Returns:
            Dictionary with query, explanation, query_type, tables_used, is_financial
        """
        logger.info("Generating query for question: %s", user_question)

        system_prompt = self._build_system_prompt(max_rows)
        user_prompt = self._build_user_prompt(user_question, include_explanation)

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=2000,
            )

            result_text = response.choices[0].message.content
            result_text = self._extract_json(result_text)
            result = json.loads(result_text)

            logger.info("Successfully generated query: %s...", result.get("query", "")[:100])
            return result

        except json.JSONDecodeError as e:
            result_text_safe = result_text if "result_text" in dir() else "N/A"
            logger.error("Failed to parse JSON response: %s", e)
            raise ValueError(
                f"Model did not return valid JSON. Response: "
                f"{result_text_safe[:200] if result_text_safe != 'N/A' else 'N/A'}..."
            )
        except Exception as e:
            logger.error("Error generating query: %s", e)
            raise

    def validate_query(self, query: str) -> Dict[str, Any]:
        """Validate a SQL query for safety and correctness."""
        issues: List[str] = []
        warnings: List[str] = []

        query_upper = query.upper()

        dangerous_patterns = [
            (r"\bDROP\b", "DROP"),
            (r"\bDELETE\b", "DELETE"),
            (r"\bTRUNCATE\b", "TRUNCATE"),
            (r"\bALTER\b", "ALTER"),
            (r"\bCREATE\b", "CREATE"),
            (r"\bINSERT\b", "INSERT"),
            (r"\bUPDATE\b", "UPDATE"),
        ]
        for pattern, keyword in dangerous_patterns:
            if re.search(pattern, query_upper):
                issues.append(f"Query contains potentially dangerous keyword: {keyword}")

        if "SELECT *" in query_upper and "LIMIT" not in query_upper:
            warnings.append("Query uses SELECT * without LIMIT - may return large results")

        known_tables = ["yellow_taxi", "green_taxi", "fhv", "fhvhv", "taxi_zones"]
        references_known_table = any(table in query.lower() for table in known_tables)

        if not references_known_table:
            issues.append("Query does not reference any known tables")

        return {"is_valid": len(issues) == 0, "issues": issues, "warnings": warnings}

    def refine_query(
        self,
        original_question: str,
        original_query: str,
        user_feedback: str,
    ) -> Dict[str, Any]:
        """Refine a query based on user feedback."""
        logger.info("Refining query based on feedback: %s", user_feedback)

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
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=2000,
            )

            result_text = response.choices[0].message.content
            result_text = self._extract_json(result_text)
            result = json.loads(result_text)

            logger.info("Successfully refined query")
            return result

        except json.JSONDecodeError as e:
            result_text_safe = result_text if "result_text" in dir() else "N/A"
            logger.error("Failed to parse JSON response: %s", e)
            raise ValueError(
                f"Model did not return valid JSON. Response: "
                f"{result_text_safe[:200] if result_text_safe != 'N/A' else 'N/A'}..."
            )
        except Exception as e:
            logger.error("Error refining query: %s", e)
            raise

    def suggest_related_queries(
        self,
        user_question: str,
        num_suggestions: int = 3,
    ) -> List[str]:
        """Suggest related questions the user might be interested in."""
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
                model=self.model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analysis assistant specializing in NYC taxi and ride-share data.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=500,
            )

            result_text = response.choices[0].message.content
            result_text = self._extract_json(result_text)
            result = json.loads(result_text)
            return result.get("suggestions", [])

        except json.JSONDecodeError:
            logger.error("Failed to parse JSON response for suggestions")
            return []
        except Exception as e:
            logger.error("Error generating suggestions: %s", e)
            return []

    def _extract_json(self, text: str) -> str:
        """Strip markdown code fences from LLM output."""
        text = text.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        return text.strip()

    def _build_system_prompt(self, max_rows: Optional[int] = None) -> str:
        """Build the system prompt for query generation."""
        limit_guidance = ""
        if max_rows:
            limit_guidance = f"\n- Add 'LIMIT {max_rows}' to queries that might return many rows"

        return f"""You are an expert SQL query generator specializing in financial analysis of NYC taxi and for-hire vehicle data.

{self.schema_context}

## Query Generation Guidelines:

1. **Table Names**: Use the exact table/view names: yellow_taxi, green_taxi, fhv, fhvhv, taxi_zones
2. **Column Names**: Use exact column names from the schema above (case-sensitive)
3. **Joins**: When analyzing by location, join with taxi_zones using PULocationID or DOLocationID
4. **Financial Analysis**:
   - For revenue calculations, use total_amount (yellow/green), base_passenger_fare + tips (fhvhv)
   - Consider all fee components: tolls, taxes, surcharges, tips
   - Calculate profit margins: compare passenger fares to driver_pay (fhvhv only)
5. **Date Handling**:
   - Use appropriate datetime columns for each table type
   - Extract month/day/hour using SQL functions: month(), dayofweek(), hour()
6. **Aggregations**: Use appropriate aggregate functions: SUM, AVG, COUNT, MIN, MAX
7. **Performance**:
   - Use WHERE clauses to filter data when possible
   - Avoid SELECT * in production queries{limit_guidance}
8. **Safety**: Never generate queries with DROP, DELETE, INSERT, UPDATE, or other modifying operations

## Response Format:
Return ONLY valid JSON with this exact structure:
{{
    "query": "complete SQL query here",
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
        include_explanation: bool,
    ) -> str:
        """Build the user prompt for query generation."""
        explanation_note = ""
        if include_explanation:
            explanation_note = "\nInclude a clear explanation of the query logic."

        return f"""Generate a SQL query to answer this question:

"{user_question}"
{explanation_note}

Remember to return your response as valid JSON following the specified format.
"""


class NarrativeGenerator:
    """Generates narrative explanations of query results."""

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        model_name: str = "qwen",
        cf_access_client_id: Optional[str] = None,
        cf_access_client_secret: Optional[str] = None,
    ):
        extra_headers = _build_cf_headers(cf_access_client_id, cf_access_client_secret)
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key or "not-needed",
            default_headers=extra_headers,
        )
        self.model_name = model_name
        logger.info("NarrativeGenerator initialized (model=%s)", model_name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def generate_narrative(
        self,
        user_question: str,
        query_results: List[Dict[str, Any]],
        query_used: str,
        max_length: str = "medium",
    ) -> str:
        """Generate a narrative explanation of query results."""
        logger.info("Generating narrative for %d result rows", len(query_results))

        results_preview = query_results[:20] if len(query_results) > 20 else query_results

        length_guidance = {
            "short": "2-3 sentences",
            "medium": "1-2 paragraphs",
            "long": "2-3 paragraphs with detailed analysis",
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
                model=self.model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analyst specializing in transportation and financial analysis.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=1000,
            )

            narrative = response.choices[0].message.content
            logger.info("Successfully generated narrative")
            return narrative

        except Exception as e:
            logger.error("Error generating narrative: %s", e)
            raise
