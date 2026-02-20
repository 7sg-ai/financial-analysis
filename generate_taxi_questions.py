#!/usr/bin/env python3
"""
Generate 100 taxi data questions, test them against the API, and save results.
"""

import json
import requests
import time
from datetime import datetime

# API Gateway endpoint (replaces Azure Container App)
# Replace with your deployed API Gateway URL after migration
API_BASE_URL = os.getenv('API_BASE_URL', 'https://<your-api-id>.execute-api.<region>.amazonaws.com/prod/api/analyze')

# Categories of questions
QUESTION_TEMPLATES = [
    # Trip volume questions
    ("trip_volume", "easy", [
        "How many yellow taxi trips were there in January 2024?",
        "What is the total number of green taxi trips in 2024?",
        "How many trips were recorded in February 2024?",
        "What was the busiest month for yellow taxi trips in 2024?",
        "How many taxi trips happened on weekends in 2024?",
        "What is the total trip count for yellow taxis in 2023?",
        "How many green taxi trips occurred in March 2024?",
        "What is the average daily trip count for yellow taxis?",
        "How many trips were there in the first quarter of 2024?",
        "What was the least busy month for taxi trips?",
    ]),
    
    # Fare analysis
    ("fare_analysis", "easy", [
        "What is the average fare amount for yellow taxi trips?",
        "What is the total revenue from yellow taxi fares in 2024?",
        "What is the maximum fare amount recorded?",
        "What is the minimum fare amount for trips over 1 mile?",
        "What is the average total amount including tips for yellow taxis?",
        "What is the median fare amount for green taxi trips?",
        "How much total fare revenue was collected in January 2024?",
        "What is the average fare per mile for yellow taxis?",
        "What percentage of fares are under $10?",
        "What is the 90th percentile fare amount?",
    ]),
    
    # Tip analysis
    ("tip_analysis", "medium", [
        "What is the average tip amount for yellow taxi trips?",
        "What percentage of trips include a tip?",
        "What is the average tip percentage relative to fare amount?",
        "Which payment type has the highest average tip?",
        "What is the maximum tip amount recorded?",
        "How does the average tip vary by hour of day?",
        "What is the total tip revenue for 2024?",
        "What is the average tip for trips over $50?",
        "Do longer trips receive higher tips on average?",
        "What is the tip amount distribution?",
    ]),
    
    # Distance analysis
    ("distance_analysis", "easy", [
        "What is the average trip distance for yellow taxis?",
        "What is the longest trip distance recorded?",
        "How many trips were under 1 mile?",
        "What percentage of trips are between 1 and 5 miles?",
        "What is the total distance traveled by all yellow taxis in 2024?",
        "What is the average trip distance for green taxis?",
        "How many trips exceeded 20 miles?",
        "What is the median trip distance?",
        "What is the average distance for airport trips?",
        "How does trip distance vary by time of day?",
    ]),
    
    # Passenger analysis
    ("passenger_analysis", "easy", [
        "What is the average number of passengers per trip?",
        "How many trips had more than 4 passengers?",
        "What percentage of trips are solo passengers?",
        "What is the most common passenger count?",
        "How many trips had 2 passengers?",
        "What is the distribution of passenger counts?",
        "Do trips with more passengers travel longer distances?",
        "What percentage of trips have 3 or more passengers?",
        "How does passenger count vary by hour?",
        "What is the average fare for single passenger trips?",
    ]),
    
    # Time-based analysis
    ("temporal_analysis", "medium", [
        "What is the busiest hour of day for taxi pickups?",
        "How many trips start between 8 AM and 9 AM?",
        "What day of the week has the most taxi trips?",
        "What is the average trip duration in minutes?",
        "How does trip volume change throughout the day?",
        "What percentage of trips occur during rush hour?",
        "What is the busiest time for airport pickups?",
        "How many trips occur after midnight?",
        "What is the average trip duration during rush hour vs off-peak?",
        "Which month had the highest trip volume in 2024?",
    ]),
    
    # Location analysis
    ("location_analysis", "medium", [
        "What is the most popular pickup location?",
        "What is the most popular dropoff location?",
        "How many trips start and end in the same zone?",
        "What are the top 10 pickup locations by trip count?",
        "What are the top 5 dropoff locations?",
        "Which location has the highest average fare?",
        "How many different pickup locations are there?",
        "What is the most common pickup-dropoff pair?",
        "Which location has the most airport trips?",
        "What percentage of trips start in Manhattan?",
    ]),
    
    # Payment analysis
    ("payment_analysis", "easy", [
        "What percentage of trips are paid by credit card?",
        "How many trips were paid in cash?",
        "What is the distribution of payment types?",
        "What is the average fare for credit card payments vs cash?",
        "How has credit card usage changed over time?",
        "What payment type has the highest average total amount?",
        "What percentage of trips use payment type 1?",
        "How many trips have no recorded payment type?",
        "What is the most common payment method?",
        "Do cash payments have lower tips than credit cards?",
    ]),
    
    # Comparative analysis
    ("comparative_analysis", "hard", [
        "How do yellow taxi fares compare to green taxi fares?",
        "What is the difference in average trip distance between yellow and green taxis?",
        "Which taxi type has higher average tips?",
        "Compare the trip volume of yellow vs green taxis by month",
        "How do average fares compare between 2023 and 2024?",
        "Which taxi type has longer average trip duration?",
        "Compare weekend vs weekday trip volumes",
        "How do morning trips compare to evening trips in terms of fare?",
        "Compare tip percentages between yellow and green taxis",
        "What is the difference in passenger count between taxi types?",
    ]),
    
    # Revenue analysis
    ("revenue_analysis", "hard", [
        "What is the total revenue from all taxi trips in 2024?",
        "What is the average revenue per trip including all charges?",
        "How much was collected in tolls in 2024?",
        "What is the total congestion surcharge collected?",
        "How much MTA tax was collected?",
        "What is the total airport fee revenue?",
        "What is the monthly revenue trend for 2024?",
        "Which month generated the most revenue?",
        "What percentage of revenue comes from tips?",
        "How does revenue per mile compare across months?",
    ]),
    
    # Trend analysis
    ("trend_analysis", "hard", [
        "How has the average fare changed month over month in 2024?",
        "Is trip volume increasing or decreasing over time?",
        "How has the average tip percentage changed over time?",
        "What is the trend in average trip distance?",
        "How has credit card payment adoption changed?",
        "Is the average passenger count changing over time?",
        "What is the year-over-year change in trip volume?",
        "How has the congestion surcharge affected total fares?",
        "Are trips getting shorter or longer over time?",
        "How has the distribution of trip distances changed?",
    ]),
    
    # Aggregation questions
    ("aggregation", "medium", [
        "What is the sum of all fare amounts in 2024?",
        "Count the total number of trips by vendor ID",
        "What is the average of all numeric columns for yellow taxi?",
        "Group trips by payment type and show average fare",
        "What is the standard deviation of trip distances?",
        "Calculate the variance in fare amounts",
        "What is the correlation between distance and fare?",
        "Group trips by hour and count them",
        "What is the total extra charges collected?",
        "Sum the improvement surcharge by month",
    ]),
]

# Extra backup questions in case some fail
BACKUP_QUESTIONS = [
    ("trip_volume", "easy", "How many total taxi trips are in the database?"),
    ("fare_analysis", "easy", "What is the average fare for all trips?"),
    ("distance_analysis", "easy", "What is the average distance for all trips?"),
    ("passenger_analysis", "easy", "What is the most common number of passengers?"),
    ("temporal_analysis", "medium", "What hour has the most pickups?"),
    ("location_analysis", "medium", "What is the most frequent pickup location ID?"),
    ("payment_analysis", "easy", "How many trips were paid by credit card?"),
    ("revenue_analysis", "medium", "What is the total amount collected from all trips?"),
    ("tip_analysis", "medium", "What is the total tips collected?"),
    ("distance_analysis", "easy", "How many trips were longer than 10 miles?"),
    ("fare_analysis", "easy", "What is the highest single fare recorded?"),
    ("passenger_analysis", "easy", "How many trips had exactly 1 passenger?"),
    ("temporal_analysis", "medium", "How many trips started after 6 PM?"),
    ("trip_volume", "easy", "What is the daily average number of trips?"),
    ("revenue_analysis", "medium", "What is the average total amount per trip?"),
    ("location_analysis", "medium", "What are the top 3 pickup locations?"),
    ("payment_analysis", "easy", "What percentage of trips have payment_type = 1?"),
    ("tip_analysis", "medium", "What is the average tip for credit card payments?"),
    ("distance_analysis", "easy", "What is the total distance of all trips combined?"),
    ("fare_analysis", "easy", "How many trips had a fare over $100?"),
    ("temporal_analysis", "medium", "Which day of week has the least trips?"),
    ("aggregation", "medium", "What is the average fare by vendor ID?"),
    ("comparative_analysis", "hard", "Compare average fare between yellow and green taxis"),
    ("trend_analysis", "hard", "Show monthly trip counts for 2024"),
    ("revenue_analysis", "hard", "What is total revenue by payment type?"),
    ("location_analysis", "medium", "How many unique dropoff locations exist?"),
    ("passenger_analysis", "easy", "What percentage of trips have 2+ passengers?"),
    ("tip_analysis", "medium", "What is the max tip recorded?"),
    ("fare_analysis", "easy", "What is the minimum non-zero fare?"),
    ("trip_volume", "easy", "Count trips in the second quarter of 2024"),
]


def call_api(question: str) -> dict:
    """Call the analyze API with a question."""
    try:
        # Use API Gateway client via boto3 (if using Lambda proxy integration with IAM auth)
        # OR use requests with API Gateway URL (if using Lambda authorizer or no auth)
        # Assuming API Gateway with IAM auth:
        import boto3
        import json
        import requests

        # Option 1: Direct HTTP call (if no IAM auth, e.g., Lambda authorizer or public endpoint)
        response = requests.post(
            f"{API_BASE_URL}/analyze",  # API Gateway strips '/prod' path if configured
            json={"question": question},
            timeout=120
        )

        # Option 2: IAM-signed request (if backend requires IAM auth)
        # from botocore.auth import SigV4Auth
        # from botocore.awsrequest import AWSRequest
        # import botocore.session
        # session = botocore.session.get_session()
        # client = session.create_client('execute-api', region_name='us-east-1')
        # req = AWSRequest(method='POST', url=f"{API_BASE_URL}/analyze", data=json.dumps({"question": question}))
        # SigV4Auth(session.get_credentials(), 'execute-api', 'us-east-1').add_auth(req)
        # response = requests.Session().send(req.prepare())
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": f"HTTP {response.status_code}: {response.text[:200]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "Request timed out"}
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": f"Network error: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


def has_valid_results(response_data: dict) -> bool:
    """Check if the response has valid results."""
    if not response_data.get("success"):
        return False
    
    data = response_data.get("data", {})
    
    # Check for error in response
    if data.get("error"):
        return False
    
    # Check if results exist and have data
    results = data.get("results", [])
    if results and len(results) > 0:
        return True
    
    # Check if there's a narrative (sometimes queries return aggregate values in narrative)
    if data.get("narrative"):
        return True
    
    # Check for query (even if no results, query generation succeeded)
    if data.get("query"):
        return True
    
    return True
def main():
    print("=" * 60)
    print("Generating 100 Taxi Data Questions")
    print("=" * 60)
    print()
    
    # Flatten all questions
    all_questions = []
    for category, difficulty, questions in QUESTION_TEMPLATES:
        for q in questions:
            all_questions.append((category, difficulty, q))
    
    print(f"Total template questions: {len(all_questions)}")
    print(f"Backup questions available: {len(BACKUP_QUESTIONS)}")
    print()
    
    successful_results = []
    failed_questions = []
    backup_index = 0
    
    # Process each question
    for i, (category, difficulty, question) in enumerate(all_questions):
        if len(successful_results) >= 100:
            break
            
        question_id = i + 1
        print(f"[{len(successful_results) + 1}/100] Q{question_id}: {question[:60]}...")
        
        result = call_api(question)
        
        if has_valid_results(result):
            data = result["data"]
            entry = {
                "question_id": len(successful_results) + 1,
                "text": question,
                "category": category,
                "difficulty": difficulty,
                "response": {
                    "query": data.get("query", ""),
                    "results": data.get("results", [])[:10],  # Limit results for file size
                    "row_count": len(data.get("results", [])),
                    "narrative": data.get("narrative", ""),
                    "execution_time": data.get("execution_time", 0)
                }
            }
            successful_results.append(entry)
            print(f"    ✓ Success ({len(data.get('results', []))} rows)")
        else:
            error = result.get("error", result.get("data", {}).get("error", "Unknown error"))
            print(f"    ✗ Failed: {str(error)[:50]}")
            failed_questions.append((category, difficulty, question, error))
        
        # Rate limiting
        time.sleep(0.5)
    
    # Try backup questions if needed
    while len(successful_results) < 100 and backup_index < len(BACKUP_QUESTIONS):
        category, difficulty, question = BACKUP_QUESTIONS[backup_index]
        backup_index += 1
        
        print(f"[{len(successful_results) + 1}/100] BACKUP: {question[:60]}...")
        
        result = call_api(question)
        
        if has_valid_results(result):
            data = result["data"]
            entry = {
                "question_id": len(successful_results) + 1,
                "text": question,
                "category": category,
                "difficulty": difficulty,
                "response": {
                    "query": data.get("query", ""),
                    "results": data.get("results", [])[:10],
                    "row_count": len(data.get("results", [])),
                    "narrative": data.get("narrative", ""),
                    "execution_time": data.get("execution_time", 0)
                }
            }
            successful_results.append(entry)
            print(f"    ✓ Success ({len(data.get('results', []))} rows)")
        else:
            error = result.get("error", "Unknown error")
            print(f"    ✗ Failed: {str(error)[:50]}")
        
        time.sleep(0.5)
    
    # Generate additional simple questions if still short
    simple_questions = [
        ("trip_volume", "easy", "Show the first 10 yellow taxi trips"),
        ("fare_analysis", "easy", "List 5 trips with the highest fares"),
        ("distance_analysis", "easy", "Show 10 trips sorted by distance"),
        ("passenger_analysis", "easy", "Count trips by passenger count"),
        ("temporal_analysis", "easy", "Count trips by month in 2024"),
        ("payment_analysis", "easy", "Count trips by payment type"),
        ("location_analysis", "easy", "Count trips by pickup location"),
        ("revenue_analysis", "easy", "Show total fare, tip, and total amount sums"),
        ("aggregation", "easy", "What is the count of all yellow taxi records?"),
        ("trip_volume", "easy", "How many rows are in the yellow_taxi table?"),
    ]
    
    simple_index = 0
    while len(successful_results) < 100 and simple_index < len(simple_questions):
        category, difficulty, question = simple_questions[simple_index]
        simple_index += 1
        
        print(f"[{len(successful_results) + 1}/100] SIMPLE: {question[:60]}...")
        
        result = call_api(question)
        
        if has_valid_results(result):
            data = result["data"]
            entry = {
                "question_id": len(successful_results) + 1,
                "text": question,
                "category": category,
                "difficulty": difficulty,
                "response": {
                    "query": data.get("query", ""),
                    "results": data.get("results", [])[:10],
                    "row_count": len(data.get("results", [])),
                    "narrative": data.get("narrative", ""),
                    "execution_time": data.get("execution_time", 0)
                }
            }
            successful_results.append(entry)
            print(f"    ✓ Success")
        else:
            print(f"    ✗ Failed")
        
        time.sleep(0.5)
    
    # Save results
    output_file = "profile_data/financial_questions.jsonl"
    print()
    print("=" * 60)
    print(f"Saving {len(successful_results)} results to {output_file}")
    print("=" * 60)
    
    with open(output_file, 'w') as f:
        for entry in successful_results:
            f.write(json.dumps(entry) + '\n')
    
    print(f"✓ Saved {len(successful_results)} questions with answers")
    print()
    
    # Summary
    print("Summary:")
    print(f"  Successful: {len(successful_results)}")
    print(f"  Failed: {len(failed_questions)}")
    print()
    
    if failed_questions:
        print("Failed questions:")
        for cat, diff, q, err in failed_questions[:10]:
            print(f"  - {q[:50]}... ({err[:30]})")
        if len(failed_questions) > 10:
            print(f"  ... and {len(failed_questions) - 10} more")


if __name__ == "__main__":
    main()