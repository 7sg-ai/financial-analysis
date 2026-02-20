#!/usr/bin/env python3
"""
Generate taxi data questions for January 2024 data only, test them against the API, and save results.
"""

import json
import boto3
from botocore.config import Config
import time
from datetime import datetime

# AWS API Gateway endpoint (replaces Azure Container Instance)
# Replace with your deployed API Gateway invoke URL
API_BASE_URL = "https://<your-api-id>.execute-api.<region>.amazonaws.com/prod"

# Categories of questions - filtered for January 2024 only
QUESTION_TEMPLATES = [
    # Trip volume questions
    ("trip_volume", "easy", [
        "How many yellow taxi trips were there in January 2024?",
        "What is the total number of green taxi trips in January 2024?",
        "How many taxi trips happened on weekends in January 2024?",
        "What is the average daily trip count for yellow taxis in January 2024?",
        "How many trips were there in the first week of January 2024?",
        "What was the busiest day in January 2024?",
        "How many trips occurred on New Year's Day 2024?",
        "What is the total trip count for all taxis in January 2024?",
        "How many weekday trips were there in January 2024?",
        "What was the least busy day in January 2024?",
    ]),
    
    # Fare analysis
    ("fare_analysis", "easy", [
        "What is the average fare amount for yellow taxi trips in January 2024?",
        "What is the total revenue from yellow taxi fares in January 2024?",
        "What is the maximum fare amount recorded in January 2024?",
        "What is the minimum fare amount for trips over 1 mile in January 2024?",
        "What is the average total amount including tips for yellow taxis in January 2024?",
        "What is the median fare amount for green taxi trips in January 2024?",
        "What is the average fare per mile for yellow taxis in January 2024?",
        "What percentage of fares are under $10 in January 2024?",
        "What is the 90th percentile fare amount in January 2024?",
        "What is the average fare for trips on weekends in January 2024?",
    ]),
    
    # Tip analysis
    ("tip_analysis", "medium", [
        "What is the average tip amount for yellow taxi trips in January 2024?",
        "What percentage of trips include a tip in January 2024?",
        "What is the average tip percentage relative to fare amount in January 2024?",
        "Which payment type has the highest average tip in January 2024?",
        "What is the maximum tip amount recorded in January 2024?",
        "How does the average tip vary by hour of day in January 2024?",
        "What is the total tip revenue for January 2024?",
        "What is the average tip for trips over $50 in January 2024?",
        "Do longer trips receive higher tips on average in January 2024?",
        "What is the tip amount distribution in January 2024?",
    ]),
    
    # Distance analysis
    ("distance_analysis", "easy", [
        "What is the average trip distance for yellow taxis in January 2024?",
        "What is the longest trip distance recorded in January 2024?",
        "How many trips were under 1 mile in January 2024?",
        "What percentage of trips are between 1 and 5 miles in January 2024?",
        "What is the total distance traveled by all yellow taxis in January 2024?",
        "What is the average trip distance for green taxis in January 2024?",
        "How many trips exceeded 20 miles in January 2024?",
        "What is the median trip distance in January 2024?",
        "What is the average distance for airport trips in January 2024?",
        "How does trip distance vary by time of day in January 2024?",
    ]),
    
    # Passenger analysis
    ("passenger_analysis", "easy", [
        "What is the average number of passengers per trip in January 2024?",
        "How many trips had more than 4 passengers in January 2024?",
        "What percentage of trips are solo passengers in January 2024?",
        "What is the most common passenger count in January 2024?",
        "How many trips had 2 passengers in January 2024?",
        "What is the distribution of passenger counts in January 2024?",
        "Do trips with more passengers travel longer distances in January 2024?",
        "What percentage of trips have 3 or more passengers in January 2024?",
        "How does passenger count vary by hour in January 2024?",
        "What is the average fare for single passenger trips in January 2024?",
    ]),
    
    # Time-based analysis
    ("temporal_analysis", "medium", [
        "What is the busiest hour of day for taxi pickups in January 2024?",
        "How many trips start between 8 AM and 9 AM in January 2024?",
        "What day of the week has the most taxi trips in January 2024?",
        "What is the average trip duration in minutes in January 2024?",
        "How does trip volume change throughout the day in January 2024?",
        "What percentage of trips occur during rush hour in January 2024?",
        "What is the busiest time for airport pickups in January 2024?",
        "How many trips occur after midnight in January 2024?",
        "What is the average trip duration during rush hour vs off-peak in January 2024?",
        "Which week in January 2024 had the highest trip volume?",
    ]),
    
    # Location analysis
    ("location_analysis", "medium", [
        "What is the most popular pickup location in January 2024?",
        "What is the most popular dropoff location in January 2024?",
        "How many trips start and end in the same zone in January 2024?",
        "What are the top 10 pickup locations by trip count in January 2024?",
        "What are the top 5 dropoff locations in January 2024?",
        "Which location has the highest average fare in January 2024?",
        "How many different pickup locations are there in January 2024?",
        "What is the most common pickup-dropoff pair in January 2024?",
        "Which location has the most airport trips in January 2024?",
        "What percentage of trips start in Manhattan in January 2024?",
    ]),
    
    # Payment analysis
    ("payment_analysis", "easy", [
        "What percentage of trips are paid by credit card in January 2024?",
        "How many trips were paid in cash in January 2024?",
        "What is the distribution of payment types in January 2024?",
        "What is the average fare for credit card payments vs cash in January 2024?",
        "What payment type has the highest average total amount in January 2024?",
        "What percentage of trips use payment type 1 in January 2024?",
        "How many trips have no recorded payment type in January 2024?",
        "What is the most common payment method in January 2024?",
        "Do cash payments have lower tips than credit cards in January 2024?",
        "What is the total amount collected by payment type in January 2024?",
    ]),
    
    # Comparative analysis
    ("comparative_analysis", "hard", [
        "How do yellow taxi fares compare to green taxi fares in January 2024?",
        "What is the difference in average trip distance between yellow and green taxis in January 2024?",
        "Which taxi type has higher average tips in January 2024?",
        "Compare weekend vs weekday trip volumes in January 2024",
        "How do morning trips compare to evening trips in terms of fare in January 2024?",
        "Compare tip percentages between yellow and green taxis in January 2024",
        "What is the difference in passenger count between taxi types in January 2024?",
        "Compare first week vs last week of January 2024 trip volumes",
        "How do weekday fares compare to weekend fares in January 2024?",
        "Which taxi type has longer average trip duration in January 2024?",
    ]),
    
    # Revenue analysis
    ("revenue_analysis", "hard", [
        "What is the total revenue from all taxi trips in January 2024?",
        "What is the average revenue per trip including all charges in January 2024?",
        "How much was collected in tolls in January 2024?",
        "What is the total congestion surcharge collected in January 2024?",
        "How much MTA tax was collected in January 2024?",
        "What is the total airport fee revenue in January 2024?",
        "What is the daily revenue trend for January 2024?",
        "Which day generated the most revenue in January 2024?",
        "What percentage of revenue comes from tips in January 2024?",
        "How does revenue per mile compare across weeks in January 2024?",
    ]),
    
    # Daily trend analysis
    ("trend_analysis", "medium", [
        "How does the average fare vary by day of week in January 2024?",
        "Is trip volume higher on weekdays or weekends in January 2024?",
        "How does the average tip percentage vary by day of week in January 2024?",
        "What is the trend in daily trip distance in January 2024?",
        "How does credit card vs cash usage vary by day in January 2024?",
        "How does the average passenger count vary by day of week in January 2024?",
        "What is the daily trip count trend in January 2024?",
        "How does the congestion surcharge vary throughout January 2024?",
        "Are trips longer or shorter on weekends in January 2024?",
        "How does the average fare vary throughout January 2024?",
    ]),
    
    # Aggregation questions
    ("aggregation", "medium", [
        "What is the sum of all fare amounts in January 2024?",
        "Count the total number of trips by vendor ID in January 2024",
        "What is the average of all numeric columns for yellow taxi in January 2024?",
        "Group trips by payment type and show average fare in January 2024",
        "What is the standard deviation of trip distances in January 2024?",
        "Calculate the variance in fare amounts in January 2024",
        "What is the correlation between distance and fare in January 2024?",
        "Group trips by hour and count them in January 2024",
        "What is the total extra charges collected in January 2024?",
        "Sum the improvement surcharge by day in January 2024",
    ]),
]

# Extra backup questions in case some fail
BACKUP_QUESTIONS = [
    ("trip_volume", "easy", "How many total taxi trips are in January 2024?"),
    ("fare_analysis", "easy", "What is the average fare for all trips in January 2024?"),
    ("distance_analysis", "easy", "What is the average distance for all trips in January 2024?"),
    ("passenger_analysis", "easy", "What is the most common number of passengers in January 2024?"),
    ("temporal_analysis", "medium", "What hour has the most pickups in January 2024?"),
    ("location_analysis", "medium", "What is the most frequent pickup location ID in January 2024?"),
    ("payment_analysis", "easy", "How many trips were paid by credit card in January 2024?"),
    ("revenue_analysis", "medium", "What is the total amount collected from all trips in January 2024?"),
    ("tip_analysis", "medium", "What is the total tips collected in January 2024?"),
    ("distance_analysis", "easy", "How many trips were longer than 10 miles in January 2024?"),
    ("fare_analysis", "easy", "What is the highest single fare recorded in January 2024?"),
    ("passenger_analysis", "easy", "How many trips had exactly 1 passenger in January 2024?"),
    ("temporal_analysis", "medium", "How many trips started after 6 PM in January 2024?"),
    ("trip_volume", "easy", "What is the daily average number of trips in January 2024?"),
    ("revenue_analysis", "medium", "What is the average total amount per trip in January 2024?"),
    ("location_analysis", "medium", "What are the top 3 pickup locations in January 2024?"),
    ("payment_analysis", "easy", "What percentage of trips have payment_type = 1 in January 2024?"),
    ("tip_analysis", "medium", "What is the average tip for credit card payments in January 2024?"),
    ("distance_analysis", "easy", "What is the total distance of all trips combined in January 2024?"),
    ("fare_analysis", "easy", "How many trips had a fare over $100 in January 2024?"),
    ("temporal_analysis", "medium", "Which day of week has the least trips in January 2024?"),
    ("aggregation", "medium", "What is the average fare by vendor ID in January 2024?"),
    ("comparative_analysis", "hard", "Compare average fare between yellow and green taxis in January 2024"),
    ("revenue_analysis", "hard", "What is total revenue by payment type in January 2024?"),
    ("location_analysis", "medium", "How many unique dropoff locations exist in January 2024?"),
    ("passenger_analysis", "easy", "What percentage of trips have 2+ passengers in January 2024?"),
    ("tip_analysis", "medium", "What is the max tip recorded in January 2024?"),
    ("fare_analysis", "easy", "What is the minimum non-zero fare in January 2024?"),
    ("trip_volume", "easy", "Count trips in the second week of January 2024"),
    ("trip_volume", "easy", "How many trips on January 15, 2024?"),
]


def call_api(question: str) -> dict:
    """Call the analyze API with a question."""
    try:
        # Use boto3 to invoke Lambda directly (or API Gateway via HTTP)
        # Option 1: Direct Lambda invocation (recommended for internal use)
        lambda_client = boto3.client('lambda', config=Config(connect_timeout=120, read_timeout=120))
        response = lambda_client.invoke(
            FunctionName='financial-analysis-api',
            InvocationType='RequestResponse',
            Payload=json.dumps({"question": question})
        )
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        if response['StatusCode'] == 200:
            return {"success": True, "data": response_payload}
        else:
            return {"success": False, "error": f"Lambda status {response['StatusCode']}: {str(response_payload)}"}
            
    except botocore.exceptions.EndpointConnectionError as e:
        return {"success": False, "error": "Connection failed (check Lambda function and VPC settings)"}
    except botocore.exceptions.ReadTimeoutError as e:
        return {"success": False, "error": "Request timed out"}
    except botocore.exceptions.ClientError as e:
        return {"success": False, "error": f"AWS client error: {str(e)}"}
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
    
    return False
import boto3
from botocore.config import Config
import json
import time

def main():
    print("=" * 60)
    print("Generating Taxi Data Questions (January 2024 Only)")
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
        ("trip_volume", "easy", "Show the first 10 yellow taxi trips in January 2024"),
        ("fare_analysis", "easy", "List 5 trips with the highest fares in January 2024"),
        ("distance_analysis", "easy", "Show 10 trips sorted by distance in January 2024"),
        ("passenger_analysis", "easy", "Count trips by passenger count in January 2024"),
        ("temporal_analysis", "easy", "Count trips by day in January 2024"),
        ("payment_analysis", "easy", "Count trips by payment type in January 2024"),
        ("location_analysis", "easy", "Count trips by pickup location in January 2024"),
        ("revenue_analysis", "easy", "Show total fare, tip, and total amount sums for January 2024"),
        ("aggregation", "easy", "What is the count of all yellow taxi records in January 2024?"),
        ("trip_volume", "easy", "How many rows are in the yellow_taxi table for January 2024?"),
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
    output_file = "profile_data/financial_questions_jan.jsonl"
    print()
    print("=" * 60)
    print(f"Saving {len(successful_results)} results to {output_file}")
    print("=" * 60)
    
    # Upload to S3
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        s3.put_object(
            Bucket="your-bucket-name",  # Replace with your S3 bucket name
            Key=output_file,
            Body='\n'.join(json.dumps(entry) for entry in successful_results)
        )
        print(f"✓ Uploaded {len(successful_results)} questions to S3")
    except Exception as e:
        print(f"✗ Failed to upload to S3: {str(e)}")
        # Fallback to local file if S3 upload fails
        with open(output_file, 'w') as f:
            for entry in successful_results:
                f.write(json.dumps(entry) + '\n')
        print(f"✓ Saved {len(successful_results)} questions locally")
    
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