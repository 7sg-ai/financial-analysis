"""
Data schemas and metadata for taxi trip datasets
"""
from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class DatasetSchema:
    """Schema definition for a dataset"""
    name: str
    description: str
    columns: Dict[str, str]
    financial_columns: List[str]
    date_columns: List[str]
    sample_queries: List[str]


# Yellow Taxi Schema
YELLOW_TAXI_SCHEMA = DatasetSchema(
    name="yellow_taxi",
    description="Yellow taxi trip records including pickup/dropoff times, locations, fares, and payment details",
    columns={
        "VendorID": "A code indicating the TPEP provider (1=Creative Mobile Technologies, 2=VeriFone Inc.)",
        "tpep_pickup_datetime": "Date and time when the meter was engaged",
        "tpep_dropoff_datetime": "Date and time when the meter was disengaged",
        "passenger_count": "Number of passengers in the vehicle (driver entered value)",
        "trip_distance": "Trip distance in miles",
        "RatecodeID": "Rate code in effect at the end of the trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride)",
        "store_and_fwd_flag": "Whether trip record was held in vehicle memory (Y/N)",
        "PULocationID": "TLC Taxi Zone for pickup location",
        "DOLocationID": "TLC Taxi Zone for dropoff location",
        "payment_type": "Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip)",
        "fare_amount": "Time-and-distance fare calculated by meter",
        "extra": "Miscellaneous extras and surcharges (rush hour, overnight charges)",
        "mta_tax": "MTA tax automatically triggered based on metered rate",
        "tip_amount": "Tip amount (automatically populated for credit card, cash tips not included)",
        "tolls_amount": "Total amount of tolls paid in trip",
        "improvement_surcharge": "Improvement surcharge assessed trips",
        "total_amount": "Total amount charged to passengers (does not include cash tips)",
        "congestion_surcharge": "Congestion surcharge for trips in Manhattan",
        "airport_fee": "Airport fee for pickups at LaGuardia and JFK"
    },
    financial_columns=[
        "fare_amount", "extra", "mta_tax", "tip_amount", 
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee"
    ],
    date_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    sample_queries=[
        "What was the total revenue from yellow taxis in January 2024?",
        "What is the average tip percentage by payment type?",
        "Which pickup zones generated the highest revenue?"
    ]
)


# Green Taxi Schema
GREEN_TAXI_SCHEMA = DatasetSchema(
    name="green_taxi",
    description="Green taxi trip records (street-hail livery serving areas outside Manhattan core)",
    columns={
        "VendorID": "A code indicating the LPEP provider",
        "lpep_pickup_datetime": "Date and time when the meter was engaged",
        "lpep_dropoff_datetime": "Date and time when the meter was disengaged",
        "passenger_count": "Number of passengers in the vehicle",
        "trip_distance": "Trip distance in miles",
        "RatecodeID": "Rate code in effect at the end of the trip",
        "store_and_fwd_flag": "Whether trip record was held in vehicle memory",
        "PULocationID": "TLC Taxi Zone for pickup location",
        "DOLocationID": "TLC Taxi Zone for dropoff location",
        "payment_type": "Payment method",
        "fare_amount": "Time-and-distance fare",
        "extra": "Miscellaneous extras and surcharges",
        "mta_tax": "MTA tax",
        "tip_amount": "Tip amount",
        "tolls_amount": "Total tolls paid",
        "improvement_surcharge": "Improvement surcharge",
        "total_amount": "Total amount charged",
        "congestion_surcharge": "Congestion surcharge",
        "trip_type": "Code indicating street-hail or dispatch"
    },
    financial_columns=[
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge"
    ],
    date_columns=["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    sample_queries=[
        "Compare green taxi revenue to yellow taxi revenue",
        "What are peak hours for green taxi trips?",
        "Calculate average revenue per mile for green taxis"
    ]
)


# FHV (For-Hire Vehicle) Schema
FHV_SCHEMA = DatasetSchema(
    name="fhv",
    description="For-Hire Vehicle trip records (non-taxi, non-Uber/Lyft services)",
    columns={
        "dispatching_base_num": "TLC Base License Number of the base that dispatched the trip",
        "pickup_datetime": "Date and time of the pickup",
        "dropOff_datetime": "Date and time of the dropoff",
        "PUlocationID": "TLC Taxi Zone for pickup",
        "DOlocationID": "TLC Taxi Zone for dropoff",
        "SR_Flag": "Shared ride flag (Y/N)",
        "Affiliated_base_number": "Base number affiliated with the trip"
    },
    financial_columns=[],  # FHV data doesn't include fare information
    date_columns=["pickup_datetime", "dropOff_datetime"],
    sample_queries=[
        "How many FHV trips were taken in 2024?",
        "What percentage of FHV trips are shared rides?",
        "Which bases dispatch the most trips?"
    ]
)


# FHVHV (High Volume For-Hire Vehicle) Schema - Uber/Lyft
FHVHV_SCHEMA = DatasetSchema(
    name="fhvhv",
    description="High Volume For-Hire Vehicle records (Uber, Lyft, Via, etc.)",
    columns={
        "hvfhs_license_num": "License number of the HVFHS base",
        "dispatching_base_num": "TLC Base License Number",
        "originating_base_num": "Base number of the base that received the original trip request",
        "request_datetime": "Date/time when passenger requested the trip",
        "on_scene_datetime": "Date/time when driver arrived at pickup location",
        "pickup_datetime": "Date/time when passenger(s) entered the vehicle",
        "dropoff_datetime": "Date/time when passenger(s) exited the vehicle",
        "PULocationID": "TLC Taxi Zone for pickup",
        "DOLocationID": "TLC Taxi Zone for dropoff",
        "trip_miles": "Total miles for trip",
        "trip_time": "Total time of trip in seconds",
        "base_passenger_fare": "Base passenger fare before tolls, tips, taxes",
        "tolls": "Total amount of tolls paid",
        "bcf": "Black Car Fund fee",
        "sales_tax": "Sales tax on the fare",
        "congestion_surcharge": "Congestion surcharge",
        "airport_fee": "Airport fee",
        "tips": "Tip amount",
        "driver_pay": "Total driver pay (not including tips)",
        "shared_request_flag": "Whether ride was shared (Y/N)",
        "shared_match_flag": "Whether shared ride was matched (Y/N)",
        "access_a_ride_flag": "Whether trip was on behalf of Access-a-Ride (Y/N)",
        "wav_request_flag": "Whether wheelchair accessible vehicle requested (Y/N)",
        "wav_match_flag": "Whether wheelchair accessible vehicle matched (Y/N)"
    },
    financial_columns=[
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee", "tips", "driver_pay"
    ],
    date_columns=[
        "request_datetime", "on_scene_datetime", 
        "pickup_datetime", "dropoff_datetime"
    ],
    sample_queries=[
        "What is the total revenue from HVFHS services in 2024?",
        "Compare driver pay to passenger fares",
        "What's the average wait time between request and pickup?",
        "Calculate total tip revenue by month"
    ]
)


# Zone lookup schema
ZONE_SCHEMA = DatasetSchema(
    name="taxi_zones",
    description="Taxi zone lookup table mapping LocationIDs to boroughs and zones",
    columns={
        "LocationID": "Unique identifier for taxi zone",
        "Borough": "NYC borough name",
        "Zone": "Zone name/neighborhood",
        "service_zone": "Service zone category"
    },
    financial_columns=[],
    date_columns=[],
    sample_queries=[
        "Which Manhattan zones have the highest trip volume?",
        "Compare revenue across boroughs"
    ]
)


# Complete schema registry
SCHEMA_REGISTRY = {
    "yellow_taxi": YELLOW_TAXI_SCHEMA,
    "green_taxi": GREEN_TAXI_SCHEMA,
    "fhv": FHV_SCHEMA,
    "fhvhv": FHVHV_SCHEMA,
    "taxi_zones": ZONE_SCHEMA
}


def get_schema_context() -> str:
    """
    Generate a comprehensive schema context string for LLM prompts
    """
    context_parts = [
        "# NYC Taxi and For-Hire Vehicle Trip Data Schema\n",
        "## Available Datasets:\n"
    ]
    
    for schema_name, schema in SCHEMA_REGISTRY.items():
        context_parts.append(f"\n### {schema.name.upper()} Dataset")
        context_parts.append(f"Description: {schema.description}\n")
        context_parts.append("Columns:")
        for col, desc in schema.columns.items():
            context_parts.append(f"  - {col}: {desc}")
        
        if schema.financial_columns:
            context_parts.append(f"\nFinancial Columns: {', '.join(schema.financial_columns)}")
        if schema.date_columns:
            context_parts.append(f"Date Columns: {', '.join(schema.date_columns)}")
        context_parts.append("")
    
    context_parts.append("\n## Data File Patterns:")
    context_parts.append("- Yellow Taxi: yellow_tripdata_YYYY-MM.parquet")
    context_parts.append("- Green Taxi: green_tripdata_YYYY-MM.parquet")
    context_parts.append("- FHV: fhv_tripdata_YYYY-MM.parquet")
    context_parts.append("- FHVHV: fhvhv_tripdata_YYYY-MM.parquet")
    context_parts.append("- Zone Lookup: taxi_zone_lookup.csv")
    context_parts.append("\nAll 2024 monthly data files (01-12) are available.")
    
    return "\n".join(context_parts)

