#!/usr/bin/env python3
"""
Test script to verify the analysis engine without running the full API
"""
import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_analysis():
    """Test the analysis engine with sample questions"""
    from dotenv import load_dotenv
    from config import get_settings
    from analysis_engine import FinancialAnalysisEngine
    
    # Load environment
    env_file = project_root / ".env"
    if env_file.exists():
        load_dotenv(env_file)
    
    # Get settings
    settings = get_settings()
    
    # Create engine
    logger.info("Initializing Financial Analysis Engine...")
    engine = FinancialAnalysisEngine(settings)
    
    # Initialize data (load just January for faster testing)
    logger.info("Loading data...")
    engine.initialize_data(months=["01"], year="2024")
    
    # Test questions
    test_questions = [
        "How many yellow taxi trips were taken in January 2024?",
        "What was the total revenue from yellow taxis?",
        "What is the average fare amount for yellow taxis?",
    ]
    
    logger.info("\n" + "=" * 80)
    logger.info("RUNNING TEST QUERIES")
    logger.info("=" * 80 + "\n")
    
    for i, question in enumerate(test_questions, 1):
        logger.info(f"\n[Question {i}/{len(test_questions)}]")
        logger.info(f"Q: {question}\n")
        
        try:
            # Analyze
            response = engine.analyze(
                question=question,
                return_format="both",
                include_narrative=True,
                max_rows=10
            )
            
            # Print query
            logger.info(f"Generated Query:")
            logger.info(f"{response.query}\n")
            
            # Print results
            logger.info(f"Results ({len(response.results)} rows):")
            logger.info(response.to_tabular(format="simple"))
            
            # Print narrative
            if response.narrative:
                logger.info(f"\nNarrative:")
                logger.info(response.narrative)
            
            logger.info(f"\nExecution Time: {response.execution_time_ms}ms")
            logger.info("\n" + "-" * 80)
            
        except Exception as e:
            logger.error(f"Error processing question: {e}", exc_info=True)
    
    # Cleanup
    logger.info("\nShutting down engine...")
    engine.shutdown()
    
    logger.info("\nTest complete!")


def main():
    """Main entry point"""
    try:
        test_analysis()
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

