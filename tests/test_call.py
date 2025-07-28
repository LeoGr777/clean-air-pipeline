"""
Test function to call a function within a utility

"""

import os 
from dotenv import load_dotenv
from test_util import test

load_dotenv()

# Specific constants for this extraction script
API_KEY     = os.getenv("API_KEY")

def main():
    test()

main()