import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Get Census API key from environment variable
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

if not CENSUS_API_KEY or CENSUS_API_KEY == "YOUR_CENSUS_API_KEY_HERE":
    raise ValueError(
        "CENSUS_API_KEY not found or not set. "
        "Please copy .env.example to .env and add your Census API key. "
        "Get your free key from: https://api.census.gov/data/key_signup.html"
    )

# Time interval for calibration (default: 1 hour)
TIME_INTERVAL = "1H"
