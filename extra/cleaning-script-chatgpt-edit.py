import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import numpy as np

# Caching mechanism
cache = {}

def do_geocode(place, geolocator, attempt=1, max_attempts=3):
    if place in cache:
        return cache[place]
    
    try:
        location = geolocator.reverse(place, language='en', timeout=10)
        if location is not None:
            result = location.address
        else:
            result = None  # or "Unknown", if you prefer
        cache[place] = result
        return result
    except GeocoderTimedOut:
        if attempt < max_attempts:
            return do_geocode(place, geolocator, attempt=attempt+1)
        raise


def add_location(df):
    df["Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School"] = df["Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School"].fillna("")
    
    # Prepare location strings
    df["Location"] = df["Location Latitude"].astype(str) + ", " + df["Location Longitude"].astype(str)
    df["Location"].replace("nan, nan", "0, 0", inplace=True)
    
    # Initialize geolocator
    geolocator = Nominatim(user_agent="USER2")
    
    # Perform geocoding
    df["Geocode"] = df["Location"].apply(lambda place: do_geocode(place, geolocator))

    # Extract countries and states
    df["Country"] = df["Geocode"].apply(lambda loc: loc.split(",")[-1].strip() if loc else np.nan)
    df["State"] = df.apply(lambda row: row["Geocode"].split(",")[-3].strip() if row["Country"] == "United States" else np.nan, axis=1)

    return df


def get_output_schema():
    return pd.DataFrame({
 			'Survey Year' : prep_string(),
            'Gender' : prep_string(),
            'Location Latitude' : prep_decimal(),
            'Location Longitude' : prep_decimal(),
            'When did you first begin your graduate business school research with respect to engaging in the application process?' : prep_string(),
 			'Were you a Domestic or International applicant?' : prep_string(),
 			'Did you decide to choose an MBA/MS/Online or other Graduate Program at a different school?' : prep_string(),
            'What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -' : prep_string(),
             'What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): - Other - Text' : prep_string(),
             'What are your plans for the upcoming year? - Selected Choice' : prep_string(),
             'What are your plans for the upcoming year? - Other - Text' : prep_string(),
             'To which other schools did you apply? (If applicable)' : prep_string(),
             "Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School" : prep_string(),
             'Name of the school that you will be attending:' : prep_string(),
             'What is your race? Select all that apply. - Selected Choice' : prep_string(),
             'Term and year to which you applied:' : prep_string(),
             'What was the modality of the program that you ultimately enrolled in?' : prep_string(),
             'Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program  1' : prep_string(),
             'Program at Krannert to which you applied: -  Selected Choice' : prep_string(),
             'Name of the Program that you will be attending:' : prep_string(),
             'Country' : prep_string(),
             'State' : prep_string(),
             'Recorded Date' : prep_string(),
             'Response ID': prep_string(),
 			})
