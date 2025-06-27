# import os
# import re
# import requests
# from dotenv import load_dotenv

# def data_extraction():
#     """
#     Fetch rental and sale listings for a set of US states using the RentCast API.

#     Returns:
#         dict: Nested dictionary of API responses keyed by region and state code.
#     """

#     # Load the API key from environment
#     load_dotenv()

#     API_KEY = os.getenv("API_KEY")
#     if not API_KEY:
#         raise RuntimeError("API_KEY not set in environment")

#     headers = {
#         "accept": "application/json",
#         "X-Api-Key": API_KEY
#     }

#     # Base URLs with placeholder state code WV
#     base_urls = {
#         "rental_listings": (
#             "https://api.rentcast.io/v1/listings/rental/long-term"
#             "?state=WV&status=Active&daysOld=5&limit=500"
#         ),
#         "sale_listings": (
#             "https://api.rentcast.io/v1/listings/sale"
#             "?state=WV&status=Active&daysOld=5&limit=500"
#         )
#     }

#     # States to iterate through (WV already included in template)
#     states = ["WV", "VA", "SC", "NC", "MD", "GA", "FL", "DE"]

#     # Container for all data
#     all_data_1 = {region: {} for region in base_urls}

#     # Fetch data for each region and state
#     for region, url_template in base_urls.items():
#         for state_code in states:
#             # Substitute the state code in the URL
#             url = re.sub(r"state=[A-Z]{2}", f"state={state_code}", url_template)
#             response = requests.get(url, headers=headers)

#             if response.status_code == 200:
#                 all_data_1[region][state_code] = response.json()
#             else:
#                 all_data_1[region][state_code] = None
#                 print(f"Error fetching {region} for {state_code}: {response.status_code}")
    
#     # print(json.dumps(rental_listings, indent=2))
#     return all_data_1
