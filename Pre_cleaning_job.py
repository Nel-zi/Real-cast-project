import json
from collections import defaultdict
import copy
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


with open('all_data_22.json', 'r', encoding='utf-8') as f:
    all_data_22 = json.load(f)

# def pre_cleaning():
#     # 1) extract…
#     all_data = data_extraction()



def pre_cleaning(all_data_22):
    # Extracting the 2 main data endpoints from the global data
    sale_listings = all_data_22["sale_listings"]
    rental_listings = all_data_22["rental_listings"]

    # saving the jason data to current directory
    with open('sale_listings.json', 'w', encoding='utf-8') as f:
        json.dump(sale_listings, f, ensure_ascii=False, indent=2)

    with open('rental_listings.json', 'w', encoding='utf-8') as f:
        json.dump(rental_listings, f, ensure_ascii=False, indent=2)

    # Making a copy of the data before editing
    sale_listings_copy = copy.deepcopy(sale_listings)
    rental_listings_copy = copy.deepcopy(rental_listings)

    # Further extraction and pre-cleaning starting with sale_listings_copy
    property_history = defaultdict(list)
    property_agent = defaultdict(list)
    property_officer = defaultdict(list)
    property_sales_info = defaultdict(list)

    for state, listings in sale_listings_copy.items():
        for listing in listings:
            # grab the unique id
            listing_id = listing["id"]

            # extract & inject id into history
            history = listing.pop("history", {})
            for date_key, event in history.items():
                event["id"] = listing_id

            # extract & inject id into agent
            listing_agent = listing.pop("listingAgent", {})
            if listing_agent:
                listing_agent["id"] = listing_id

            # extract & inject id into office
            listing_office = listing.pop("listingOffice", {})
            if listing_office:
                listing_office["id"] = listing_id

            # whatever remains is my “sales info” (it still has 'id' there)
            sales = listing

            # putting all together
            property_history[state].append(history)
            property_agent[state].append(listing_agent)
            property_officer[state].append(listing_office)
            property_sales_info[state].append(sales)

    # Next is the rental_listings_copy

    # Prepare collectors
    rental_history = defaultdict(list)
    rental_listing_info = defaultdict(list)

    # Extract history + rest for each listing
    for state, listings in rental_listings_copy.items():
        for listing in listings:
            listing_id = listing["id"]

            # pop & tag history
            history = listing.pop("history", {})
            for date_key, event in history.items():
                event["id"] = listing_id
            rental_history[state].append(history)

            # everything left is the “rest” of the listing
            #    (it still includes 'id' plus all other top-level fields)
            rest = listing
            rental_listing_info[state].append(rest)

    # Exacting the fields i want to keep in property_sales_info:
    fields = [
        "id", "addressLine1", "city", "state", "zipCode", "county",
        "propertyType", "bedrooms", "bathrooms", "squareFootage",
        "lotSize", "yearBuilt", "price", "listedDate", "daysOnMarket"
    ]

    # Preparing a flat list to collect each listing’s info
    extracted_property_sales_info = []

    for listings in property_sales_info.values():        # loop each state’s list
        for listing in listings:                  # loop each listing dict
            # Building a new dict with only the desired keys
            info = {}
            for field in fields:
                info[field] = listing.get(field)
            # Add it to the flat list
            extracted_property_sales_info.append(info)

    # Exacting the fields i want to keep in property_history:
    fields = [
        "event", "price", "listingType", "listedDate",
        "removedDate", "daysOnMarket", "id"
    ]

    extracted_property_history = []

    for state_list in property_history.values():        # loop each state’s list
        for date_dict in state_list:                  # loop each {"2025-06-25": {...}} entry
            # date_dict has one key (the date) and one value (the actual listing dict)
            for date_key, listing in date_dict.items():
                # building info dict
                info = {}
                for field in fields:
                    # copy the field value (or None if missing)
                    value = listing.get(field)
                    info[field] = value
                extracted_property_history.append(info)

    # Exacting the fields you want to keep in property_agent:
    fields = ["name", "phone", "email", "id"]

    # Preparing a flat list to collect each listing’s info
    extracted_property_agent = []

    for listings in property_agent.values():        # loop each state’s list
        for listing in listings:                  # loop each listing dict
            # Build a new dict with only the desired keys
            info = {}
            for field in fields:
                info[field] = listing.get(field)
            # Add it to the flat list
            extracted_property_agent.append(info)

    # Exacting the fields i want to keep in property_officer:
    fields = ["name", "phone", "email", "website", "id"]

    # Preparing a flat list to collect each listing’s info
    extracted_property_officer = []

    for listings in property_officer.values():        # loop each state’s list
        for listing in listings:                  # loop each listing dict
            # Building a new dict with only the desired keys
            info = {}
            for field in fields:
                info[field] = listing.get(field)
            # Add it to the flat list
            extracted_property_officer.append(info)

    # Exacting the fields i want to keep in rental_listing_info:
    fields = [
        "id", "addressLine1", "city", "state", "zipCode", "county",
        "propertyType", "bedrooms", "bathrooms", "squareFootage",
        "price", "listedDate", "daysOnMarket"
    ]

    # Preparing a flat list to collect each listing’s info
    extracted_rental_listing_info = []

    for listings in rental_listing_info.values():        # loop each state’s list
        for listing in listings:                  # loop each listing dict
            # Building a new dict to collect
            info = {}
            for field in fields:
                info[field] = listing.get(field)
            extracted_rental_listing_info.append(info)

    # Exacting the fields i want to keep in rental_history:
    fields = [
        "event", "price", "listingType", "listedDate",
        "removedDate", "daysOnMarket", "id"
    ]

    extracted_rental_history = []

    for state_list in rental_history.values():        # loop each state’s list
        for date_dict in state_list:                  # loop each {"2025-06-25": {...}} entry
            for date_key, listing in date_dict.items():
                # building info dict
                info = {}
                for field in fields:
                    value = listing.get(field)
                    info[field] = value
                extracted_rental_history.append(info)
    
    print(extracted_property_sales_info)
    return {
        "extracted_property_sales_info": extracted_property_sales_info,
        "extracted_property_history": extracted_property_history,
        "extracted_property_agent": extracted_property_agent,
        "extracted_property_officer": extracted_property_officer,
        "extracted_rental_listing_info": extracted_rental_listing_info,
        "extracted_rental_history": extracted_rental_history
    }


# x = pre_cleaning(all_data_22)
# print(json.dumps(x, indent=2))