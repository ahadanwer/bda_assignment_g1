#!/usr/bin/env python3
"""
MapReduce Mapper for Big Dataset (14 columns)
Handles: order_id,tracking_number,grand_total,weight,pickup_address,destination_address,
         cod_payment,cod,seller_id,company_name,courier_name,courier_service_value,status,status_at
"""

import sys
import csv
import re

# Comprehensive list of Pakistani cities (sorted by length desc for better matching)
PAKISTANI_CITIES = [
    'dera ghazi khan', 'rahim yar khan', 'dera ismail khan', 'mandi bahauddin', 
    'lakki marwat', 'mirpur khas', 'tando allahyar', 'saidu sharif', 'pak pattan',
    'tando adam', 'bahawalnagar', 'abbottabad', 'rawalpindi', 'faisalabad', 
    'gujranwala', 'sheikhupura', 'bahawalpur', 'shekhupura', 'muzaffargarh',
    'nawabshah', 'jacobabad', 'shikarpur', 'abottabad', 'charsadda', 'tharparkar',
    'rawalakot', 'timergara', 'battagram', 'islamabad', 'hyderabad', 'sargodha',
    'karachi', 'peshawar', 'sialkot', 'mingora', 'chiniot', 'kamoke', 'jhelum',
    'sadiqabad', 'khanewal', 'hafizabad', 'khanpur', 'mansehra', 'haripur',
    'charsadda', 'nowshera', 'mirpurkhas', 'jamshoro', 'khushab', 'chakwal',
    'mianwali', 'jalalpur', 'khairpur', 'narowal', 'lodhran', 'rajanpur',
    'layyah', 'bhakkar', 'umerkot', 'havelian', 'balakot', 'lahore', 'multan',
    'quetta', 'sukkur', 'larkana', 'mardan', 'sahiwal', 'okara', 'kohat',
    'gojra', 'mandi', 'kasur', 'vihari', 'vehari', 'attock', 'taxila',
    'gujrat', 'badin', 'thatta', 'dadu', 'mailsi', 'swabi', 'hangu',
    'bannu', 'karak', 'tank', 'zhob', 'loralai', 'pishin', 'chaman',
    'turbat', 'gwadar', 'khuzdar', 'hub', 'kotri', 'matiari', 'sanghar',
    'bagh', 'kotli', 'gilgit', 'skardu', 'hunza', 'chitral', 'dir',
    'swat', 'buner', 'shangla', 'kohistan', 'malakand', 'alpurai', 'daggar', 'wah'
]

def extract_city(address):
    if not address:
        return "Unknown"
    # Clean and normalize address
    address_lower = address.strip().strip('"').lower()
    # Check if any known city is contained in address (sorted by length, so longer matches first)
    for city in PAKISTANI_CITIES:
        if city in address_lower:
            # Verify it's a word boundary match to avoid false positives
            if re.search(r'\b' + re.escape(city) + r'\b', address_lower):
                return city.title()
    return "Unknown"

def main():
    reader = csv.reader(sys.stdin)
    next(reader, None)  # Skip header
    for row in reader:
        try:
            if len(row) < 14: continue
        
            # Parse 14-column format
            order_id = row[0].strip()
            tracking_number = row[1].strip()
            grand_total = row[2].strip()
            weight = row[3].strip()
            pickup_address = row[4].strip()
            destination_address = row[5].strip()
            cod_payment = row[6].strip()  # 1=COD, 0=prepaid
            cod_amount = row[7].strip()
            seller_id = row[8].strip()
            company_name = row[9].strip()
            courier_name = row[10].strip()
            courier_service = row[11].strip()
            status = row[12].strip()
            status_at = row[13].strip()
        
            # Extract cities
            pickup_city = extract_city(pickup_address)
            destination_city = extract_city(destination_address)
            
            # Emit: order_id \t 
            # tracking|total|weight|pickup|dest|cod_flag|cod_amt|company|courier|service|status|status_at
            if order_id and tracking_number and status and status_at:
                print("{}\t{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
                      order_id, tracking_number, grand_total, weight,
                      pickup_city, destination_city, cod_payment, cod_amount,
                      company_name, courier_name, courier_service, status, status_at))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
