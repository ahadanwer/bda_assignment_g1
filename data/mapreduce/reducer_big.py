#!/usr/bin/env python3
"""
MapReduce Reducer for Big Dataset
Outputs: order_id,tracking_number,courier_name,courier_service,company_name,final_status,
         grand_total,weight,is_cod,cod_amount,pickup_city,destination_city,delivery_time_hours,
         first_status_at,last_status_at
"""

import sys
from datetime import datetime

def parse_timestamp(ts_str):
    try:
        return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
    except:
        return None

def main():
    # Output header (15 columns)
    print("order_id,tracking_number,courier_name,courier_service,company_name,final_status,"
          "grand_total,weight,is_cod,cod_amount,pickup_city,destination_city,delivery_time_hours,"
          "first_status_at,last_status_at")
    
    current_order = None
    events = []
    
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            
            order_id = parts[0]
            event_data = parts[1].split('|')
            
            if len(event_data) != 12:
                continue
            
            # New order - process previous
            if current_order and current_order != order_id:
                process_order(current_order, events)
                events = []
            
            current_order = order_id
            events.append({
                'tracking': event_data[0],
                'total': event_data[1],
                'weight': event_data[2],
                'pickup': event_data[3],
                'dest': event_data[4],
                'cod_flag': event_data[5],
                'cod_amt': event_data[6],
                'company': event_data[7],
                'courier': event_data[8],
                'service': event_data[9],
                'status': event_data[10],
                'timestamp': event_data[11]
            })
            
        except:
            continue
    
    # Process last order
    if current_order and events:
        process_order(current_order, events)

def process_order(order_id, events):
    """Process all events for one order"""
    if not events:
        return
    
    # Sort by timestamp
    events_sorted = sorted(events, key=lambda x: x['timestamp'])
    
    first_event = events_sorted[0]
    last_event = events_sorted[-1]
    
    # Calculate delivery time
    first_ts = parse_timestamp(first_event['timestamp'])
    last_ts = parse_timestamp(last_event['timestamp'])
    
    if first_ts and last_ts:
        delivery_hours = (last_ts - first_ts).total_seconds() / 3600
    else:
        delivery_hours = 0
    
    # Clean numeric fields
    try:
        grand_total = float(last_event['total']) if last_event['total'] else 0.0
    except:
        grand_total = 0.0
    
    try:
        weight = float(last_event['weight']) if last_event['weight'] else 0.0
    except:
        weight = 0.0
    
    try:
        cod_amt = float(last_event['cod_amt']) if last_event['cod_amt'] else 0.0
    except:
        cod_amt = 0.0
    
    # Output (15 fields)
    print("{},{},{},{},{},{},{},{},{},{},{},{},{:.2f},{},{}".format(
          order_id,
          last_event['tracking'],
          last_event['courier'],
          last_event['service'],
          last_event['company'],
          last_event['status'],
          grand_total,
          weight,
          last_event['cod_flag'],
          cod_amt,
          last_event['pickup'],
          last_event['dest'],
          delivery_hours,
          first_event['timestamp'],
          last_event['timestamp']))

if __name__ == '__main__':
    main()
