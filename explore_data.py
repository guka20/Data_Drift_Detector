import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

print("Generating synthetic IoT temperature dataset with drift...")

# კონფიგურაცია
START_DATE = datetime(2024, 1, 1, 0, 0, 0)
DAYS = 120 
EVENTS_PER_HOUR_INDOOR = 12 
EVENTS_PER_HOUR_OUTDOOR = 25  

data = []
event_id = 0

# მონაცემების დაგენერირება თითოეულ დღეზე
for day in range(DAYS):
    current_date = START_DATE + timedelta(days=day)
    
    
    # ნორმალური მონაცემები იქნება 60 დღის განმავლობაში
    # 60 დღის შემდეგ დაიწყებს Data Drift
    if day < 60:
        indoor_mean = 22.0 
        indoor_stddev = 1.5
    elif 60 <= day < 80:
        # აქ უკვე იწყებს Data Drift
        indoor_mean = 32.0  #
        indoor_stddev = 2.0
    else:
        # უბრუნდება ნორმალურ მდგომარეობას
        indoor_mean = 22.0
        indoor_stddev = 1.5
    
    seasonal_factor = (day / DAYS) * 15 
    outdoor_base = 28.0 + seasonal_factor
    
    # რეალური მონაცემისთვის დღის განმავლობაში უფრო სითბო იქნება
    for hour in range(24):
        event_time = current_date + timedelta(hours=hour)
        
        # საღამოს საათებში სიგრილე
        time_of_day_factor = 5 * (0.5 - abs(hour - 12) / 24) 
        
        # შიდა სივრცის ევენთები
        for _ in range(EVENTS_PER_HOUR_INDOOR):
            temp = random.gauss(indoor_mean, indoor_stddev)
            temp = max(15, min(45, temp))  
            
            data.append({
                'id': f'__export__.temp_log_{event_id}_{uuid.uuid4().hex[:12]}',
                'room_id/id': 'Room Admin',
                'noted_date': event_time.strftime('%d-%m-%Y %H:%M'),
                'temp': int(round(temp)),
                'out/in': 'In'
            })
            event_id += 1
        
        # გარე სივრცის ევენთები
        outdoor_mean = outdoor_base + time_of_day_factor
        outdoor_stddev = 3.0
        
        for _ in range(EVENTS_PER_HOUR_OUTDOOR):
            temp = random.gauss(outdoor_mean, outdoor_stddev)
            temp = max(20, min(50, temp)) 
            
            data.append({
                'id': f'__export__.temp_log_{event_id}_{uuid.uuid4().hex[:12]}',
                'room_id/id': 'Room Admin',
                'noted_date': event_time.strftime('%d-%m-%Y %H:%M'),
                'temp': int(round(temp)),
                'out/in': 'Out'
            })
            event_id += 1
    
    if (day + 1) % 20 == 0:
        print(f"Generated {day + 1}/{DAYS} days ({len(data):,} events)")


df = pd.DataFrame(data)

df = df.sample(frac=1).reset_index(drop=True)

filename = 'IOT-temp-with-drift.csv'
df.to_csv(filename, index=False)
