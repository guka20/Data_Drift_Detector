import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

print("🔧 Generating synthetic IoT temperature dataset with drift...")

# Configuration
START_DATE = datetime(2024, 1, 1, 0, 0, 0)
DAYS = 120  # 4 months of data
EVENTS_PER_HOUR_INDOOR = 12  # Increased from 4
EVENTS_PER_HOUR_OUTDOOR = 25  # Increased from 10

data = []
event_id = 0

# Generate data day by day
for day in range(DAYS):
    current_date = START_DATE + timedelta(days=day)
    
    # Define drift scenarios based on day
    # INDOOR: Normal for 60 days, then drift (AC broken), then recover
    if day < 60:
        # Phase 1: Normal operation
        indoor_mean = 22.0  # Comfortable room temp
        indoor_stddev = 1.5
    elif 60 <= day < 80:
        # Phase 2: DRIFT - AC broken, temps rise
        indoor_mean = 32.0  # Hot!
        indoor_stddev = 2.0
    else:
        # Phase 3: AC fixed, back to normal
        indoor_mean = 22.0
        indoor_stddev = 1.5
    
    # OUTDOOR: Seasonal variation (summer gets hotter)
    # Simulate realistic outdoor temps with seasonal drift
    seasonal_factor = (day / DAYS) * 15  # Gradual warming
    outdoor_base = 28.0 + seasonal_factor
    
    # Add daily cycle (hotter during day)
    for hour in range(24):
        event_time = current_date + timedelta(hours=hour)
        
        # Daily temperature cycle (cooler at night)
        time_of_day_factor = 5 * (0.5 - abs(hour - 12) / 24)  # Peak at noon
        
        # INDOOR events
        for _ in range(EVENTS_PER_HOUR_INDOOR):
            temp = random.gauss(indoor_mean, indoor_stddev)
            temp = max(15, min(45, temp))  # Clamp to realistic range
            
            data.append({
                'id': f'__export__.temp_log_{event_id}_{uuid.uuid4().hex[:12]}',
                'room_id/id': 'Room Admin',
                'noted_date': event_time.strftime('%d-%m-%Y %H:%M'),
                'temp': int(round(temp)),
                'out/in': 'In'
            })
            event_id += 1
        
        # OUTDOOR events
        outdoor_mean = outdoor_base + time_of_day_factor
        outdoor_stddev = 3.0
        
        for _ in range(EVENTS_PER_HOUR_OUTDOOR):
            temp = random.gauss(outdoor_mean, outdoor_stddev)
            temp = max(20, min(50, temp))  # Clamp to realistic range
            
            data.append({
                'id': f'__export__.temp_log_{event_id}_{uuid.uuid4().hex[:12]}',
                'room_id/id': 'Room Admin',
                'noted_date': event_time.strftime('%d-%m-%Y %H:%M'),
                'temp': int(round(temp)),
                'out/in': 'Out'
            })
            event_id += 1
    
    if (day + 1) % 20 == 0:
        print(f"📅 Generated {day + 1}/{DAYS} days ({len(data):,} events)")

# Create DataFrame
df = pd.DataFrame(data)

# Shuffle to simulate real-world disorder
df = df.sample(frac=1).reset_index(drop=True)

# Save to CSV
filename = 'IOT-temp-with-drift.csv'
df.to_csv(filename, index=False)

print(f"\n✅ Generated {len(df):,} events")
print(f"💾 Saved to: {filename}")
print(f"\n📊 Dataset summary:")
print(f"   - Indoor events: {len(df[df['out/in'] == 'In']):,}")
print(f"   - Outdoor events: {len(df[df['out/in'] == 'Out']):,}")
print(f"   - Date range: {df['noted_date'].min()} to {df['noted_date'].max()}")
print(f"\n🔥 Built-in drift scenarios:")
print(f"   - Indoor: DRIFT from day 60-80 (AC broken, temps rise from 22°C to 32°C)")
print(f"   - Outdoor: Gradual seasonal warming over 4 months")
print(f"\n📋 Next steps:")
print(f"   1. Replace your IOT-temp.csv with {filename}")
print(f"   2. Run: python3 producer.py")
print(f"   3. Run: python3 kafka_to_clickhouse.py")
print(f"   4. Run: python3 compute_baseline.py")
print(f"   5. Run: python3 drift_detector.py")
print(f"   6. You WILL see drift events! 🎯")