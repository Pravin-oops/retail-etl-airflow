import csv
import random
import os
from datetime import datetime
from faker import Faker

# --- CONFIGURATION (V1.1 Robust Fix) ---

# 1. Check if we are on Windows
IS_WINDOWS = (os.name == 'nt')

# 2. Define the Docker Path
DOCKER_DIR = '/data'

# 3. Path Selection Logic
# - If Windows: ALWAYS use local project folder (Prevents C:\data confusion)
# - If Linux: Check if /data exists. If yes, assume Docker. If no, use local.
if IS_WINDOWS:
    USE_LOCAL_DIR = True
else:
    # On Linux/Mac, only use /data if it actually exists (Docker environment)
    USE_LOCAL_DIR = not os.path.exists(DOCKER_DIR)

if USE_LOCAL_DIR:
    # Calculate path relative to THIS script file
    # Works regardless of where you run the command from
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Go up one level (../) then into 'data'
    DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'data')
else:
    DATA_DIR = DOCKER_DIR

# Format: sales_data_DDMMYYYY.csv (e.g., sales_data_01012026.csv)
date_suffix = datetime.now().strftime("%d%m%Y")
filename_dated = f'sales_data_{date_suffix}.csv'
OUTPUT_FILE = os.path.join(DATA_DIR, filename_dated)

# Ensure the directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize Faker
fake = Faker()
NUM_ROWS = 1000

print(f"--- Starting Data Generation (V2) ---")
print(f"OS Detected: {'Windows' if IS_WINDOWS else 'Linux/Mac'}")
print(f"Target Path: {OUTPUT_FILE}")  # <--- Verify this line!

# Define Product Categories
CATEGORIES = ['Electronics', 'Home', 'Office', 'Books', 'Garden']
PRODUCTS = {
    'Electronics': ['Wireless Mouse', 'Gaming Monitor', 'USB-C Cable', 'Mechanical Keyboard'],
    'Home': ['Blender', 'Desk Lamp', 'Throw Pillow', 'Picture Frame'],
    'Office': ['Stapler', 'Whiteboard', 'Ballpoint Pens', 'File Organizer'],
    'Books': ['Python 101', 'History of Rome', 'Cooking for Beginners', 'Sci-Fi Novel'],
    'Garden': ['Shovel', 'Plant Pot', 'Garden Hose', 'Rake']
}

with open(OUTPUT_FILE, 'w', newline='') as f:
    writer = csv.writer(f)
    
    # HEADER
    writer.writerow(['TRANS_ID', 'CUST_ID', 'CUST_NAME', 'PROD_ID', 'PROD_NAME', 'CATEGORY', 'PRICE', 'QUANTITY', 'TXN_DATE'])
    
    for i in range(1000, 1000 + NUM_ROWS):
        # 5% Chance of NULL Category
        cat = random.choice(CATEGORIES) if random.random() > 0.05 else ''
        prod_name = random.choice(PRODUCTS.get(cat, ['Generic Item']))
        
        # 2% Chance of Negative Price
        price = round(random.uniform(5.0, 500.0), 2)
        if random.random() < 0.02:
            price = price * -1 
            
        # 5% Chance of Future Date
        if random.random() < 0.05:
            txn_date = fake.future_date()
        else:
            txn_date = fake.date_between(start_date='-1y', end_date='today')

        writer.writerow([
            i,
            f"C{random.randint(1, 100):03d}",
            fake.name(),
            f"P{random.randint(1, 50):03d}",
            prod_name,
            cat,
            price,
            random.randint(1, 10),
            txn_date
        ])

print(f"✅ Success! Generated {NUM_ROWS} rows.")