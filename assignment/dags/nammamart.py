import os
import pandas as pd
import numpy as np

# 1. Setup local project directories matching the project structure
os.makedirs("include/data/nammamart", exist_ok=True)
os.makedirs("include/quarantine", exist_ok=True)
os.makedirs("include/sql", exist_ok=True)
os.makedirs("dags", exist_ok=True)

print(" Project directories created successfully.")

# 2. Generate and write mock data files to mimic NammaMart datasets for profiling
# This ensures the pipeline code can be run and tested immediately inside Colab.
np.random.seed(42)

# Mock Stores
stores_df = pd.DataFrame({
    "store_id": [f"STR_{i:02d}" for i in range(1, 16)],
    "store_name": [f"Store_{i}" for i in range(1, 16)],
    "zone": np.random.choice(["North", "South", "East", "West"], 15),
    "area": [f"Area_{i}" for i in range(1, 16)],
    "pincode": [560000 + np.random.randint(1, 99) for _ in range(15)],
    "open_date": pd.date_range(start="2025-01-01", periods=15, freq="ME").strftime("%Y-%m-%d"),
    "manager_name": [f"Manager_{i}" for i in range(1, 16)],
    "capacity_orders_per_day": np.random.randint(150, 300, 15)
})
stores_df.to_csv("assignment/nammamart_dataset/stores.csv", index=False)

# Mock Products
products_df = pd.DataFrame({
    "product_id": [f"PRD_{i:03d}" for i in range(1, 153)],
    "product_name": [f"Product_{i}" for i in range(1, 153)],
    "category": np.random.choice(["Fruits", "Vegetables", "Dairy", "Bakery", "Beverages", "Snacks"], 152),
    "sub_category": ["SubCat_A" if i % 2 == 0 else "SubCat_B" for i in range(1, 153)],
    "brand": np.random.choice(["Brand_X", "Brand_Y", "Brand_Z"], 152),
    "mrp": np.round(np.random.uniform(20, 500, 152), 2),
    "cost_price": np.round(np.random.uniform(10, 350, 152), 2),
    "unit": np.random.choice(["1kg", "500g", "1L", "Pack"], 152),
    "is_perishable": np.random.choice([True, False], 152),
    "launch_date": pd.date_range(start="2024-01-01", periods=152, freq="D").strftime("%Y-%m-%d")
})
# Introduce validity anomaly: some cost prices > mrp
products_df.loc[0, "cost_price"] = 600.0
products_df.to_csv("assignment/nammamart_dataset/products.csv", index=False)

# Mock Customers
customers_df = pd.DataFrame({
    "customer_id": [f"CST_{i:04d}" for i in range(1, 406)],
    "full_name": [f"Customer {i}" for i in range(1, 406)],
    "email": [f"cust_{i}@gmail.com" for i in range(1, 406)],
    "phone": [f"98765{i:05d}" for i in range(1, 406)],
    "city": "Bengaluru",
    "area": np.random.choice(["Indiranagar", "Jayanagar", "Koramangala", "HSR"], 405),
    "pincode": np.random.choice([560038, 560041, 560034, 560102], 405),
    "signup_date": pd.date_range(start="2025-01-01", periods=405, freq="D").strftime("%Y-%m-%d"),
    "loyalty_tier": np.random.choice(["GOLD", "Silver", "Bronze", "gold", "SILVER"], 405), # Inconsistent casing
    "is_active": np.random.choice([True, False], 405, p=[0.9, 0.1])
})
customers_df.to_csv("assignment/nammamart_dataset/customers.csv", index=False)

# Mock Orders
order_ids = list(range(10001, 12413))
order_ts = pd.date_range(start="2026-09-01", end="2026-09-30 23:59:00", periods=len(order_ids))
quantities = np.random.randint(1, 10, len(order_ids))
unit_prices = np.round(np.random.uniform(20, 450, len(order_ids)), 2)
discount_pcts = np.random.choice([0, 5, 10, 15], len(order_ids))
order_amounts = np.round(quantities * unit_prices * (1 - discount_pcts / 100), 2)

orders_df = pd.DataFrame({
    "order_id": order_ids,
    "order_ts": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
    "customer_id": np.random.choice(customers_df["customer_id"], len(order_ids)),
    "store_id": np.random.choice(stores_df["store_id"], len(order_ids)),
    "product_id": np.random.choice(products_df["product_id"], len(order_ids)),
    "quantity": quantities,
    "unit_price": unit_prices,
    "discount_pct": discount_pcts,
    "order_amount": order_amounts,
    "payment_mode": np.random.choice(["UPI", "Card", "Cash on Delivery", "Wallet", "upi", "UPI "], len(order_ids)),
    "order_status": np.random.choice(["DELIVERED", "CANCELLED", "RETURNED"], len(order_ids), p=[0.8, 0.1, 0.1]),
    "channel": np.random.choice(["App", "Web"], len(order_ids)),
    "delivery_minutes": np.random.randint(8, 45, len(order_ids))
})

# Introduce validation issues (anomalies)
orders_df.loc[0, "quantity"] = -2  # Accuracy rule violation
orders_df.loc[1, "delivery_minutes"] = np.nan  # Completeness rule violation for DELIVERED order
orders_df.loc[1, "order_status"] = "DELIVERED"
orders_df.loc[2, "customer_id"] = "CST_9999" # Referential integrity anomaly
orders_df.to_csv("assignment/nammamart_dataset/orders.csv", index=False)

# Mock Payments
payments_df = pd.DataFrame({
    "payment_id": [f"PMT_{i:05d}" for i in range(1, 2229)],
    "order_id": np.random.choice(orders_df["order_id"], 2228, replace=False),
    "payment_ts": pd.date_range(start="2026-09-01", periods=2228, freq="15min").strftime("%Y-%m-%d %H:%M:%S"),
    "payment_mode": np.random.choice(["UPI", "Card", "Wallet"], 2228),
    "amount_paid": np.round(np.random.uniform(50, 1000, 2228), 2),
    "payment_status": np.random.choice(["SUCCESS", "FAILED"], 2228, p=[0.9, 0.1]),
    "gateway_ref": [f"GW_REF_{i:06d}" for i in range(1, 2229)]
})
payments_df.to_csv("assignment/nammamart_dataset/payments.csv", index=False)

print("✓ Mock data files generated successfully inside 'assignment/nammamart_dataset/'.\n")

# 3. Discovery & Data Profiling Script (FDE Discovery Step)
print("=== NAMMAMART DATA PROFILING REPORT ===\n")
files_to_profile = {
    "orders": "assignment/nammamart_dataset/orders.csv",
    "payments": "assignment/nammamart_dataset/payments.csv",
    "customers": "assignment/nammamart_dataset/customers.csv",
    "products": "assignment/nammamart_dataset/products.csv",
    "stores": "assignment/nammamart_dataset/stores.csv"
}

for name, path in files_to_profile.items():
    df = pd.read_csv(path)
    print(f"File: {name}.csv")
    print(f"  Total Rows: {len(df)}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Null Values:\n{df.isnull().sum().to_dict()}")
    print(f"  Duplicate Keys ({df.columns[0]}): {df.duplicated(subset=[df.columns[0]]).sum()}")
    
    # Display unique values of categorical fields
    for col in df.columns:
        if col in ["order_status", "payment_mode", "loyalty_tier", "zone", "category"]:
            print(f"  Distinct Values in '{col}': {df[col].unique()}")
    print("-" * 50)

# 4. Check Referential Integrity Relationships
orders = pd.read_csv(files_to_profile["orders"])
customers = pd.read_csv(files_to_profile["customers"])
products = pd.read_csv(files_to_profile["products"])
stores = pd.read_csv(files_to_profile["stores"])

invalid_cust = orders[~orders["customer_id"].isin(customers["customer_id"])]
invalid_prd = orders[~orders["product_id"].isin(products["product_id"])]
invalid_str = orders[~orders["store_id"].isin(stores["store_id"])]

print("=== REFERENTIAL INTEGRITY RESULTS ===")
print(f"Orders with invalid customer_id: {len(invalid_cust)}")
print(f"Orders with invalid product_id: {len(invalid_prd)}")
print(f"Orders with invalid store_id: {len(invalid_str)}")
