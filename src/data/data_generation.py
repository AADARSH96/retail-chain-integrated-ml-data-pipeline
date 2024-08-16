import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from src.utils.spark_utils import load_config, initialize_spark

# Load configuration
config = load_config()

# Initialize Spark session
spark = initialize_spark(config["spark"]["app_name"], config["spark"]["jdbc_driver_path"])

# Setting random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Extract configuration values
NUM_PRODUCTS = config["data_generation"]["num_products"]
NUM_STORES = config["data_generation"]["num_stores"]
NUM_CUSTOMERS = config["data_generation"]["num_customers"]
NUM_YEARS = config["data_generation"]["num_years"]
START_YEAR = config["data_generation"]["start_year"]
END_YEAR = START_YEAR + NUM_YEARS - 1
DATE_RANGE = pd.date_range(f"{START_YEAR}-01-01", f"{END_YEAR}-12-31")

# Define product names and brands for each category
product_names = {
    "Electronics": ["Smartphone", "Laptop", "Tablet", "Smartwatch", "Camera", "Headphones", "Bluetooth Speaker",
                    "Television", "Monitor", "Gaming Console"],
    "Groceries": ["Milk", "Bread", "Eggs", "Butter", "Cheese", "Chicken Breast", "Ground Beef", "Apple", "Banana",
                  "Orange"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sweater", "Dress", "Skirt", "Shoes", "Sneakers", "Hat", "Socks"],
    "Home": ["Sofa", "Dining Table", "Chair", "Bed", "Wardrobe", "Lamp", "Curtains", "Carpet", "Cookware Set",
             "Coffee Maker"],
    "Toys": ["Lego Set", "Doll", "Action Figure", "Board Game", "Puzzle", "Toy Car", "Stuffed Animal",
             "Building Blocks", "Remote Control Car", "Bicycle"]
}

brands = {
    "Electronics": ["Samsung", "Apple", "Sony", "LG", "Dell", "HP", "Bose", "Canon", "GoPro", "Microsoft"],
    "Groceries": ["Organic Valley", "Wonder Bread", "Land O'Lakes", "Sargento", "Tyson", "Johnsonville", "Chiquita",
                  "Dole", "Sunkist", "Florida's Natural"],
    "Clothing": ["Nike", "Adidas", "Levi's", "North Face", "Gap", "Old Navy", "Puma", "Under Armour", "H&M", "Uniqlo"],
    "Home": ["Ikea", "Ashley", "La-Z-Boy", "Pottery Barn", "West Elm", "Bed Bath & Beyond", "Target", "Wayfair",
             "Williams-Sonoma", "Breville"],
    "Toys": ["Lego", "Mattel", "Hasbro", "Ravensburger", "Fisher-Price", "Hot Wheels", "Ty", "Melissa & Doug", "Nerf",
             "Schwinn"]
}

first_names = ["John", "Jane", "Chris", "Katie", "Michael", "Jessica", "Daniel", "Ashley", "David", "Emily"]
last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"]
domains = ["example.com", "mail.com", "test.com", "demo.com"]

# Define store locations globally
store_locations = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL",
    "Fort Worth, TX", "Columbus, OH", "San Francisco, CA", "Charlotte, NC",
    "Indianapolis, IN", "Seattle, WA", "Denver, CO", "Washington, DC",
    "Boston, MA", "El Paso, TX", "Detroit, MI", "Nashville, TN",
    "Portland, OR", "Memphis, TN", "Oklahoma City, OK", "Las Vegas, NV",
    "Louisville, KY", "Baltimore, MD", "Milwaukee, WI", "Albuquerque, NM",
    "Tucson, AZ", "Fresno, CA", "Sacramento, CA", "Long Beach, CA",
    "Kansas City, MO", "Mesa, AZ", "Virginia Beach, VA", "Atlanta, GA",
    "Colorado Springs, CO", "Omaha, NE", "Raleigh, NC", "Miami, FL",
    "Oakland, CA", "Minneapolis, MN", "Tulsa, OK", "Wichita, KS",
    "New Orleans, LA", "Arlington, TX"
]

def generate_product_data():
    product_ids = [f"P{str(i).zfill(5)}" for i in range(NUM_PRODUCTS)]
    categories = list(product_names.keys())
    product_data = []
    for pid in product_ids:
        category = random.choice(categories)
        product_name = random.choice(product_names[category])
        brand_name = random.choice(brands[category])
        subcategory = random.choice(product_names[category])
        price = round(random.uniform(5.0, 500.0), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)
        product_data.append(
            [pid, product_name, brand_name, category, subcategory, price, cost, f"SUP{random.randint(1, 50)}"])
    return pd.DataFrame(product_data,
                        columns=["Product_ID", "Product_Name", "Brand_Name", "Category", "Subcategory", "Price", "Cost",
                                 "Supplier_ID"])


def generate_store_data():
    store_ids = [f"S{str(i).zfill(3)}" for i in range(NUM_STORES)]
    store_types = ["Warehouse", "Retail Outlet"]
    store_data = []
    for sid, location in zip(store_ids, store_locations):
        store_size = random.uniform(5000, 20000)
        store_type = random.choice(store_types)
        store_data.append([sid, location, store_size, store_type])
    return pd.DataFrame(store_data, columns=["Store_ID", "Store_Location", "Store_Size", "Store_Type"])


def generate_customer_data():
    customer_data = []
    existing_customer_ids = set()

    for year in range(START_YEAR, END_YEAR + 1):
        new_customer_ids = [f"C{str(i + year * NUM_CUSTOMERS).zfill(4)}" for i in
                            range(int(NUM_CUSTOMERS * 0.6))]  # 60% new customers each year
        num_existing_customers = len(existing_customer_ids)
        if num_existing_customers > 0:
            retained_customer_ids = random.sample(list(existing_customer_ids), min(int(NUM_CUSTOMERS * 0.4),
                                                                                   num_existing_customers))  # 40% retained customers from previous years
        else:
            retained_customer_ids = []
        year_customer_ids = new_customer_ids + retained_customer_ids
        existing_customer_ids.update(year_customer_ids)

        for cid in year_customer_ids:
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"
            phone = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            address = f"{random.randint(100, 9999)} {random.choice(['Main St', 'Second St', 'Third St', 'Oak St', 'Pine St'])}"
            city = random.choice(store_locations).split(",")[0]
            state = random.choice(store_locations).split(",")[1].strip()
            zip_code = f"{random.randint(10000, 99999)}"
            join_date = datetime(year, random.randint(1, 12), random.randint(1, 28))

            # Ensure DOB is realistic (at least 18 years before the join date)
            age_at_join = random.randint(18, 70)  # Customers are between 18 and 70 years old when they join
            dob = join_date - timedelta(days=age_at_join * 365 + random.randint(0, 365))

            # Random gender
            gender = random.choice(["Male", "Female", "Other"])

            customer_data.append(
                [cid, first_name, last_name, email, phone, address, city, state, zip_code, join_date, dob, gender])

    return pd.DataFrame(customer_data,
                        columns=["Customer_ID", "First_Name", "Last_Name", "Email", "Phone", "Address", "City", "State",
                                 "Zip_Code", "Customer_Join_Date", "DOB", "Gender"])

def generate_time_data():
    time_data = []
    for single_date in DATE_RANGE:
        day_of_week = single_date.strftime("%A")
        week_of_year = single_date.isocalendar()[1]
        month = single_date.strftime("%B")
        quarter = f"Q{((single_date.month - 1) // 3) + 1}"
        year = single_date.year
        time_data.append([single_date, day_of_week, week_of_year, month, quarter, year])
    return pd.DataFrame(time_data, columns=["Date", "Day_of_Week", "Week_of_Year", "Month", "Quarter", "Year"])


def generate_sales_data(product_df, customer_ids):
    sales_data = []
    for single_date in DATE_RANGE:
        day_of_week = single_date.strftime("%A")
        month = single_date.strftime("%B")

        # Adjust daily transaction volume based on the day of the week
        if day_of_week in ["Saturday", "Sunday"]:
            daily_transactions = random.randint(100, 300)  # Higher sales on weekends
        else:
            daily_transactions = random.randint(50, 150)

        for _ in range(daily_transactions):
            transaction_id = f"T{str(len(sales_data) + 1).zfill(7)}"
            product_id = random.choice(product_df['Product_ID'].tolist())
            store_id = random.choice(store_locations)
            customer_id = random.choice(customer_ids)

            # Adjust quantity sold based on the season (e.g., higher sales in December)
            if month in ["November", "December"]:
                quantity_sold = random.randint(2, 15)  # Higher sales during holiday season
            else:
                quantity_sold = random.randint(1, 10)

            sales_amount = quantity_sold * product_df.loc[product_df['Product_ID'] == product_id, 'Price'].values[0]
            sales_data.append(
                [transaction_id, product_id, store_id, customer_id, single_date, quantity_sold, round(sales_amount, 2)])
    return pd.DataFrame(sales_data,
                        columns=["Transaction_ID", "Product_ID", "Store_ID", "Customer_ID", "Date", "Quantity_Sold",
                                 "Sales_Amount"])

def generate_supplier_data():
    supplier_ids = list(set([f"SUP{str(random.randint(1, 50))}" for _ in range(50)]))
    supplier_data = []
    for supplier_id in supplier_ids:
        supplier_name = f"Supplier_{supplier_id}"
        contact_number = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        email = f"contact@{supplier_name.lower()}.com"
        lead_time_days = random.randint(7, 30)
        supplier_data.append([supplier_id, supplier_name, contact_number, email, lead_time_days])
    return pd.DataFrame(supplier_data,
                        columns=["Supplier_ID", "Supplier_Name", "Contact_Number", "Email", "Lead_Time_Days"])


def generate_feedback_data(sales_df):
    feedback_text_to_rating = config["feedback"]["texts"]

    feedback_data = []
    for _, row in sales_df.iterrows():
        if random.random() < config["feedback"]["chance"]:  # 10% chance of feedback
            feedback_id = f"FB{str(len(feedback_data) + 1).zfill(7)}"
            feedback_date = row['Date'] + timedelta(days=random.randint(1, 30))
            feedback_text = random.choice(list(feedback_text_to_rating.keys()))
            feedback_rating = feedback_text_to_rating[feedback_text]
            feedback_data.append(
                [feedback_id, row['Product_ID'], row['Customer_ID'], feedback_date, feedback_text, feedback_rating])
    return pd.DataFrame(feedback_data, columns=["Feedback_ID", "Product_ID", "Customer_ID", "Date", "Feedback_Text",
                                                "Feedback_Rating"])


def calculate_loyalty_points(sales_df):
    # Calculate Loyalty Points based on Purchase History
    sales_summary = sales_df.groupby("Customer_ID")["Sales_Amount"].sum().reset_index()
    sales_summary["Points_Earned"] = sales_summary["Sales_Amount"]
    sales_summary["Points_Redeemed"] = sales_summary["Points_Earned"].apply(lambda x: random.randint(0, int(x * 0.5)))
    sales_summary["Membership_Tier"] = sales_summary["Points_Earned"].apply(
        lambda x: "Silver" if x < 75000 else "Gold" if x < 80000 else "Platinum")

    # Generate Loyalty Program Data
    loyalty_data = []
    for _, row in sales_summary.iterrows():
        loyalty_id = f"LOY{str(len(loyalty_data) + 1).zfill(7)}"
        loyalty_data.append(
            [loyalty_id, row["Customer_ID"], row["Points_Earned"], row["Points_Redeemed"], row["Membership_Tier"]])

    return pd.DataFrame(loyalty_data,
                        columns=["Loyalty_ID", "Customer_ID", "Points_Earned", "Points_Redeemed", "Membership_Tier"])


def main():
    # Generate data
    print("Generating Product Data...")
    product_df = generate_product_data()

    print("Generating Store Data...")
    store_df = generate_store_data()

    print("Generating Customer Data...")
    customer_df = generate_customer_data()

    print("Generating Time Data...")
    time_df = generate_time_data()

    print("Generating Sales Data...")
    sales_df = generate_sales_data(product_df, customer_df['Customer_ID'].tolist())

    print("Generating Supplier Data...")
    supplier_df = generate_supplier_data()

    print("Generating Feedback Data...")
    feedback_df = generate_feedback_data(sales_df)

    print("Calculating Loyalty Points based on Purchase History...")
    loyalty_df = calculate_loyalty_points(sales_df)

    print("Converting DataFrames to Spark DataFrames...")
    # Convert pandas DataFrames to Spark DataFrames
    product_sdf = spark.createDataFrame(product_df)
    store_sdf = spark.createDataFrame(store_df)
    customer_sdf = spark.createDataFrame(customer_df)
    time_sdf = spark.createDataFrame(time_df)
    sales_sdf = spark.createDataFrame(sales_df)
    supplier_sdf = spark.createDataFrame(supplier_df)
    feedback_sdf = spark.createDataFrame(feedback_df)
    loyalty_sdf = spark.createDataFrame(loyalty_df)

    # Return all Spark DataFrames
    return {
        "product_sdf": product_sdf,
        "store_sdf": store_sdf,
        "customer_sdf": customer_sdf,
        "time_sdf": time_sdf,
        "sales_sdf": sales_sdf,
        "supplier_sdf": supplier_sdf,
        "feedback_sdf": feedback_sdf,
        "loyalty_sdf": loyalty_sdf
    }


if __name__ == "__main__":
    dataframes = main()
    print("Data generation complete.")
