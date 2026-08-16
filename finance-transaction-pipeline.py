# Databricks notebook source
import random
from datetime import date, timedelta

random.seed(42)  # keeps the "random" data same every run - good for testing

# ---- Sample values to pick from ----
first_names = ["Rahul", "Priya", "Amit", "Sneha", "Vikram", "Anjali", "Karan", "Divya", "Arjun", "Neha"]
last_names = ["Sharma", "Verma", "Patel", "Reddy", "Nair", "Iyer", "Singh", "Das", "Menon", "Kapoor"]
cities = ["Chennai", "Bengaluru", "Mumbai", "Delhi", "Hyderabad"]

customers_data = []

for cid in range(1, 151):  # 150 customers
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    name = f"{fn} {ln}"
    
    # inject messy formatting on ~15% of rows
    if random.random() < 0.15:
        name = f"  {name.upper()}  "
    
    # inject null city on some rows, otherwise pick a real city
    city = random.choice(cities) if random.random() > 0.1 else None
    
    signup_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
    
    customers_data.append((cid, name, city, signup_date.isoformat()))

customers_df = spark.createDataFrame(
    customers_data, ["customer_id", "customer_name", "city", "signup_date"]
)

display(customers_df.limit(10))
print("Total customers:", customers_df.count())

# COMMAND ----------

import random
import builtins

random.seed(43)

branches = ["Chennai Main", "T Nagar", "Anna Nagar", "Velachery", "OMR"]
account_types = ["Savings", "Current", "Credit"]

accounts_data = []
account_id = 1

for customer_id in range(1, 151):  # loop through all 150 customers
    num_accounts = random.randint(1, 3)  # each customer gets 1-3 accounts
    
    for _ in range(num_accounts):
        # account_type: ~10% chance of null
        account_type = random.choice(account_types) if random.random() > 0.1 else None
        
        # balance: normally between 500 and 200000, but force a few negative ones
        balance = builtins.round(random.uniform(500, 200000), 2)
        if random.random() < 0.02:  # ~2% of rows get a bad negative balance
            balance = builtins.round(random.uniform(-5000, -100), 2)
        
        branch = random.choice(branches)
        
        accounts_data.append((account_id, customer_id, account_type, balance, branch))
        account_id += 1

accounts_df = spark.createDataFrame(
    accounts_data, ["account_id", "customer_id", "account_type", "balance", "branch"]
)

accounts_df.show(10)
print("Total accounts:", accounts_df.count())

# COMMAND ----------

import random
import builtins
from datetime import date, timedelta

random.seed(44)

transaction_types = ["Debit", "Credit", "Transfer"]
merchant_categories = ["Groceries", "Fuel", "Dining", "Shopping", "Bills", "Entertainment"]

# grab the real account_ids you generated (not a hardcoded range)
account_ids = [row.account_id for row in accounts_df.select("account_id").collect()]

transactions_data = []
transaction_id = 1
start_date = date(2025, 1, 1)

for acc_id in account_ids:
    num_transactions = random.randint(0, 10)  # 0-10 transactions per account
    
    for _ in range(num_transactions):
        txn_type = random.choice(transaction_types)
        merchant = random.choice(merchant_categories)
        
        # amount: normally 50-15000, but ~5% chance of null
        amount = builtins.round(random.uniform(50, 15000), 2) if random.random() > 0.05 else None
        
        txn_date = start_date + timedelta(days=random.randint(0, 364))
        
        transactions_data.append((transaction_id, acc_id, txn_type, amount, txn_date.isoformat(), merchant))
        transaction_id += 1

# inject a few orphan transactions - account_id that doesn't exist in accounts_df
for fake_acc_id in [9999, 9998, 9997]:
    txn_date = start_date + timedelta(days=random.randint(0, 364))
    transactions_data.append((
        transaction_id, fake_acc_id, "Debit", 1200.0, txn_date.isoformat(), "Shopping"
    ))
    transaction_id += 1

transactions_df = spark.createDataFrame(
    transactions_data,
    ["transaction_id", "account_id", "transaction_type", "amount", "transaction_date", "merchant_category"]
)

transactions_df.show(10)
print("Total transactions:", transactions_df.count())

# COMMAND ----------

def profile_dataframe(df, name):
    print(f"\n===== PROFILE: {name} =====")
    print("Row count:", df.count())
    print("Duplicate rows:", df.count() - df.dropDuplicates().count())
    
    for col_name in df.columns:
        null_count = df.filter(df[col_name].isNull()).count()
        distinct_count = df.select(col_name).distinct().count()
        print(f"  {col_name:20s} | nulls: {null_count:5d} | distinct: {distinct_count}")

# now call it for each of your 3 DataFrames
profile_dataframe(customers_df, "customers_df")
profile_dataframe(accounts_df, "accounts_df")
profile_dataframe(transactions_df, "transactions_df")

# COMMAND ----------

from pyspark.sql import functions as F

# ---- Clean customers_df ----
customers_clean = (
    customers_df
    .dropDuplicates()
    .withColumn("customer_name", F.trim(F.initcap(F.col("customer_name"))))
    .withColumn("city", F.when(F.col("city").isNull(), "Unknown").otherwise(F.col("city")))
)

# ---- Clean accounts_df ----
accounts_clean = (
    accounts_df
    .withColumn("account_type", F.when(F.col("account_type").isNull(), "Unknown").otherwise(F.col("account_type")))
    .filter(F.col("balance") >= 0)  # drop bad negative balances
)

# ---- Clean transactions_df ----
valid_account_ids = accounts_clean.select("account_id")

transactions_clean = (
    transactions_df
    .filter(F.col("amount").isNotNull())
    .join(F.broadcast(valid_account_ids), on="account_id", how="inner")  # drops orphan account_ids
)

print("customers_clean:", customers_clean.count())
print("accounts_clean:", accounts_clean.count())
print("transactions_clean:", transactions_clean.count())

customers_clean.show(10)

# COMMAND ----------

dq_results = []

def dq_check(name, passed, detail=""):
    dq_results.append((name, "PASS" if passed else "FAIL", detail))

# Check 1: no nulls in key columns
null_key_count = customers_clean.filter(
    F.col("customer_id").isNull() | F.col("customer_name").isNull()
).count()
dq_check("no nulls in customer key columns", null_key_count == 0, f"{null_key_count} bad rows")

# Check 2: no duplicate customer_ids
dup_customers = customers_clean.groupBy("customer_id").count().filter("count > 1").count()
dq_check("no duplicate customer_ids", dup_customers == 0, f"{dup_customers} duplicates")

# Check 3: referential integrity - every transaction's account_id exists in accounts_clean
orphan_txns = transactions_clean.join(
    accounts_clean.select("account_id"), on="account_id", how="left_anti"
).count()
dq_check("transactions reference valid accounts", orphan_txns == 0, f"{orphan_txns} orphan transactions")

# Check 4: no negative balances
bad_balances = accounts_clean.filter(F.col("balance") < 0).count()
dq_check("no negative balances", bad_balances == 0, f"{bad_balances} negative balances")

# Check 5: amount sanity check
bad_amounts = transactions_clean.filter(F.col("amount") <= 0).count()
dq_check("all transaction amounts > 0", bad_amounts == 0, f"{bad_amounts} bad amounts")

# print results
print(f"{'CHECK':45s} | {'RESULT':6s} | DETAIL")
print("-" * 70)
for name, result, detail in dq_results:
    print(f"{name:45s} | {result:6s} | {detail}")

# COMMAND ----------

# ---- Part A: Join ----
enriched = (
    transactions_clean
    .join(accounts_clean, on="account_id", how="inner")
    .join(customers_clean, on="customer_id", how="inner")
)

enriched.show(5)
print("Enriched row count:", enriched.count())

# ---- Part B1: Spend by branch ----
branch_summary = (
    enriched.groupBy("branch")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount")
    )
    .orderBy(F.col("total_amount").desc())
)

branch_summary.show()

# ---- Part B2: Spend by customer (top 10) ----
customer_summary = (
    enriched.groupBy("customer_id", "customer_name")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_spend")
    )
    .orderBy(F.col("total_spend").desc())
)

customer_summary.show(10)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# Step 1: build city_spend
city_spend = (
    enriched.groupBy("customer_id", "customer_name", "city")
    .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
)

# Step 2: define window spec
window_spec = Window.partitionBy("city").orderBy(F.col("total_spend").desc())

# Step 3: add rank column
ranked = city_spend.withColumn("rank_in_city", F.rank().over(window_spec))

# Step 4: filter top 3 per city
top3_per_city = ranked.filter(F.col("rank_in_city") <= 3).orderBy("city", "rank_in_city")

top3_per_city.show(20)

# COMMAND ----------

branch_ranked = branch_summary.withColumn(
    "revenue_rank", 
    F.dense_rank().over(Window.orderBy(F.col("total_amount").desc()))
)
branch_ranked.show()

# COMMAND ----------

# Pivot 1: transaction count by branch x transaction_type
pivot1 = (
    enriched.groupBy("branch")
    .pivot("transaction_type")
    .agg(F.count("transaction_id"))
    .na.fill(0)
)
pivot1.show()

# Pivot 2: monthly spend by merchant_category
enriched_with_month = enriched.withColumn(
    "month", F.date_format(F.to_date("transaction_date"), "yyyy-MM")
)

pivot2 = (
    enriched_with_month.groupBy("merchant_category")
    .pivot("month")
    .agg(F.round(F.sum("amount"), 2))
    .na.fill(0)
)
pivot2.show()

# COMMAND ----------

# "map" equivalent - just a DataFrame select/transform
amount_pairs = enriched.select("customer_id", "amount")
amount_pairs.show(5)

# "flatMap" equivalent - split names into words, explode into rows, count
word_counts = (
    customers_clean
    .select(F.explode(F.split(F.col("customer_name"), " ")).alias("word"))
    .groupBy("word")
    .count()
    .orderBy(F.col("count").desc())
)
word_counts.show(10)

# COMMAND ----------

enriched.write.mode("overwrite").saveAsTable("enriched_transactions")
branch_summary.write.mode("overwrite").saveAsTable("branch_summary")
top3_per_city.write.mode("overwrite").saveAsTable("top3_per_city")

print("Tables saved. Check Catalog on the left sidebar -> workspace -> default")

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.finance_pipeline")

output_base = "/Volumes/workspace/default/finance_pipeline/curated"

enriched.write.mode("overwrite").parquet(f"{output_base}/enriched_transactions_parquet")
branch_summary.write.mode("overwrite").parquet(f"{output_base}/branch_summary_parquet")

enriched.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_base}/enriched_transactions_csv")
branch_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_base}/branch_summary_csv")

print("Export complete:", output_base)