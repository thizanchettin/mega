import os

env = os.environ.get("ENV", "unknown")

print("Hello from Databricks!")
print(f"Running in environment: {env}")
