import os
import sys

print("Python version:", sys.version)
print("Current directory:", os.getcwd())

input_dir = "D:/FARES Clean/2024/q1"
print("Checking directory:", input_dir)
print("Directory exists:", os.path.exists(input_dir))

if os.path.exists(input_dir):
    print("\nListing directory contents:")
    for item in os.listdir(input_dir):
        print("-", item) 