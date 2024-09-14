import os
import glob
import shutil

temp_dir = r'C:\Users\iesha\AppData\Local\Temp\spark-*'

for temp_file in glob.glob(temp_dir):
    try:
        shutil.rmtree(temp_file)
        print(f"Deleted: {temp_file}")
    except Exception as e:
        print(f"Failed to delete {temp_file}: {e}")
