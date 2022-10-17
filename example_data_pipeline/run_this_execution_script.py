import os
import time

# [ User Level Data ] - Raw -> Processed
os.system("python3 1_2_create_raw_user_level_data.py") 
os.system("python3 4_3_process_user_level_data.py") 

# [ User Data ] 

# Create Data Table & Insert Raw Data 
insert_data_scripts = ['1_1_create_raw_user_data','2_1_insert_raw_data','2_2_insert_raw_data','2_3_insert_raw_data']

for insert_data_script in insert_data_scripts:
    os.system("python3" + insert_data_script + ".py") 
    time.sleep(3)

    # Update Pre-Processed Data Table 
    os.system("python3 3_create_preprocess_data.py") 
    time.sleep(3)

    # Create/Insert/Update Processed Data
    os.system("python3 4_1_process_user_data.py") 
    time.sleep(3)

    os.system("python3 4_2_process_user_data.py") # update using spark sql delta
    time.sleep(3)

    # Merge Processed Data & Update Final Data
    os.system("python3 5_merge_table.py") 
    time.sleep(3)

# # Update Pre-Processed Data Table 
# os.system("python3 3_create_preprocess_data.py") 

# # Create/Insert/Update Processed Data
# os.system("python3 4_1_process_user_data.py") 
# os.system("python3 4_2_process_user_data.py") # update using spark sql delta

# # Merge Processed Data & Update Final Data
# os.system("python3 5_merge_table.py") 

# Print All Table
os.system("python3 6_get_all_table.py") 






