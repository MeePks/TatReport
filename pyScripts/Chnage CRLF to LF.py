import os
from datetime import datetime

# Get the current date
current_date = datetime.now().strftime("%Y-%m-%d")

# Define input and output directories
input_dir_old = rf'\\ccaintranet.com\dfs-dc-01\Split\Retail\GiantEagle\OracleAp\{current_date}'
input_dir = input_dir_old + '_deleted'
output_dir = input_dir_old

try:
    # Rename the old directory and create a new one
    if not os.path.exists(input_dir_old):
        raise FileNotFoundError(f"Directory '{input_dir_old}' does not exist.")
    
    os.rename(input_dir_old, input_dir)
    print(f"Renamed directory '{input_dir_old}' to '{input_dir}'.")
    
    os.mkdir(input_dir_old)
    print(f"Created a new directory '{input_dir_old}'.")
    
except FileExistsError:
    print(f"Directory '{input_dir_old}' already exists.")
except FileNotFoundError as fnf_error:
    print(fnf_error)
except OSError as os_error:
    print(f"OS error occurred: {os_error}")

try:
    # Rename the old directory and create a new one   
    os.mkdir(input_dir_old)
    print(f"Created a new directory '{input_dir_old}'.")
    
except FileExistsError:
    print(f"Directory '{input_dir_old}' already exists.")
except FileNotFoundError as fnf_error:
    print(fnf_error)
except OSError as os_error:
    print(f"OS error occurred: {os_error}")



try:
    # Process files in the renamed directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)
            
            try:
                # Read and rewrite the file with the desired newline characters
                with open(input_path, 'r', newline='\n') as infile, open(output_path, 'w', newline='\r\n') as outfile:
                    for line in infile:
                        outfile.write(line)
                print(f"Processed file '{filename}' successfully.")
            
            except FileNotFoundError:
                print(f"File '{input_path}' not found.")
            except IOError as io_error:
                print(f"I/O error while processing file '{filename}': {io_error}")
            except Exception as e:
                print(f"Unexpected error while processing file '{filename}': {e}")

except FileNotFoundError as fnf_error:
    print(f"Source directory '{input_dir}' not found: {fnf_error}")
except PermissionError:
    print(f"Permission error accessing directory '{input_dir}'.")
except Exception as e:
    print(f"Unexpected error: {e}")
