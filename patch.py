import sys
import re

file_path = '/home/ricky_ubuntu/SEM6/dm/Proj/core/execution/operators.py'
with open(file_path, 'r') as f:
    content = f.read()

# Replace the Local values block
content = re.sub(
    r'(?m)^[ \t]+# Local values\n(?:[ \t]+if\(func=="COUNT"\):\n)?[ \t]+w_count = len\(values\)\n(?:[ \t]+elif\(func=="SUM"\):\n|[ \t]+if\(func=="SUM"\):\n)',
    '                    # Local values\n                    w_count = len(values)\n                    if(func=="SUM"):\n',
    content
)

# Replace the Merge block
content = re.sub(
    r'(?m)^[ \t]+# Merge\n[ \t]*(?:if\(func=="COUNT"\):\n[ \t]+|)prev_count = state_data\.get\(f"\{state_key\}_count", 0\)\n[ \t]+new_count = prev_count \+ w_count\n[ \t]*(?:elif|if)\(func=="SUM"\):\n',
    '                    # Merge\n                    prev_count = state_data.get(f"{state_key}_count", 0)\n                    new_count = prev_count + w_count\n                    if(func=="SUM"):\n',
    content
)

# Replace the Update state block
content = re.sub(
    r'(?m)^[ \t]+# Update state\n[ \t]*(?:if\(func=="COUNT"\):\n[ \t]+|)state_data\[f"\{state_key\}_count"\] = new_count\n[ \t]*(?:elif|if)\(func=="SUM"\):\n',
    '                    # Update state\n                    state_data[f"{state_key}_count"] = new_count\n                    if(func=="SUM"):\n',
    content
)

# Replace the return block
content = content.replace("elif func == 'AVG': result_val = new_sum / new_count", "elif func == 'AVG': result_val = new_avg")

with open(file_path, 'w') as f:
    f.write(content)

print("Patched successfully!")
