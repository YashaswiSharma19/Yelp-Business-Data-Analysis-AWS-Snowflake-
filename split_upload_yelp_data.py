#!/usr/bin/env python
# coding: utf-8

# In[19]:


import json


# In[20]:


input_file="C:\\Users\\LENOVO\\Downloads\\datapipeline_project\\yelp_academic_dataset_review.json"


# In[21]:


output_prefix="D:\complete DAP"
num_files=20

#count total lines in the file
with open(input_file, "r",encoding="utf8") as f:
    total_lines= sum(1 for _ in f)
    
lines_per_file= total_lines//num_files
print(f"total lines: {total_lines}, lines per file: {lines_per_file}")

#split into multiple smaller files
with open(input_file, "r",encoding="utf8") as f:
    for i in range(num_files):
        output_filename = f"{output_prefix}+{i+1}.json"
        
        with open(output_filename, "w", encoding="utf8") as output_file:
            for j in range(lines_per_file):
                line=f.readline()
                if not line:
                    break # stop if file ends early
                    
                output_file.write(line)
                
print("JSON file successfully split into smaller parts")

