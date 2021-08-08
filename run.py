import os
import subprocess

# --------------------------------------------------------------------------------
# Translate cc3m train dataset on GCP VMs with uploading to a Google Storage bucket

hostname = subprocess.check_output('hostname', shell=True).decode('UTF-8')
assert hostname.startswith('cc3m-data-')
idx = int(hostname.replace('cc3m-data-', ''))
idx = idx - 1

start = idx * 100000
end = (idx + 1) * 100000

bucket_name = '[...]'
blob_prefix = '[...]'
upload_batch_size = 10000

command = f'nohup python -u cc3m_processing.py --input_dir "./" --input_fn "cc3m_train.jsonl" --inf {start} --sup {end} --output_dir "./" --batch_size 100 --buf_size 100 --bucket_name {bucket_name} --blob_prefix {blob_prefix} --upload_batch_size {upload_batch_size}'
print(command)
os.system(command)

# --------------------------------------------------------------------------------
# Translate cc3m valid dataset locally without uploading

start = 0
end = 15840
command = f'python cc3m_processing.py --input_dir "./" --input_fn "cc3m_valid.jsonl" --inf {start} --sup {end} --output_dir "./" --batch_size 100 --buf_size 100'
print(command)
os.system(command)

# --------------------------------------------------------------------------------
