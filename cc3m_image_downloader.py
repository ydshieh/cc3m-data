import sys
import os
from datetime import datetime
from urllib.request import urlopen
import requests
from PIL import Image
from torchvision.transforms import functional as TF
from multiprocessing import Pool
import logging
import json
import shutil
from pathlib import Path

# Setup
tmp_dir = './tmp/'
log_fn = 'cc3m_image_download.log'
if not os.path.isdir(tmp_dir):
    os.makedirs(tmp_dir, exist_ok=True)

logging.basicConfig(filename=os.path.join(log_fn), filemode='w', level=logging.INFO)
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

MAX_IMAGE_SIZE = 512


# Resize function
def resize(img):

    if min(img.size) > MAX_IMAGE_SIZE:
        img = TF.resize(img, size=MAX_IMAGE_SIZE, interpolation=Image.LANCZOS)

    return img


def process(entry, target_dir, fn_prefix):

    image_id, image_url = entry['image_id'], entry['image_url']

    try:

        fn = f'{fn_prefix}_{image_id:08d}.jpg'  # create filename
        f_path = os.path.join(target_dir, fn)  # concat to get filepath
        f_path_temp = Path(os.path.join(tmp_dir, fn))

        if not os.path.isfile(f_path):

            req = requests.get(image_url, stream=True, timeout=2, verify=False)

            # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
            req.raw.decode_content = True

            # check status
            assert req.status_code == 200, "status != 200"

            # Open a local file with wb (write binary) permission.
            with open(f_path_temp, 'wb') as fp:
                shutil.copyfileobj(req.raw, fp)

            with Image.open(f_path_temp).convert('RGB') as image:
                img = resize(image)  # resize PIL image
                img.save(f_path)  # save PIL image

            try:
                f_path_temp.unlink()
            except FileNotFoundError:
                pass

    except Exception as e:
        try:
            f_path_temp.unlink()
        except FileNotFoundError:
            pass
        logging.error(f"image_id: {entry['image_id']}")
        logging.error(image_url)
        logging.error(" ".join(repr(e).splitlines()))
        logging.error("=" * 8)


if __name__ == '__main__':

    split = 'train'

    image_root_dir = './cc3m_images/'
    fn_prefix = f'cc3m_{split}'

    n_processes = 32
    buf_size_per_proc = 1000

    buf_size = buf_size_per_proc * n_processes
    buf = []


    target_dir = os.path.join(image_root_dir, fn_prefix)

    if not os.path.isdir(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    cc3m_jsonl_file_path = f'./cc3m_{split}.jsonl'

    with open(cc3m_jsonl_file_path, 'r', encoding='UTF-8') as fp:

        for line in fp:

            entry = json.loads(line)
            buf.append((entry, target_dir, fn_prefix))

            if len(buf) >= buf_size:

                with Pool(n_processes) as p:
                    r = p.starmap(process, buf)
                    buf = []

        if len(buf) > 0:
            with Pool(n_processes) as p:
                r = p.starmap(process, buf)
                buf = []
