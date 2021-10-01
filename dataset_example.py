from datasets import load_dataset


dataset_name = "image_caption_dataset.py"
dataset_config_name = "coco_2017"
cache_dir = None
keep_in_memory = False
data_dir = "./"

dataset = load_dataset(
    dataset_name, dataset_config_name, cache_dir=cache_dir, keep_in_memory=keep_in_memory, data_dir=data_dir
)

for example in dataset["train"]:
    print(example)
    break

for example in dataset["validation"]:
    print(example)
    break