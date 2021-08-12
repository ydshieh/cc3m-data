import csv
import json
import os

import datasets
import pandas as pd
import numpy as np


class ImageCaptionBuilderConfig(datasets.BuilderConfig):

    def __init__(self, name, splits, langs, prefix_before_image_fn=False, zfill=1, **kwargs):

        super().__init__(name, **kwargs)

        self.splits = splits
        self.langs = langs
        self.prefix_before_image_fn = prefix_before_image_fn
        self.zfill = zfill


# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """\
@InProceedings{None,
    title = {Generic images to captions dataset},
    author={Yih-Dar SHIEH},
    year={2020}
}
"""

# TODO: Add description of the dataset here
# You can copy an official description
_DESCRIPTION = """\

"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

# TODO: Add link to the official dataset URLs here
# The HuggingFace dataset library don't host the datasets but only point to the original files
# This can be an arbitrary nested dict/list of URLs (see below in `_split_generators` method)
_URLs = {}


# TODO: Name of the dataset usually match the script name with CamelCase instead of snake_case
class ImageCaptionDataset(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = datasets.Version("0.0.0")

    BUILDER_CONFIG_CLASS = ImageCaptionBuilderConfig
    BUILDER_CONFIGS = [
        ImageCaptionBuilderConfig(name='coco_2017', splits=['train', 'valid'], prefix_before_image_fn=False, zfill=12, langs=['en', 'fr']),
        ImageCaptionBuilderConfig(name='cc3m', splits=['train', 'valid'], prefix_before_image_fn=True, zfill=8, langs=['en', 'fr']),
        ImageCaptionBuilderConfig(name='cc12m', splits=['train', 'valid'], prefix_before_image_fn=True, zfill=8, langs=['en', 'fr'])
    ]
    DEFAULT_CONFIG_NAME = "coco_2017"

    def _info(self):
        # TODO: This method specifies the datasets.DatasetInfo object which contains informations and typings for the dataset

        feature_dict = {
            "image_id": datasets.Value("int64"),
            "id": datasets.Value("int64"),
            "caption": datasets.Value("string"),
        }
        for lang in self.config.langs:
            feature_dict[lang] = datasets.Value("string")
        feature_dict["image_url"] = datasets.Value("string")
        feature_dict["image_file"] = datasets.Value("string")

        features = datasets.Features(feature_dict)

        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=features,  # Here we define them above because they are different between the two configurations
            # If there's a common (input, target) tuple from the features,
            # specify them here. They'll be used if as_supervised=True in
            # builder.as_dataset.
            supervised_keys=None,
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        """Returns SplitGenerators."""
        # TODO: This method is tasked with downloading/extracting the data and defining the splits depending on the configuration
        # If several configurations are possible (listed in BUILDER_CONFIGS), the configuration selected by the user is in self.config.name

        data_dir = self.config.data_dir

        splits = []
        for split in self.config.splits:
            if split == 'train':
                dataset = datasets.SplitGenerator(
                    name=datasets.Split.TRAIN,
                    # These kwargs will be passed to _generate_examples
                    gen_kwargs={
                        "jsonl_file": os.path.join(data_dir, f'{self.config.name}_translated_train.jsonl'),
                        "image_dir": os.path.join(data_dir, f'{self.config.name}_images', f'{self.config.name}_train'),
                        "split": "train",
                    }
                )
            elif split in ['val', 'valid', 'validation', 'dev']:
                dataset = datasets.SplitGenerator(
                    name=datasets.Split.VALIDATION,
                    # These kwargs will be passed to _generate_examples
                    gen_kwargs={
                        "jsonl_file": os.path.join(data_dir, f'{self.config.name}_translated_valid.jsonl'),
                        "image_dir": os.path.join(data_dir, f'{self.config.name}_images', f'{self.config.name}_valid'),
                        "split": "valid",
                    },
                )
            elif split == 'test':
                dataset = datasets.SplitGenerator(
                    name=datasets.Split.TEST,
                    # These kwargs will be passed to _generate_examples
                    gen_kwargs={
                        "jsonl_file": os.path.join(data_dir, f'{self.config.name}_translated_test.jsonl'),
                        "image_dir": os.path.join(data_dir, f'{self.config.name}_images', f'{self.config.name}_test'),
                        "split": "test",
                    },
                )
            else:
                continue

            splits.append(dataset)

        return splits

    def _generate_examples(
        # method parameters are unpacked from `gen_kwargs` as given in `_split_generators`
        self, jsonl_file, image_dir, split
    ):
        """ Yields examples as (key, example) tuples. """
        # This method handles input defined in _split_generators to yield (key, example) tuples from the dataset.
        # The `key` is here for legacy reason (tfds) and is not important in itself.

        if split == 'dev':
            split = 'valid'

        with open(jsonl_file, 'r', encoding='UTF-8') as fp:

            for id_, line in enumerate(fp):

                ex = json.loads(line)

                example = {
                    "image_id": ex['image_id'],
                    "id": ex["id"],
                    "caption": ex["caption"],
                }

                for lang in self.config.langs:
                    example[lang] = ex[lang]

                if 'image_url' in ex:
                    example['image_url'] = ex['image_url']
                else:
                    example['image_url'] = ''

                fn = f'{str(ex["image_id"]).zfill(self.config.zfill)}.jpg'
                if self.config.prefix_before_image_fn:
                    fn = f'{self.config.name}_{split}_' + fn

                image_file = os.path.join(image_dir, fn)
                example['image_file'] = image_file

                yield id_, example
