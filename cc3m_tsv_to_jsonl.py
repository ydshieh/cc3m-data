import pandas as pd
import json
from copy import deepcopy


default_entry = {
    "image_id": -1,
    "id": -1,
    "caption": None,
    "image_url": None
}


def convert_to_jsonl(chunked_df, output_fn):

    idx = -1
    with open(output_fn, 'w', encoding='UTF-8') as fp:

        for chunk in chunked_df:
            for _, row in list(chunk.iterrows()):

                idx += 1

                entry = deepcopy(default_entry)
                entry['image_id'] = idx
                entry['id'] = idx
                entry['caption'] = row[0]
                entry['image_url'] = row[1]

                jsonl = json.dumps(entry, ensure_ascii=False, indent=None)
                fp.write(jsonl + '\n')

                if (idx + 1) % 10000 == 0:
                    print(idx)
                    print(jsonl)
                    print('-' * 120)


if __name__ == "__main__":

    df = pd.read_csv('cc3m_captions_train.tsv', sep='\t', iterator=True, chunksize=50000, header=None)
    convert_to_jsonl(df, output_fn='cc3m_train.jsonl')

    df = pd.read_csv('cc3m_captions_valid.tsv', sep='\t', iterator=True, chunksize=50000, header=None)
    convert_to_jsonl(df, output_fn='cc3m_valid.jsonl')
