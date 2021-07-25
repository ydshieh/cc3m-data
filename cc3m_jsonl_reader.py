import json

input_fn = 'cc3m_train.jsonl'

with open(input_fn, 'r', encoding='UTF-8') as fp:
    for idx, jsonl in enumerate(fp):

        print(json.dumps(json.loads(jsonl), ensure_ascii=False, indent=4))
        print('-' * 120)

        if (idx + 1) >= 2000:
            break

    import time
    time.sleep(10000)