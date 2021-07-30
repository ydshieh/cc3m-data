import os
import json
from copy import deepcopy
from shutil import copyfile
from googletrans import Translator
import time
import re


translator = Translator()
to_remove = ['.', '?', ',', '!', '&', '_', '-', '=', ';', ':', '(', ')', '[', ']', '{', '}']

regex = re.compile(r'( \'[^ ]*(?: |$))')
regex_2 = re.compile(r'( \\ \'t(?: |$))')
apostrophe_to_check = {" 'm", " 'll ", " 'm ", " 're", " 's", " 's ", " 'd", " 're ", " 've ", " 've", " 'd ", " 't ", " 't"}

prefix_targets = [
    "a black and white photograph of ",
    "a black and white portrait of ",
    "a black and white picture of ",
    "a black and white photo of ",
    "a black and white image of ",

    "black and white photograph of ",
    "black and white portrait of ",
    "black and white picture of ",
    "black and white photo of ",
    "black and white image of ",

    "an old photograph of ",
    "an old portrait of ",
    "an old picture of ",
    "an old photo of ",
    "an old image of ",

    "old photograph of ",
    "old portrait of ",
    "old picture of ",
    "old photo of ",
    "old image of ",

    "a photograph of ",
    "a portrait of ",
    "a picture of ",
    "a photo of ",
    "an image of ",
    "a view of ",
    "a scene of ",

    "photograph of ",
    "portrait of ",
    "picture of ",
    "photo of ",
    "image of ",
    "view of ",
    "scene of ",

    "a close - up on ",
    "a close-up on ",
    "a close up on ",
    "a closeup on ",

    "close - up on ",
    "close-up on ",
    "close up on ",
    "closeup on ",

    "a close - up of ",
    "a close-up of ",
    "a close up of ",
    "a closeup of ",

    "close - up of ",
    "close-up of ",
    "close up of ",
    "closeup of ",

    "close - up ",
    "close-up ",
    "close up ",
    "closeup ",

    "a couple of ",
    "a group of ",
    "a bunch of ",

    "there are ",
    "there is ",

    "these are ",
    "this is ",

    "a lot of ",
    "some of ",
    "several ",
    "a few ",
    "some ",
    "many ",
    "and ",
    "or ",
    "of ",
]

postfix_targets = [
    ' in the background.',
    ' in the background',
    ' in the foreground.',
    ' in the foreground',
    ' in background.',
    ' in background',
    ' in foreground.',
    ' in foreground',
    ' and',
    ' or'
]


def remove_space_before_apostrophe(text):

    def repl(match_obj):

        assert match_obj.group(0)[0] == ' '

        result = match_obj.group(0)
        if result in apostrophe_to_check:
            result = result[1:]

        return result

    modified = re.sub(regex, repl, text)

    return modified


def remove_2(text):

    def repl(match_obj):

        assert match_obj.group(0).startswith(" \\ 't")
        return match_obj.group(0)[len(" \\"):]

    modified = re.sub(regex_2, repl, text)

    return modified


def _process_1_annotation(text):

    text = text.lower().strip()
    text = remove_2(text)

    while '...' in text:
        text = text.replace('...', ' , ').strip()
    while '--' in text:
        text = text.replace('--', ' - ').strip()
    while '  ' in text:
        text = text.replace('  ', ' ').strip()

    # remove trailing '.'
    while text[-1] in to_remove:
        text = text[:-1].strip()
    while text[0] in to_remove:
        text = text[1:].strip()

    text = remove_space_before_apostrophe(text)

    for target in postfix_targets:
        if text[-len(target):] == target:
            text = text[:-len(target)].strip()

    for target in prefix_targets:
        if text[:len(target)] == target:
            text = text[len(target):].strip()

    return text


def process_1_annotation(text):

    new_text = _process_1_annotation(text)
    while new_text != text:
        text = new_text
        new_text = _process_1_annotation(text)

    return text


def translate_batch(batch, langs, buf):

    en_batch = [process_1_annotation(x['caption']) for x in batch]
    for x, en_text in zip(batch, en_batch):
        x['en'] = en_text

    for lang in langs:
        lang_batch = []
        # Currently, bulk (batch) translation is not working
        for en_text in en_batch:
            lang_text = None
            lang_batch.append(lang_text)
            n_tried = 0
            while not lang_text and n_tried <= 5:
                try:
                    n_tried += 1
                    lang_text = translator.translate(en_text, src='en', dest=lang).text.lower().strip()
                    # remove trailing '.'
                    while lang_text[-1] in to_remove:
                        lang_text = lang_text[:-1].strip()
                    while lang_text[0] in to_remove:
                        lang_text = lang_text[1:].strip()
                    lang_batch[-1] = lang_text
                except Exception as e:
                    print(e)
                    lang_text = None
                    time.sleep(1.0)
            time.sleep(0.3)

        for x, lang_text in zip(batch, lang_batch):
            x[lang] = lang_text

    for x in batch:
        # put image url at the end
        image_url = x.pop('image_url')
        x['image_url'] = image_url
        jsonl = json.dumps(x, ensure_ascii=False, indent=None)
        buf.append(jsonl)


def translate_annotations(input_fn, output_fn, langs, batch_size=20, buf_size=100, inf=0, sup=None):

    entry_ids_processed = set()

    # Load previous work done
    if os.path.isfile(output_fn):
        with open(output_fn, 'r', encoding='UTF-8') as fp:
            for jsonl in fp:
                assert jsonl.strip()
                entry = json.loads(jsonl)
                entry_ids_processed.add(entry['id'])

    print(f'There are already {len(entry_ids_processed)} captions being processed!')
    print('start processing annotations ...')

    buf = []
    batch = []
    n_entries = len(entry_ids_processed)

    with open(input_fn, 'r', encoding='UTF-8') as input_fp:
        for jsonl in input_fp:

            entry = json.loads(jsonl)
            if entry['id'] in entry_ids_processed:
                continue
            if entry['id'] < inf:
                continue
            if sup is not None and entry['id'] >= sup:
                break

            batch.append(entry)
            if len(batch) == batch_size:
                translate_batch(batch, langs=langs, buf=buf)
                n_entries += len(batch)
                # empty batch
                batch = []

            if n_entries % buf_size == 0 and len(buf) > 0:
                with open(output_fn, 'a', encoding='UTF-8') as output_fp:
                    # write data to file
                    data = ''
                    for x in buf:
                        data += x + '\n'
                    output_fp.write(data)
                    # empty the buffer
                    buf = []

                print(n_entries)
                copyfile(output_fn, output_fn + '-backup')

        # remain
        if len(batch) > 0:
            translate_batch(batch, langs=langs, buf=buf)
            n_entries += len(batch)
            # empty batch
            batch = []

        # remain
        if len(buf) > 0:
            with open(output_fn, 'a', encoding='UTF-8') as output_fp:
                print(n_entries)
                # write data to file
                data = ''
                for x in buf:
                    data += x + '\n'
                output_fp.write(data)
                # empty the buffer
                buf = []

            print(n_entries)
            copyfile(output_fn, output_fn + '-backup')


if __name__ == "__main__":

    input_fn = 'cc3m_train.jsonl'

    langs = ['fr', 'es', 'pt', 'it', 'ja', 'ko', 'zh-CN']
    batch_size = 100
    buf_size = 100

    inf = 0
    sup = 100000

    output_fn = f'cc3m_train_translated_{inf}_to_{sup}.jsonl'

    translate_annotations(
        input_fn, output_fn, langs, batch_size=batch_size, buf_size=buf_size,
        inf=inf, sup=sup
    )
