"""
This script converts <file_name>.zft files from pushift.io data (https://files.pushshift.io/reddit/comments/)
into csv files.The data should be stored under '<script_location>/pushift_io_data/' folder.
Before using this script set up the required configuration:
SUBREDDITS_LST: List subreddits to extract.
Columns: columns to extract from the dataset.
ALREADY_PARSED: files that already been parsed and should be skipped.

Usage: python3.9 convert_zft_to_csv_raw_data.py
"""
import pandas as pd
from tqdm import tqdm
import zstandard
import io
import json
import os

SUBREDDITS_LST = {"cancer", 'breastcancer', 'lymphoma',
                  'thyroidcancer', 'Ovariancancer', 'ProstateCancer',
                  'lungcancer', "DrugNerds", 'HeadandNeckCancer',
                  'kidneycancer', 'mesothelioma', 'Ovariancancer',
                  'ProstateCancer', 'skincancer', 'braincancer', 'BladderCancer',
                  'Melanoma', 'MelanomaSupport', 'colorectalcancer', 'pancreaticcancer',
                  'PancreaticSurvivor', 'Cancersurvivors', 'livercancer'
                  }
COLUMNS = ('author', 'author_created_utc', 'author_fullname',
           'author_premium', 'body', 'comment_type',
           'created_utc', 'edited', 'id',
           'is_submitter', 'link_id', 'name',
           'parent_id', 'permalink', 'retrieved_on',
           'score', 'send_replies', 'subreddit', 'subreddit_id', 'author_flair_richtext'
           )
ALREADY_PARSED = {'RC_2022-09.zst', 'RC_2022-12.zst', 'RC_2022-11.zst', 'RC_2022-10.zst', 'RC_2022-07.zst',
                  'RC_2022-06.zst', 'RC_2022-05.zst', 'RC_2022-04.zst', 'RC_2022-03.zst', 'RC_2022-02.zst',
                  'RC_2022-01.zst', 'RC_2021-12.zst', 'RC_2021-11.zst', 'RC_2021-10.zst', 'RC_2021-09.zst',
                  'RC_2021-08.zst', 'RC_2021-07.zst', 'RC_2019-12.zst', 'RC_2019-11.zst', 'RC_2019-10.zst',
                  'RC_2019-09.zst'}


def parse_file(filename, year):
    print("************************************************")
    print(f"{filename}")
    print("************************************************")
    VERSION = filename[:-4]
    suberddit_statistics = {sub_reddit: 0 for sub_reddit in SUBREDDITS_LST}
    with open(f"pushift_io_data/{year}/{filename}", 'rb') as fh:
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        raw_data = []
        for line in tqdm(text_stream):
            try:
                data_line = json.loads(line)
            except:
                continue
            if data_line.get('subreddit', '') in SUBREDDITS_LST:
                relevant_data = {k: data_line.get(k, None) for k in COLUMNS}
                raw_data.append(relevant_data.values())
                suberddit_statistics[data_line.get('subreddit', '')] += 1
        raw_data_df = pd.DataFrame(raw_data, columns=relevant_data.keys())
        raw_data_df.to_csv(f'raw_data_all_years/{year}/raw_data_{VERSION}.csv')
        with open(f'raw_data_all_years/{year}/metadata_statistics_{VERSION}.txt', 'w') as convert_file:
            convert_file.write(json.dumps(suberddit_statistics))
        print(suberddit_statistics)


def parse_all_files():
    years = os.listdir('pushift_io_data')
    years.sort(reverse=True)
    for year in years:
        print(f"YEAR: {year}")
        raw_data_files = list(filter(lambda filename: filename[-4:] == '.zst', os.listdir(f'pushift_io_data/{year}/')))
        raw_data_files.sort(reverse=True)
        for raw_data_file in raw_data_files:
            if raw_data_file in ALREADY_PARSED:
                continue
            parse_file(filename=raw_data_file, year=raw_data_file[3:7])

#For specific file you can use
#parse_file(filename="RC_2018-12.zst", year="2018")
parse_all_files()

