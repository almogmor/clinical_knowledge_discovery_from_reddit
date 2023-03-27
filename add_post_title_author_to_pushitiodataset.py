"""
This script uses reddit account to crawl the website using reddit user and to extracts the title, post, author,
author_full_name and comment.
Usage: python3.9 add_post_title_author_to_pushitiodataset.py --file_name=raw_data_RC_2020-07
"""
import praw
import pandas as pd
from tqdm import tqdm
from crawler import get_reddit, get_comments_data
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--file_name', help='The raw_data_RC file name. e.g. raw_data_RC_2022-10')
args = parser.parse_args()

PREFIX = "https://www.reddit.com"

FILE_NAME = args.file_name  #e.g. 'raw_data_RC_2022-10'
print(f"***{FILE_NAME}***")
YEAR = FILE_NAME[12:16] #e.g. 2022
data = pd.read_csv(f'raw_data_all_years/{YEAR}/{FILE_NAME}.csv')
urls = data.permalink
reddit = get_reddit()

title, post, author, author_full_name, comment = [], [], [], [], []
for url in tqdm(urls.items()):
    full_url = PREFIX + url[1]
    submission = reddit.submission(url=full_url)
    try:
        title.append(submission.title)
    except:
        title.append("TITLE-NOT FOUND")
    try:
        post.append(submission.selftext)
    except:
        post.append("POST-NOT FOUND")
    try:
        author.append(submission.author)
    except:
        author.append("AUTHOR-NOT FOUND")
    try:
        author_full_name.append(submission.author_fullname)
    except:
        author_full_name.append("AUTHOR FULL NAME-NOT FOUND")
    if get_comments_data != "":
        comment.append(get_comments_data(submission=submission))
    else:
        comment.append("COMMENT NAME-NOT FOUND")


data['title(crawled)'] = title
data['post(crawled)'] = post
data['author(crawled)'] = author
data['author_fullname(crawled)'] = author_full_name
data['comment(crawled)'] = comment

print(f'{FILE_NAME}_with_posts.csv')
data.to_csv(f'raw_data_with_posts/{FILE_NAME}_with_posts.csv')
