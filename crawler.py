import praw
import pandas as pd
from tqdm import tqdm
import json

def get_reddit():
    # Define user agent
    user_agent = "praw_scraper_1.0"
    # Create an instance of reddit class
    reddit = praw.Reddit(username="almogmor345",
                         password="ALMOGMOR",
                         client_id="_42ONsfrDcPF33X1wx-bBQ",
                         client_secret="u60RZjB7jq_g13_LY4avZSTc4ANwMQ",
                         user_agent=user_agent
    )
    return reddit



def get_subreddit_data(reddit, subreddit_name):
    # Create sub-reddit instance
    subreddit = reddit.subreddit(subreddit_name)
    # Printing subreddit info
    print(subreddit.display_name)
    df = pd.DataFrame()
    titles, scores, ids, self_text, comments_text, subreddit_names, author = [], [], [], [], [], [], []
    for submission in tqdm(subreddit.new(limit=None)):
        self_text.append(submission.selftext)
        titles.append(submission.title)
        scores.append(submission.score) #upvotes
        author.append(submission.author_fullname)
        ids.append(submission.id)
        subreddit_names.append(subreddit_name)
        comments_content = get_comments_data(submission)
        comments_text.append(comments_content)

    df['Title'] = titles
    df['Id'] = ids
    df['Upvotes'] = scores  # upvotes
    df['post'] = self_text
    df['comments'] = comments_text
    return df

def extract_comment_data(comment):
    metadata_dict = {
        'Author': comment.author.name,
        'author_fullname': comment.author_fullname,
        'parent_id': comment.parent_id,
        'created_utc': comment.created_utc,
        'id': comment.id,
        'body': comment.body
    }
    return json.dumps(metadata_dict)

def get_comments_data(submission):
    comments = submission.comments
    comments_content = ""
    for comment in comments:
        comments_content += f"#COMMENT"
        try:
            comments_content += extract_comment_data(comment=comment)
            if len(comment.replies) > 0:
                try:
                    for i in range(len(comment.replies)):
                        comments_content += f'$REPLY{i + 1}${extract_comment_data(comment=comment.replies[i])}'
                except Exception as err:
                    continue
        except Exception as e:
            continue
    return comments_content


if __name__ == "__main__":
    reddit = get_reddit()
    subreddit_name_lst = ["cancer", 'breastcancer', 'lymphoma',
                          'thyroidcancer', 'Ovariancancer', 'ProstateCancer',
                          'lungcancer', "DrugNerds", "aves"]
    data_set = pd.DataFrame()
    for subreddit_name in subreddit_name_lst:
        try:
            data_set_from_subreddit = get_subreddit_data(reddit=reddit, subreddit_name=subreddit_name)
            data_set = pd.concat([data_set, data_set_from_subreddit])
        except Exception:
            data_set.to_csv('praw_cancer_dataset.csv', encoding='utf-8')
    data_set.to_csv('praw_cancer_dataset.csv', encoding='utf-8')
