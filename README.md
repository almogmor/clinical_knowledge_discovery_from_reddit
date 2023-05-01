Author: Almog Mor
(almog.mor@mail.huji.ac.il)

This project contains pipeline that enables downloading metadata from reddit, filtering according to requested subreddits and scrap relevant posts and comments.

Current data location: 
1. Metadata from pushiftIO: /cs/labs/tomhope/almog.mor/pushift_io_data
2. Filtered data in csv format: /cs/labs/tomhope/almog.mor/raw_data_all_years
3. Processed data in csv format: /cs/labs/tomhope/almog.mor/raw_data_with_posts

High level system design:
Data extraction and processing include 3 stages: 
* Downloading metadata from pushift.io in ‘zft’ format. Each file includes all the metadata about all the threads in Reddit per month.
Command example:
```bash
wget https://files.pushshift.io/reddit/comments/RC_2021-12.zst -P /cs/labs/tomhope/almog.mor/pushift_io_data/2021
```

* The second stage gets as input the ‘zft’ files, filter the relevant threads, extract the relevant columns and convert them into ‘csv’ files. In this project we chose to filter a list of cancer related threads.

Command example:
```bash
python3.9 convert_zft_to_csv_raw_data.py 
```

* The last stage is a crawler which gets the ‘permalink’ from the ‘csv’ files of stage 2 and scraps the post, comment and title from the reddit website. The last stage outputs the ‘raw_data’ file per month in a ‘csv’ format which is ready to later be annotated.

Command example:
```bash
python3.9 add_post_title_author_to_pushitiodataset.py --file_name=raw_data_RC_2020-07
```
