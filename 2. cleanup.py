import pandas as pd
import swifter
import dask.dataframe as dd
from dask.multiprocessing import get
from fuzzywuzzy import fuzz, process
import re
import os
from html import unescape
import timeit
from tqdm import tqdm


tqdm.pandas()

re_remove1 = re.compile(r'\((iTunes|ITUNES|FLAC|WAV|APE)[^)]*\)')
re_remove2 = re.compile(r'[-\u2013\u2014\u3161]\s+Single')
re_remove3 = re.compile(r'\[Single\]')
def cleanup(name):
    name = re_remove1.sub('',re_remove2.sub('',re_remove3.sub('',name)))
    return name.strip()


def spilt_author(title):
    title = title.replace((b'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c').decode('utf-8'), '-')
    artist=re.sub("\s+[-\u2013\u2014\u3161]\s+.*","",title)
    if artist==title:
        artist=re.sub("\W[-\u2013\u2014\u3161]\W.*","",title)
    if artist==title:
        artist=re.sub("\s+[-\u2013\u2014\u3161]\w*[a-zA-Z].*","",title)
    if artist==title:
        artist=re.sub("\s+\(\d{4}\)\s+[a-zA-Z].*","",title)

    if artist==title:
        artist="Others"
    artist=re.sub("[\s.]+$","",artist)
    if re.match("^\d+$", artist):
        artist="Others"
    if artist=='VA' or artist=='V.A':
        artist='Various Artists'

    return artist


def get_ratio(row):
    author = row['author']
    title = row['title']
    authors = process.extractBests(author, existing['author'], scorer=fuzz.ratio, score_cutoff=90)
    if authors:
        titles = process.extractOne(title, existing.loc[[a[2] for a in authors]]['title'], scorer=fuzz.ratio, score_cutoff=90)
        if titles:
            return titles[1]
        # print(title,' -> ',titles)
    return 0
    # return x[1] if x else 0



###main

### if there's some albums that you do want to download, put in the file existing.txt
existing=()
if os.path.isfile('existing.txt'):
    existing = pd.read_csv('existing.txt', sep='\n', columns=['title'])
    existing['title'] = existing['title'].apply(cleanup)
    existing['author'] = existing['title'].apply(spilt_author)
    existing.to_csv('k2n-done.csv', encoding='UTF-8', index=False)
    existing = set(existing['title'])
# print(existing)


hulk = pd.read_csv('hulkpop.csv', encoding='UTF-8', header=None)
hulk.columns=['title','url']
hulk['title'] = hulk['title'].apply(unescape).apply(cleanup)
hulk['author'] = hulk['title'].apply(spilt_author)
h1=hulk
# print('Starting...')

# h2=h1.swifter.progress_bar(True).apply(get_ratio, axis=1)
h2=h1.progress_apply(get_ratio, axis=1)
h3=h1[h2==0]
h3.to_csv('hulkpop-todownload.csv', encoding='UTF-8', index=False)

