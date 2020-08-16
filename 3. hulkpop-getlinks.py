###
'''
Get all links by post id
(replace steps 1 to 3)

'''
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict
import dask
import dask.dataframe as dd
from dask.dataframe.utils import make_meta
from tqdm import tqdm


WORKERS=8
dask.config.set(work_stealing=False)
dask.config.set(num_workers=WORKERS)



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


re_hublog = re.compile(r"href='(.*)'>Click here to proceed")
def decode_hulblog(url):
    r = requests.get(url)
    re = re_hublog.search(r.text)
    return re[1] if re else url

#re_remove1 = re.compile(r'\((iTunes|ITUNES|FLAC|WAV|APE)[^)]*\)')
tags = ['MP3','FLAC','ITUNES','APE','WAV','Zip']
re_remove4 = re.compile(r'\(('+'|'.join(tags)+')[^)]*\)', re.IGNORECASE)
re_tags = re.compile(r'^\W*('+'|'.join(tags)+')', re.IGNORECASE)
re_download = re.compile(r'\W*Download Zip file')
re_cd = re.compile(r'(CD\s*\d+)')
def getlinks(row):
    try:
        r = requests.get(row['url'])
        if r.status_code != 200:
            row['error'] = "True"
            pbar.update(1)
            return row
        row['url'] = r.url
        soup = BeautifulSoup(r.text, 'lxml')

        #title
        t = soup.select_one('h1.post-title')
        # print(t.text)
        row['title'] = cleanup(re_remove4.sub('',t.text))
        row['author'] = spilt_author(row['title'])

        s = soup.select_one('div.entry-inner')
        links = s.findAll('a', target="_blank")
        # print(links)

        types = defaultdict(list)
        for l in links:
            url = l['href']
            if 'safelink.hulblog.com' not in url:
                continue

            url = decode_hulblog(url)
            text = url
            m = re_cd.search(l.text)
            if m:
                text = m[1].replace(' ','') + '|' + url
            # print(text)
            # title = l.text
            p = l.find_parent('p')
            if not p:
                p = l
            type = p.find_previous_sibling('p', text=re_tags)
            if type:
                re=re_tags.search(type.text)
                # print(row['title'], url, re)
                types[re[1]].append(text)
            else:
                # type = p.find_previous_sibling('p', text=re_download)
                # if type:
                types['Zip'].append(text)
        for k, v in types.items():
            row[k] = ' '.join(v)
        # print('end='+row['title'])
    except Exception as e:
        # print(e)
        row['error'] = "True"
        # pass
    pbar.update(1)
    return row



hulk = pd.read_csv('hulkpop.csv', encoding='UTF-8', sep='\n', header=None, names=['url'] )
for t in ['title','author']+tags+['error']:
    hulk[t]=''
pbar = tqdm(total=len(hulk), ncols=80)

ddata = dd.from_pandas(hulk, npartitions=WORKERS)
h3 = ddata.apply(getlinks, axis=1, meta=make_meta(hulk))
files=h3.to_csv('out/hulkpop-links-*.csv', encoding='UTF-8', index=False, line_terminator='\n')
