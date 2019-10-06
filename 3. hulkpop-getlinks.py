import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict
import dask
import dask.dataframe as dd
from dask.dataframe.utils import make_meta
from tqdm import tqdm


WORKERS=64
dask.config.set(work_stealing=False)
dask.config.set(num_workers=WORKERS)


re_hublog = re.compile(r"href='(.*)'>Click here to proceed")
def decode_hulblog(url):
    r = requests.get(url)
    re = re_hublog.search(r.text)
    return re[1] if re else url

#re_remove1 = re.compile(r'\((iTunes|ITUNES|FLAC|WAV|APE)[^)]*\)')
tags = ['MP3','FLAC','ITUNES','APE','WAV','Zip']
re_remove2 = re.compile(r'\(('+'|'.join(tags)+')[^)]*\)', re.IGNORECASE)
re_tags = re.compile(r'^\W*('+'|'.join(tags)+')', re.IGNORECASE)
re_download = re.compile(r'\W*Download Zip file')
re_cd = re.compile(r'(CD\s*\d+)')
def getlinks(row):
    try:
        # print('start='+row['title'])
        row['title'] = re_remove2.sub('',row['title']).strip()

        r = requests.get(row['url'])
        soup = BeautifulSoup(r.text, 'html5lib')
        s = soup.select_one('div.entry-inner')
        links = s.findAll('a', target="_blank")

        types = defaultdict(list)
        for l in links:
            if l.get('class') and 'sd-button' in l.get('class'):
                continue

            url = l['href']
            if 'url.hulblog.com' in url:
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
    except:
        row['error'] = "True"
        pass
    pbar.update(1)
    return row



hulk = pd.read_csv('hulkpop-todownload.csv', encoding='UTF-8')
for t in tags:
    hulk[t]=''
hulk['Zip']=''
hulk['error']=''

pbar = tqdm(total=len(hulk), ncols=80)

ddata = dd.from_pandas(hulk, npartitions=WORKERS)
h3 = ddata.apply(getlinks, axis=1, meta=make_meta(hulk))
files=h3.to_csv('out/hulkpop-links-*.csv', encoding='UTF-8', index=False, line_terminator='\n')
