import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm

tags = ['MP3','FLAC','ITUNES','APE','WAV','Zip']
def create_createjob(row):
    error=True
    for tag in tags:
        if tag in row and row[tag]:
            cds = defaultdict(list)
            nocds = []

            for s in row[tag].split():
                if '|' in s:
                    cd, url = s.split('|', 2)
                    cds[cd].append(url)
                else:
                    nocds.append(s)

            for cd,urls in cds.items():
                filename = 'crawljob/hulkpop-%05d-%s-%s.crawljob' % (row.name + 8000, tag, cd)
                with open(filename, mode='w', encoding='utf8') as f:
                    f.write('packageName=%s (%s) %s\n' % (row['title'], tag, cd))
                    f.write('text=' + ' '.join(urls) + '\n')
                    f.write('comment=' + row['url'] + '\n')

            if nocds:
                filename = 'crawljob/hulkpop-%05d-%s.crawljob' % (row.name + 8000, tag)
                with open(filename, mode='w', encoding='utf8') as f:
                    f.write('packageName='+row['title']+' ('+tag+')\n')
                    f.write('text='+' '.join(nocds)+'\n')
                    f.write('comment=' + row['url']+'\n')
            error=False
    pbar.update(1)
    return bool(error or row['error'])


df = dd.read_csv('out/hulkpop-links-*.csv', encoding='UTF-8', dtype='object', keep_default_na=False).compute()
df.index = pd.RangeIndex(0,len(df))

pbar = tqdm(total=len(df), ncols=80)
errors = df[df.apply(create_createjob, axis=1)]
errors.to_csv('errors.csv', encoding='UTF-8', index=False, line_terminator='\n')

