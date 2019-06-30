import os
import sys
from pprint import pprint
import pandas as pd
import re
from send2trash import send2trash

### edit as needed
print_detail=True



def getalbum(f):
    f = re.sub(r'\[.*?\]','',f).strip()
    m = re.match(r'(\d+)-(\d+)', f)
    if m:
        return int(m[1])
    return 0

def gettrack(f):
    f = re.sub(r'\[.*?\]','',f).strip()
    m = re.match(r'\d+-(\d+)', f)
    if m:
        return int(m[1])
    m = re.match(r'\d+', f)
    if m:
        return int(m[0])

def getsize(row):
    f = os.path.join(root,row['dir'],row['file'])
    row['size'] = os.path.getsize(f)
    return row


re_korean = re.compile(r'[\u3130-\u318F\uAC00-\uD7AF\u1100–\u11FF]')
def select_best(group):
    keep=group.iloc[0]
    if len(group)==1: return group.iloc[0]
    for _,row in group.iterrows():
        if re_korean.search(row['file']):
            keep = row
            break

    if print_detail:
        print('\nKeeping: ' + keep['fullname'])
        print('Removing:')
        for n,row in group.iterrows():
            if keep.name != n:
                print('    ' + row['file'])
    return keep




pd.set_option('max_colwidth', -1)
pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', -1)


root = sys.argv[1]
allfiles = [[d[len(root)+1:],f] for d,_,files in os.walk(root) for f in files]
dd = pd.DataFrame(allfiles, columns=['dir','file'])
dd['album'] = dd['file'].apply(getalbum)
dd['track'] = dd['file'].apply(gettrack)
dd['size']=None
dd = dd[~dd['track'].isnull()]


dup = dd[dd.duplicated(['dir','album','track'], keep=False)]
dup = dup.apply(getsize, axis=1)
dup['len'] = dup['file'].apply(lambda f: len(f))
dup['fullname'] = dup[['dir', 'file']].apply(lambda s:'/'.join(s), axis=1)
dup = dup.sort_values(['dir','album','track','len','file'])
dup2 = dup[dup.duplicated(['dir','album','track','size'], keep=False)]


best = pd.DataFrame(columns=dup.columns)
for n,g in dup2.groupby(['dir','album','track','size']):
    best = best.append(select_best(g))

# print('to keep:\n===========')
# print(best[['dir', 'file']].apply(lambda s:'/'.join(s), axis=1))
# print(best['fullname'])
best['fullname'].to_csv('dups/tokeep.csv', header=True)


toremove = dup2[~dup2.index.isin(best.index)]
# print('\nto remove:\n===========')
# print(toremove['fullname'])
toremove['fullname'].to_csv('dups/toremove.csv', header=True)
# print(toremove[['dir', 'file']].apply(lambda s:'/'.join(s), axis=1))
# for i,row in toremove[['fullname']].iterrows():
#     fn = os.path.normpath(os.path.join(root,'/'.join(row)))
#     print(fn)
#     send2trash(fn)


print('\nduplicate dir:\n===========')
dup3 = dup[~dup.index.isin(dup2.index)]
dup3 = dup3.drop_duplicates('dir')
print(dup3['dir'])
dup3['dir'].to_csv('dups/dup_dir.csv', header=True)
# print(dup.drop_duplicates().to_string(index=False))