###
# sort the folders by artist/original_folder
# artist name is guessed by using the folder name up to the 1st dash (-), most unicode dash is detected
# if 2 artist names have the same words sequence, program will take the 1st matched (eg "K. Will", "K Will", "K - Will" will match into 1)
###


import os
import sys
import re
import argparse
import unicodedata

parser = argparse.ArgumentParser()
parser.add_argument('source_dir')
parser.add_argument('dest_dir')
parser.add_argument('artist_list', nargs='?')
args = parser.parse_args()


#https://www.fileformat.info/info/unicode/category/index.htm
tbl_punctuation = {i:32 for i in range(sys.maxunicode) if unicodedata.category(chr(i))[0] in 'PMCZ'}
def remove_punctuation(s):
    return re.sub(' +',' ',s.translate(tbl_punctuation).lower()).strip()


#remove "HULKPOP" from filename
def cleanup_hulkpop(dir):
    for f in os.scandir(dir):
        if f.is_dir():
            cleanup_hulkpop(f)
        newname = re.sub(r'\s*\[?HULKPOP(\.COM)?\]?\s*', ' ', f.name, 0, re.IGNORECASE)
        if f.name != newname:
            try:
                f1, f2 = os.path.splitext(newname)
                f1 = f1.strip('- ').lstrip('.')
                newname = f1 + f2
                # print(f.path, '=>', os.path.join(dir, newname))
                os.rename(f.path, os.path.join(dir, newname))
            except:
                pass



##main

artist_list={}

if args.artist_list and os.path.exists(args.artist_list):
    for l in open(args.artist_list, "r", encoding="utf-8").readlines():
        artist_list[remove_punctuation(l)] = l.strip()

# print(artist_list)
if os.path.exists(args.dest_dir):
    artist_list.update({remove_punctuation(d.name):d.name for d in os.scandir(args.dest_dir) if d.is_dir()})
# print(artist_list)


#https://www.compart.com/en/unicode/category/Pd
#delimiter = "[-\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u2212\u2e3a\u2e3b\ufe58\ufe63\uff0d\u3161]"
delimiter = '['+''.join([chr(i) for i in range(sys.maxunicode) if unicodedata.category(chr(i)) == 'Pd'])+']'
music_tags = {'.mp3':'MP3','.flac':'FLAC','.m4a':'ITUNES','.ape':'APE','.wav':'WAV','.wave':'WAV'}
re_remove1 = re.compile(r'\s*\(('+'|'.join(music_tags.values())+')\)$', re.IGNORECASE)

for dir in os.scandir(args.source_dir):
    if dir.is_dir():
        name = dir.name.strip()
        # replace 'â€“' (â€” character showing instead of em dash (—))
        title = name.replace((b'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c').decode('utf-8'), '-')
        newtitle = title

        # check if dir is a CD
        cd = ''
        m = re.match("(.*\)) (CD\d+)$", newtitle)
        if m:
            newtitle = m[1]
            cd = m[2]

        # rename dir tagging with MP3/ITUNES/FLAC/etc
        newtitle = re.sub(' \(Zip\)$', '', newtitle)
        newtitle = re_remove1.sub('', newtitle)

        allfiles = [f for d, _, files in os.walk(dir.path) for f in files]
        filesext = list(set(music_tags.keys()) & set([os.path.splitext(f)[1] for f in allfiles]))
        filestag = set([music_tags[f] for f in filesext])

        if len(filestag)==1:
            newtitle = newtitle + ' (' + filestag.pop() + ')'

        # remove square bracket in front
        title2 = re.sub("^\s*\[[^]]+]\s*", "", title)

        # try extract artist
        artist=re.sub("\s+"+delimiter+"\s+.*","", title2)
        if artist==title2:
            artist=re.sub("\W"+delimiter+"\W.*","", title2)
        if artist==title2:
            artist=re.sub("\s+"+delimiter+"\w*[a-zA-Z].*","", title2)
        # if artist==title:
        #     artist=re.sub("\s+\(\d{4}\)\s+[a-zA-Z].*","",title)

        if artist==title2:
            artist="Others"
        artist=artist.strip().strip('.').strip()
        if artist=='VA' or artist=='V.A' or artist=='Various Artist':
            artist='Various Artists'

        artist2 = remove_punctuation(artist)
        if artist2 in artist_list.keys():
            artist = artist_list[artist2]
        else:
            artist_list[artist2] = artist

        # split artist by 1st letter
        letter = artist[0].upper()
        if not letter.isalpha(): letter='0'

        # if there's in 1 subdir, move that instead (except if it contains 'CD1')
        source = dir.path
        sfiles = os.scandir(source)
        files = [next(sfiles, None), next(sfiles, None)]
        if not files[1] and files[0] and files[0].is_dir():
            if cd or not re.match("CD\d+", files[0].name):
                source = files[0].path

        # create dest dir parent
        dest = os.path.join(args.dest_dir, letter, artist, newtitle)
        if cd:
            dest = os.path.join(dest, cd)
        # print(source, '=>', dest)
        path = os.path.split(dest)[0]
        if not os.path.exists(path):
            os.makedirs(path)
        if not os.path.isdir(path):
            print("ERROR: path is not dir: " + path)
            continue

        # move source dir to dest
        if not os.path.exists(dest):
            os.rename(source, dest)
            if source != dir.path:
                os.rmdir(dir.path)
            print("%s => %s%s%s%s%s" % (name, letter, os.sep, artist, os.sep, newtitle))

            #remove "HULKPOP" from filename
            for d, f in [[d, f] for d, _, files in os.walk(dest) for f in files]:
                f1, f2 = os.path.splitext(f)
                f1 = re.sub('\s*\[?HULKPOP(\.COM)?\]?\s*', ' ', f1, re.IGNORECASE).strip('_- ')
                f1 = re.sub('\s*\[?OMGKPOP(\.TOP)?\]?\s*', ' ', f1, re.IGNORECASE).strip('_- ')
                # convert '#Uffff' to the unicode equivalent
                f1 = re.sub(r'#U([A-Fa-f0-9]{4})', lambda m: chr(int(m[1], 16)), f1)
                try:
                    os.rename(os.path.join(d, f), os.path.join(d, f1 + f2))
                except:
                    pass
        else:
            print("ERROR: path exist: %s%s%s" % (artist, os.sep, title))


if args.artist_list:
    with open(args.artist_list,"w",encoding="utf-8") as f:
        f.writelines("%s\n" % item for item in artist_list.values())




