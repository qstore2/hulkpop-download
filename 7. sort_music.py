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
from pprint import pprint

rclone_arg1='qstore122_redtopia_to:FLAC/!to_sort/'
rclone_arg2='qstore122_redtopia_to:FLAC/'

parser = argparse.ArgumentParser()
parser.add_argument('source_dir')
parser.add_argument('dest_dir')
parser.add_argument('artist_list', nargs='?')
args = parser.parse_args()


#https://www.fileformat.info/info/unicode/category/index.htm
tbl_punctuation = {i:32 for i in range(sys.maxunicode) if unicodedata.category(chr(i))[0] in 'PMCZ'}
def remove_punctuation(s):
    return re.sub(' +',' ',s.translate(tbl_punctuation).lower()).strip()


artist_list={}

if args.artist_list and os.path.exists(args.artist_list):
    for l in open(args.artist_list, "r", encoding="utf-8").readlines():
        artist_list[remove_punctuation(l)] = l

# print(artist_list)
artist_list.update({remove_punctuation(d.name):d.name for d in os.scandir(args.dest_dir) if d.is_dir()})
# print(artist_list)


#https://www.compart.com/en/unicode/category/Pd
delimiter = "[-\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u2212\u2e3a\u2e3b\ufe58\ufe63\uff0d\u3161]"
for dir in os.scandir(args.source_dir):
    if dir.is_dir():
        name = dir.name.strip()
        title = name.replace((b'\xc3\xa2\xe2\x82\xac\xe2\x80\x9c').decode('utf-8'), '-')
        title = re.sub(' \(Zip\)$', '', title)
        title2 = re.sub("\s*\[[^]]+]\s*", "", title)

        artist=re.sub("\s+"+delimiter+"\s+.*","", title2)
        if artist==title2:
            artist=re.sub("\W"+delimiter+"\W.*","", title2)
        if artist==title2:
            artist=re.sub("\s+"+delimiter+"\w*[a-zA-Z].*","", title2)
        # if artist==title:
        #     artist=re.sub("\s+\(\d{4}\)\s+[a-zA-Z].*","",title)

        if artist==title2:
            artist="Others"
        artist=artist.strip()
        if re.match("^\d+$", artist):
            artist="Others"
        if artist=='VA' or artist=='V.A':
            artist='Various Artists'

        artist2 = remove_punctuation(artist)
        if artist2 in artist_list.keys():
            artist = artist_list[artist2]
        else:
            artist_list[artist2] = artist

        path = os.path.join(args.dest_dir, artist)
        if not os.path.exists(path):
            os.mkdir(path)
        if not os.path.isdir(path):
            print("ERROR path is not dir: " + artist)

        source = dir.path
        sfiles = os.scandir(source)
        files = [next(sfiles, None), next(sfiles, None)]
        if not files[1] and files[0] and files[0].is_dir():
            if not re.match("CD\d+", files[0].name):
                source = files[0].path

        dest = os.path.join(args.dest_dir, artist, title)
        if not os.path.exists(dest):
            os.rename(source, dest)
            if source != dir.path:
                os.rmdir(dir.path)
            print("%s => %s/%s" % (name, artist, title))

            #remove "HULKPOP" from filename
            for d, f in [[d, f] for d, _, files in os.walk(dest) for f in files]:
                f1, f2 = os.path.splitext(f)
                f1 = re.sub('\s*\[?HULKPOP(\.COM)?\]?\s*', ' ', f1, re.IGNORECASE).strip('- ')
                try:
                    os.rename(os.path.join(d, f), os.path.join(d, f1 + f2))
                except:
                    pass
        else:
            print("ERROR: path exist: %s/%s" % (artist, title))


with open(args.artist_list,"w",encoding="utf-8") as f:
    f.writelines("%s\n" % item for item in artist_list.values())




