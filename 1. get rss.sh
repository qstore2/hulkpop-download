for i in `seq 1 2500`; do curl -L "http://omgkpop.top/feed/?paged=$i" -o feed$i.xml;done

for f in *.xml;do cat $f|xmlstarlet sel -t -m '//item' -v link -n;done > hulkpop.csv
