for i in `seq 1 2500`; do curl -L "https://hulkpop.com/feed/?paged=$i" -o feed$i.xml;done

for f in *.xml;do cat $f|xml sel -t -m '//item' -v "concat('\"',title,'\",',link)" -n;done > hulkpop.csv
