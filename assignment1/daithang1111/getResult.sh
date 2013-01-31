cat part-r-00000 part-r-00001 part-r-00002 part-r-00003 part-r-00004 >part-r
awk -F ' ' '{print $1}' part-r |sort| uniq -c > countResult
