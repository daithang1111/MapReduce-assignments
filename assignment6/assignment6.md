MapReduce Class, Spring 2013
====================

Thang Nguyen (daithang1111)
--------------------------
Assignment6
---------------------

### Note ###
> I was able to generate two outputs which are accurate and the same. Below are pig codes for extracting All/Egypt. Thanks!

### Analysis_1, countsAll ###
> 

a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, date:chararray, user:chararray, content:chararray);

x = foreach a generate $1;

y = foreach x generate REPLACE(REPLACE(SUBSTRING((chararray)$0, 4,13),'Feb','2'),'Jan','1');

z =group y by $0;

t = foreach z generate group as term, COUNT(y) as count;

p = FILTER t BY (SIZE($0)==7);

q =FILTER p BY (SUBSTRING($0,0,1)=='2' AND SUBSTRING($0,2,4)<'09') OR (SUBSTRING($0,0,1)=='1' AND SUBSTRING($0,2,4)>'22');

r = foreach q generate CONCAT(REPLACE(SUBSTRING($0,0,4),' ','/'),SUBSTRING($0,4,7)) as ll, count as count;

store r into 'daithang1111-all-pig';
 

### Analysis_2, countsEgypt ###
>

b = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, date:chararray, user:chararray, content:chararray);

a = FILTER b BY ($3 matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');

x = foreach a generate $1;

y = foreach x generate REPLACE(REPLACE(SUBSTRING((chararray)$0, 4,13),'Feb','2'),'Jan','1');

z =group y by $0;

t = foreach z generate group as term, COUNT(y) as count;

p = FILTER t BY (SIZE($0)==7);

q =FILTER p BY (SUBSTRING($0,0,1)=='2' AND SUBSTRING($0,2,4)<'09') OR (SUBSTRING($0,0,1)=='1' AND SUBSTRING($0,2,4)>'22');

r = foreach q generate CONCAT(REPLACE(SUBSTRING($0,0,4),' ','/'),SUBSTRING($0,4,7)) as ll, count as count;

store r into 'daithang1111-egypt-pig';



Grading
=======

Everything looks great!
Minor comment: filtering *before* grouping will make your script more efficient.

Score: 25/25

-Jimmy
