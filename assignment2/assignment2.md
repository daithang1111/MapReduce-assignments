MapReduce Class, Spring 2013
====================

Thang Nguyen (daithang1111)
--------------------------
Assignment2
---------------------


### Q0

>I use "pairs" and "tripes" implementation. For "pairs", each map emits all unique ordered word pairs and (word *), each reducers then sum up total unique pairs, and number of occurrence of each word (sum of (word *)), I did a trick on reducer which write out, for example, if we have Count(A,B), Count(A *) then I will write out 2 pairs-values, one of which is in reverse order. By doing this trick, the final output will contains every ordered pairs 2 times. Example: (A,B) will have 2 values , 1 for Log(Count(A,B))+Log(Total)-Log(Count(B*)), and 1 for -Log(Count(A,*)). I used the second MR job to simply just add those two values to get PMI(A,B). The final output record will be (A,B) PMI(A,B). 

>For "tripes", similar to the approach I used for pairs, the only difference is that I used "stripes" strategy in which a mapper emits a word and all associative words with value of 1, regardless of order. For example, a sentence A B C, the mapper emits A->*:1,B:1,C:1.

> The first term is "''but", it appears "1" time.
### Q2

> The third to last term is "zorah", it appears "8" times.
### Q3

> There are "41788" unique words.
### Q4

> The first term is "aaron", it appears "416" times.
### Q5

> The third to last term is "zorah", it appears "8" times.
### Q6

> There are "31940" unique words.

Grading
=======

-Jimmy
