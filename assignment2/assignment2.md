MapReduce Class, Spring 2013
====================

Thang Nguyen (daithang1111)
--------------------------
Assignment2
---------------------


### Q0

>I use "pairs" and "tripes" implementation. For "pairs", each map emits all unique ordered word pairs and (word *), each reducers then sum up total unique pairs, and number of occurrence of each word (sum of (word *)), I did a trick on reducer which write out, for example, if we have Count(A,B), Count(A *) then I will write out 2 pairs-values, one of which is in reverse order. By doing this trick, the final output will contains every ordered pairs 2 times. Example: (A,B) will have 2 values , 1 for Log(Count(A,B))+Log(Total)-Log(Count(B*)), and 1 for -Log(Count(A,*)). I used the second MR job to simply just add those two values to get PMI(A,B). The final output record will be (A,B) PMI(A,B). 

>For "tripes", similar to the approach I used for pairs, the only difference is that I used "stripes" strategy in which a mapper emits a word and all associative words with value of 1, regardless of order. For example, a sentence A B C, the mapper emits A->*:1,B:1,C:1.

### Q1

> The total running time for "pairs" implementation is 480s.

> The total running time for "stripes" implementation is 230s. 

### Q2

> The total running time for "pairs" implementation without combiners is 512s.

> The total running time for "stripes" implementation without combiners is 240s.

### Q3

> Both implementations extract the same number of pairs PMI = 233518.

### Q4

> The pairs with max PMI is (with, &c), value = 10.27544. 

> It seems obvious that "with" is a very popular prep. &c in the literature, especially those written by previous century authors, was used intensively like a replacement for "etc" or "so on". That is why this pair has high PMI value.

### Q5

> Three words with highest PMI with "love" is ("hate", 2.57), ("hermia", 2.02), ("commandments", 1.94).

> Three words with highest PMI with "cloud" is ("tabernacle", 4.15), ("glory", 3.399), "fire", 3.235).

Grading
=======

-Jimmy
