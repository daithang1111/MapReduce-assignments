sh helpRun.sh code.DemoWordCount -input bible+shakes.nopunc.gz -output wc_daithang1111 -numReducers 5
hadoop fs -get wc_daithang1111/part-r-* daithang1111
