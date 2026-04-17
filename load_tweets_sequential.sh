#!/bin/bash

files=$(find data/*)

echo '================================================================================'
echo 'load denormalized'
echo '================================================================================'
time for file in $files; do
    echo $file
    # copy your solution to the twitter_postgres assignment here
    python3 -u load_tweets.py \
	--db=postgresql://postgres:pass@localhost:5441/pg_denormalized \
	--inputs $file
done

echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
time for file in $files; do
    echo $file
    # copy your solution to the twitter_postgres assignment here
    python3 -u load_tweets.py \
	--db=postgresql://postgres:pass@localhost:5441/pg_normalized \
	--inputs $file
done

echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
time for file in $files; do
    python3 -u load_tweets_batch.py --db=postgresql://postgres:pass@localhost:5441/pg_normalized --inputs $file
done
