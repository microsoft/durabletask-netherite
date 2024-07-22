#!/bin/bash
for i in {1..500}
do
  echo  "---- Iteration $i ----    " ` date -u`
  curl "https://functionssb4.azurewebsites.net/many/d$i-/start" --max-time 300 -d HelloSequence5.10000.200
  curl "https://functionssb4.azurewebsites.net/many/d$i-/await" -d 10000 --max-time 300
  curl "https://functionssb4.azurewebsites.net/many/d$i-/purge" -d 10000 --max-time 300
  sleep 150
done
