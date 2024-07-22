#!/bin/bash
for i in {1..100}
do
  echo  "---- Iteration $i ----    " ` date -u`
  for j in {0..99}
  do
  curl "https://functionssb4.azurewebsites.net/store/$1$j" -d 8000   
  sleep 2
  done
  sleep 30
  curl -X delete "https://functionssb4.azurewebsites.net/storevector/$1/100"   
  sleep 300
done
