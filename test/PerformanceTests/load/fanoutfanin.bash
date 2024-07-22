#!/bin/bash
for i in {1..500}
do
  echo  "---- Iteration $i ----    " ` date -u`
  curl "https://functionssb4.azurewebsites.net/many/it$i-/start" --max-time 300 -d FanOutFanInOrchestration.200/1000
  curl "https://functionssb4.azurewebsites.net/many/it$i-/await" -d 200 --max-time 300
  curl "https://functionssb4.azurewebsites.net/many/it$i-/purge" -d 200 --max-time 300
  sleep 100
done
