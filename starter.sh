#!/bin/sh

# Let's check if DB container connected to cost-net network. Connect it if it isn't.
name=`sudo docker network inspect yt-net -f '{{ range.Containers}}{{.Name}}{{end}}' 2> /dev/null`
if [ $? -eq 0 ]; then
  if [ "$name" = "pg_db" ]; then
    echo "DB is up"
  else
    sudo docker network connect yt-net pg_db
  fi
else
  echo "ERROR: Network yt-net may not exist!"
fi

container_id=`sudo docker ps -a | awk '/yt-metrics/ {print $1}'`
LOG_PATH=`sudo docker inspect "$container_id" | awk -F':' '/app.install.path/{gsub(" ","",$0); gsub("\"", "", $0); print $2}'`
LOG_FILE="$LOG_PATH/cost.cron.log"

sudo docker start -a yt-metr 2>&1 | tee "$LOG_FILE"
container_id=`sudo docker ps -a | awk '/yt-metrics/ {print $1}'`
exit_code=`sudo docker inspect "$container_id" --format='{{.State.ExitCode}}'`
if [ "$exit_code" -eq 0 ]; then
    status="OK"
else
    status="NOK"
    export LOG=`cat $LOG_FILE`
    curl -i -X POST -H 'Content-Type: application/json' \
         -d '{"username":"statistic", "text":"yt-metrics launch has failed", "attachments":[{"pretext":"Here is a log file", "text":"'"$LOG"'"}]}' \
         https://chat.ptsecurity.com/hooks/jhy9yuanatrxme3hweineb8doa > /dev/null 2>&1
fi
echo "Exit status: $status"
timestamp=`date '+%Y-%m-%d %H:%M:%S'`
psql -h localhost -p 5433 -U root -d leadtime -c "update yac_log set update_date='$timestamp', status='$status' where id = 'metr'"

