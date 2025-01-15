#/bin/sh

chmod u+x ./starter.sh
/bin/mv -f pgpass ~/.pgpass 2>/dev/null
chmod 0600 ~/.pgpass
sudo docker build -t yt-metrics .

# Let's check if the network yt-net exist and create it if it isn't.
net=`sudo docker network list | awk '/yt-net/{print $1}'`
if [ -z "$net" ]; then
  echo "Network doesn't exist. Let's created it."
  sudo docker network create yt-net
else
   echo "Network exist"
fi

# Let's check if DB container connected to yt-net network. Connect it if it isn't.
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

# Let's run the application for the first time
sudo docker run --env-file ./.env --name yt-metr --label app.install.path=${PWD} --network yt-net yt-metrics

