#!/usr/bin/env bash

teamCode="78921g1clv"
baseUrl="http://middle2017.oss-cn-shanghai.aliyuncs.com/"

serverLog="server.log.tar.gz"
clientLog="client.log.tar.gz"

mkdir -p logs

folderName=`date +%F_%H-%M`

mkdir -p logs/${folderName}

echo "download log into folder $folderName..."

cd logs/$folderName

wget -c "$baseUrl$teamCode/$serverLog"
wget -c "$baseUrl$teamCode/$clientLog"

tar xvzf ${serverLog} -C .
tar xvzf ${clientLog} -C .

