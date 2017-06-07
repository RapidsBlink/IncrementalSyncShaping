#!/usr/bin/env bash

teamCode="78921g1clv"
baseUrl="http://middle2017.oss-cn-shanghai.aliyuncs.com/"
gcServerLog="gc_server.log"
gcClientLog="gc_client.log"

warnServerLog="server-${teamCode}-WARN.log.part"
warnClientLog="client-${teamCode}-WARN.log.part"
infoServerLog="server-${teamCode}-INFO.log.part"
infoClientLog="client-${teamCode}-INFO.log.part"

mkdir -p logs

folderName=`date +%F_%H-%M`

mkdir -p logs/${folderName}

echo "download log into folder $folderName..."

cd logs/$folderName

wget "$baseUrl$teamCode/$gcServerLog"
wget "$baseUrl$teamCode/$gcClientLog"

wget "$baseUrl$teamCode/$warnServerLog"
wget "$baseUrl$teamCode/$warnClientLog"
wget "$baseUrl$teamCode/$infoClientLog"
wget "$baseUrl$teamCode/$infoServerLog"