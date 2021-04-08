#! /bin/sh
cd /home/q/cool_dispatcher/
sudo chmod a+w -R .
cd ./bin
source /home/q/python36st/envs/transformer/bin/activate
c0=`ps aux | grep "message_dispatcher" | grep "$1" | grep -v "grep"| awk -F' ' '{print $2}'|wc -l`
if [ $c0 -gt 0 ];
then
 ps aux | grep "message_dispatcher" | grep "$1" | grep -v "grep"| awk -F' ' '{print $2}'|xargs -I {} sudo kill -9 {}
fi

sudo nohup python message_dispatcher.py -t $1 -e beta > /tmp/message_dispatcher.log 2>&1  &
sudo nohup python start_web_service.py -c restart -t $1 -e beta > /tmp/web_service_start.log 2>&1 &