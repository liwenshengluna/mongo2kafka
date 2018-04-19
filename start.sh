#!/bin/bash

WORK_DIR=/inews/crawler_lws/mongo2kafuka
TASK_NAME='weixin2kafuka.py'
export LD_LIBRARY_PATH=":/usr/local/lib:/usr/lib"
count=`ps uax | grep $TASK_NAME | grep -v "grep"| wc -l`
cd $WORK_DIR
if [ $count -eq 0 ]; then
    nohup /inews/crawler_lws/anaconda2/bin/python weixin2kafuka.py 2>&1 > /dev/null 2>&1 &
fi
