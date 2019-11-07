#!/usr/bin/env python
#coding:utf-8

import os
import sys
import time
import socket
import urllib
import urllib2
import json
import subprocess

socket.setdefaulttimeout(60)

def getTime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
def getStats(endpoint,step):
    ret = {"data":{},"metrics":[]}
    cmd = subprocess.Popen(''' cat /proc/diskstats |egrep 'sd|vd|hd'|awk '$3!~/[1-9]/ {print}' ''',stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    err = cmd.stderr.read().strip()
    if err:
        print "%s get diskstats error:%s"%(getTime(),err)
        sys.exit(1)
    else:
        timestamp = int(time.time())
        for diskstat in cmd.stdout.read().strip().split("\n"):
            #print diskstat
            line = diskstat.strip().split()
            ret["data"][line[2]] = {
                "read": line[3],
                "write": line[7],
                "read_sector": line[5],
                "write_sector": line[9],
                "read_ms": line[6],
                "write_ms": line[10]
            }
            ret["metrics"].append({"metric":"read","value": line[3],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
            ret["metrics"].append({"metric":"write","value": line[7],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
            ret["metrics"].append({"metric":"read_sector","value": line[5],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
            ret["metrics"].append({"metric":"write_sector","value": line[9],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
            ret["metrics"].append({"metric":"read_ms","value": line[6],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
            ret["metrics"].append({"metric":"write_ms","value": line[10],"endpoint": endpoint, "timestamp": timestamp, "step": step, "tag":"device=%s"%line[2],"counterType":"COUNTER"})
    return ret

def pushMetricToFalcon(url,data):
    headers = {'Content-Type': 'application/json'}
    print data
    #request = urllib2.Request(url=url, headers=headers, data=json.dumps(data))
    #response = urllib2.urlopen(request).read()
    #print response

def localPrint(interval):
    while True:
        tmp1 = getStats(hostname,60)["data"]
        time.sleep(interval)
        tmp2 = getStats(hostname,60)["data"]
        for dev in tmp2:
            read_all = int(tmp2[dev]["read"]) - int(tmp1[dev]["read"])
            write_all = int(tmp2[dev]["write"]) - int(tmp1[dev]["write"])
            io_total = read_all + write_all
            print "%s\t device: %s\t read: %s\t write: %s\t read_sector:%s\t write_sector: %s\t read_ms: %s\t write_ms: %s\t io_total: %s"%(
                getTime(),
                dev,
                read_all,
                write_all,
                int(tmp2[dev]["read_sector"]) - int(tmp1[dev]["read_sector"]),
                int(tmp2[dev]["write_sector"]) - int(tmp1[dev]["write_sector"]),
                int(tmp2[dev]["read_ms"]) - int(tmp1[dev]["read_ms"]),
                int(tmp2[dev]["write_ms"]) - int(tmp1[dev]["write_ms"]),
                io_total
                )
        #print "%s"%os.linesep

if __name__ == '__main__':
    hostname = socket.gethostbyaddr(socket.gethostname())[0]
    stats = getStats(hostname,60)
    args = sys.argv
    if len(args) == 3:
        if (args[1] == "print") and (args[2].isdigit()):
            localPrint(5)
    else:
        pushMetricToFalcon('http://127.0.0.1:1988/v1/push',stats["metrics"])
