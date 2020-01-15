#!/usr/bin/python3
import json
import requests
import os
import sys
import getopt
import csv
import time


def main():
    shard = ''
    username = ''
    password = ''
    clusterid = ''
    localpath = ''
    dbfspath = ''
    releasefile = ''
    outfilepath = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:u:p:lwfo',
                                   ['shard=', 'username=', 'password=', 'clusterid=', 'localpath=', 'dbfspath=', 'releasefile=', 'outfilepath='])
    except getopt.GetoptError:
        print(
            'run.py -u <username> -p <password> -s <shard> -c <clusterid> -l <localpath> -w <dbfspath> -f <releasefile> -o <outfilepath>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                'run.py -u <username> -p <password> -s <shard> -c <clusterid> -l <localpath> -w <dbfspath> -f <releasefile> -o <outfilepath>')
            sys.exit()
        elif opt in ('-s', '--shard'):
            shard = arg
        elif opt in ('-u', '--username'):
            username = arg
        elif opt in ('-p', '--password'):
            password = arg
        elif opt in ('-c', '--clusterid'):
            clusterid = arg            
        elif opt in ('-l', '--localpath'):
            localpath = arg
        elif opt in ('-w', '--dbfspath'):
            dbfspath = arg
        elif opt in ('-f', '--releasefile'):
            releasefile = arg
        elif opt in ('-o', '--outfilepath'):
            outfilepath = arg

    print('-s is ' + shard)
    print('-u is ' + username)
    print('-c is ' + clusterid)
    print('-l is ' + localpath)
    print('-w is ' + dbfspath)
    print('-f is ' + releasefile)
    print('-o is ' + outfilepath)

    # localpath and releasefile as mutally excluisive as releasefile contains localpath
    if localpath != '' and releasefile != '':
        print('localpath and releasefile as mutally excluisive as releasefile contains localpath')
        print(
            'run.py -u <username> -p <password> -c <clustrid> -s <shard> (-l <localpath> -w <dbfspath>) or (-f <releasefile>)')
        sys.exit()
    elif localpath != '':
        run_path(shard, username, password, clusterid, localpath, dbfspath, outfilepath)
    elif releasefile != '':
        run_file(shard, username, password, clusterid, localpath, outfilepath)


def run_path(shard, username, password, clusterid, localpath, dbfspath, outfilepath):
    # Generate array from walking local path
    sourcefiles = []
    for path, subdirs, files in os.walk(localpath):
        for name in files:
            fullpath = path + os.sep + name
            # removes localpath to repo but keeps dbfs path
            fulldbfspath = dbfspath + path.replace(localpath, '')
            fulldbfspath = fulldbfspath.replace(os.sep, '/')

            name, file_extension = os.path.splitext(fullpath)
            if file_extension.lower() in ['.scala', '.sql', '.r', '.py']:
                row = [fullpath, fulldbfspath, 1]
                sourcefiles.append(row)

    run(shard, username, password, clusterid, sourcefiles, outfilepath)

def run_file(shard, username, password, clusterid, releasefile, outfilepath):
    # Generate array from file

    sourcefiles = []
    with open(releasefile) as csvfile:
        reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)  # change contents to floats
        for row in reader:  # each row is a list
            sourcefiles.append(row)

    run(shard, username, password, clusterid, sourcefiles, outfilepath)


def run(shard, username, password, clusterid, sourcefiles, outfilepath):
    # run each element in array
    for sourcefile in sourcefiles:
        nameonly = os.path.basename(sourcefile[0])
        dbfspath = sourcefile[1]

        name, file_extension = os.path.splitext(nameonly)

        # workpath removes extension
        fulldbfspath = 'dbfs:' + dbfspath + '/' + nameonly

        print('Running job for:' + fulldbfspath)
        values = {'run_name': name, 'existing_cluster_id': clusterid, 'timeout_seconds': 3600, 'spark_python_task': {'python_file': fulldbfspath}}

        resp = requests.post('https://' + shard + '.cloud.databricks.com/api/2.0/jobs/runs/submit',
                             data=json.dumps(values), auth=(username, password))
        runjson = resp.text
        d = json.loads(runjson)
        runid = d['run_id']

        i=0
        waiting = True
        while waiting:
            time.sleep(10)
            jobresp = requests.get('https://' + shard + '.cloud.databricks.com/api/2.0/jobs/runs/get?run_id='+str(runid),
                             data=json.dumps(values), auth=(username, password))
            jobjson = jobresp.text
            j = json.loads(jobjson)
            print(jobjson)
            current_state = j['state']['life_cycle_state']
            runid = j['run_id']
            if current_state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'] or i >= 12:
                break
            i=i+1

        if outfilepath != '':
            file = open(outfilepath + os.sep +  str(runid) + '.json', 'w')
            file.write(json.dumps(j))
            file.close()


if __name__ == '__main__':
    main()
