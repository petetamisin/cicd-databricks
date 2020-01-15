#!/usr/bin/python3
import json
import requests
import os
import sys
import getopt
import csv


def main():
    shard = ''
    username = ''
    password = ''
    localpath = ''
    dbfspath = ''
    releasefile = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:u:p:lwf',
                                   ['shard=', 'username=', 'password=', 'localpath=', 'dbfspath=', 'releasefile='])
    except getopt.GetoptError:
        print(
            'deploy.py -u <username> -p <password> -s <shard> -l <localpath> -w <dbfspath> -f <releasefile>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print (
                'deploy.py -u <username> -p <password> -s <shard> -l <localpath> -w <dbfspath> -f <releasefile>')
            sys.exit()
        elif opt in ('-s', '--shard'):
            shard = arg
        elif opt in ('-u', '--username'):
            username = arg
        elif opt in ('-p', '--password'):
            password = arg
        elif opt in ('-l', '--localpath'):
            localpath = arg
        elif opt in ('-w', '--dbfspath'):
            dbfspath = arg
        elif opt in ('-f', '--releasefile'):
            releasefile = arg

    print ('-s is ' + shard)
    print ('-u is ' + username)
    print ('-l is ' + localpath)
    print ('-w is ' + dbfspath)
    print ('-f is ' + releasefile)

    # localpath and releasefile as mutally excluisive as releasefile contains localpath
    if localpath != '' and releasefile != '':
        print('localpath and releasefile as mutally excluisive as releasefile contains localpath')
        print(
            'deploy.py -u <username> -p <password> -s <shard> (-l <localpath> -w <dbfspath>) or (-f <releasefile>)')
        sys.exit()
    elif localpath != '':
        deploy_path(shard, username, password, localpath, dbfspath)
    elif releasefile != '':
        deploy_file(shard, username, password, localpath)


def deploy_path(shard, username, password, localpath, dbfspath):
    # Generate array from walking local path
    sourcefiles = []
    for path, subdirs, files in os.walk(localpath):
        for name in files:
            fullpath = path + os.sep + name
            # removes localpath to repo but keeps dbfs path
            fulldbfspath = dbfspath + path.replace(localpath, '')
            fulldbfspath = fulldbfspath.replace(os.sep, '/')

            name, file_extension = os.path.splitext(fullpath)
            if file_extension.lower() in ['.scala', '.sql', '.r', '.py', '.sh', '.jar']:
                row = [fullpath, fulldbfspath]
                sourcefiles.append(row)

    deploy_dbfs(shard, username, password, sourcefiles)


def deploy_file(shard, username, password, releasefile):
    # Generate array from file

    sourcefiles = []
    with open(releasefile) as csvfile:
        reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)  # change contents to floats
        for row in reader:  # each row is a list
            sourcefiles.append(row)

    deploy(shard, username, password, sourcefiles)


def deploy_dbfs(shard, username, password, sourcefiles):
    # Deploy each element in array
    for sourcefile in sourcefiles:

        fullpath = sourcefile[0]
        nameonly = os.path.basename(sourcefile[0])
        dbfspath = sourcefile[1]

        fulldbfspath = dbfspath
        print('Creating directory at DBFS path:' + fulldbfspath)
        values = {'path': fulldbfspath}
        resp = requests.post('https://' + shard + '.cloud.databricks.com/api/2.0/dbfs/mkdirs',
                             data=json.dumps(values), auth=(username, password))
        print(resp.json())
        print(resp.status_code)

        print(fullpath)
        if os.path.isfile(fullpath):
            name, file_extension = os.path.splitext(nameonly)
            file_extension = file_extension.replace('.', '')

            # workpath removes extension
            fulldbfspath = dbfspath + '/' + nameonly

            if file_extension.lower() in ['scala', 'sql', 'r', 'py', 'sh', 'jar']:
                print('Importing file:' + fullpath + ' to DBFS path:' + fulldbfspath)
                files = {'content': open(fullpath, 'rb')}
                values = {'path': fulldbfspath, 'overwrite': 'true'}
                resp = requests.post('https://' + shard + '.cloud.databricks.com/api/2.0/dbfs/put', files=files,
                                     data=values, auth=(username, password))
                print(resp.text)


if __name__ == '__main__':
    main()
