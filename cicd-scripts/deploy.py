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
    workspacepath = ''
    releasefile = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:u:p:lwf',
                                   ['shard=', 'username=', 'password=', 'localpath=', 'workspacepath=', 'releasefile='])
    except getopt.GetoptError:
        print(
            'deploy.py -u <username> -p <password> -s <shard> -l <localpath> -w <workspacepath> -f <releasefile>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print (
                'deploy.py -u <username> -p <password> -s <shard> -l <localpath> -w <workspacepath> -f <releasefile>')
            sys.exit()
        elif opt in ('-s', '--shard'):
            shard = arg
        elif opt in ('-u', '--username'):
            username = arg
        elif opt in ('-p', '--password'):
            password = arg
        elif opt in ('-l', '--localpath'):
            localpath = arg
        elif opt in ('-w', '--workspacepath'):
            workspacepath = arg
        elif opt in ('-f', '--releasefile'):
            releasefile = arg

    print ('-s is ' + shard)
    print ('-u is ' + username)
    print ('-l is ' + localpath)
    print ('-w is ' + workspacepath)
    print ('-f is ' + releasefile)

    # localpath and releasefile as mutally excluisive as releasefile contains localpath
    if localpath != '' and releasefile != '':
        print('localpath and releasefile as mutally excluisive as releasefile contains localpath')
        print(
            'deploy.py -u <username> -p <password> -s <shard> (-l <localpath> -w <workspacepath>) or (-f <releasefile>)')
        sys.exit()
    elif localpath != '':
        deploy_path(shard, username, password, localpath, workspacepath)
    elif releasefile != '':
        deploy_file(shard, username, password, localpath)


def deploy_path(shard, username, password, localpath, workspacepath):
    # Generate array from walking local path
    notebooks = []
    for path, subdirs, files in os.walk(localpath):
        for name in files:
            fullpath = path + '/' + name
            # removes localpath to repo but keeps workspace path
            fullworkspacepath = workspacepath + path.replace(localpath, '')

            name, file_extension = os.path.splitext(fullpath)
            if file_extension.lower() in ['.scala', '.sql', '.r', '.py']:
                row = [fullpath, fullworkspacepath]
                notebooks.append(row)

    deploy(shard, username, password, notebooks)


def deploy_file(shard, username, password, releasefile):
    # Generate array from file

    notebooks = []
    with open(releasefile) as csvfile:
        reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)  # change contents to floats
        for row in reader:  # each row is a list
            notebooks.append(row)

    deploy(shard, username, password, notebooks)


def deploy(shard, username, password, notebooks):
    # Deploy each element in array
    for notebook in notebooks:

        fullpath = notebook[0]
        nameonly = os.path.basename(notebook[0])
        workspacepath = notebook[1]

        fullworkspacepath = workspacepath
        print('Creating directory at Workspace path:' + fullworkspacepath)
        try:
            values = {'path': fullworkspacepath}
            resp = requests.post('https://' + shard + '.cloud.databricks.com/api/2.0/workspace/mkdirs',
                                 data=json.dumps(values), auth=(username, password))
            print(resp.json())
            print(resp.status_code)
        except RESOURCE_ALREADY_EXISTS:
            print('Not created. Already exists.')

        print(fullpath)
        if os.path.isfile(fullpath):
            name, file_extension = os.path.splitext(nameonly)
            file_extension = file_extension.replace('.', '')

            # workpath removes extension
            fullworkspacepath = workspacepath + '/' + name

            # sourcetype is same as extesion except for python, which has a py extension
            sourcetype = file_extension.upper() if file_extension.upper() != 'PY' else 'PYTHON'

            if file_extension.lower() in ['scala', 'sql', 'r', 'py']:
                print('Importing file:' + fullpath + ' to Workspace path:' + fullworkspacepath)
                files = {'content': open(fullpath, 'rb')}
                values = {'path': fullworkspacepath, 'language': sourcetype, 'overwrite': 'true', 'format': 'SOURCE'}
                resp = requests.post('https://' + shard + '.cloud.databricks.com/api/2.0/workspace/import', files=files,
                                     data=values, auth=(username, password))
                print(resp.text)


if __name__ == '__main__':
    main()
