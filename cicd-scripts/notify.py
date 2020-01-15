#!/usr/bin/python3
import json
import requests
import sys
import getopt
import os
import glob


def main():
    slackurl = ''
    message = ''
    outputpath = ''
    channel = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:cmp',
                                   ['slackurl=', 'channel=', 'message=', 'outputpath='])
    except getopt.GetoptError:
        print(
            'notify.py -s <slackurl> -c <channel> (-m <message> or -p <outputpath>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                'notify.py -s <slackurl> -c <channel> (-m <message> or -p <outputpath>)')
            sys.exit()
        elif opt in ('-s', '--slackurl'):
            slackurl = arg
        elif opt in ('-m', '--message'):
            message = arg
        elif opt in ('-p', '--outputpath'):
            outputpath = arg
        elif opt in ('-c', '--channel'):
            channel = arg

    print('-s is ' + slackurl)
    print('-m is ' + message)
    print('-p is ' + outputpath)
    print('-c is ' + channel)


    if message != '' and outputpath == '':
        send_slack(slackurl, message, '', channel)
    if outputpath != '':
        send_output_slacks(slackurl, message, outputpath, channel)


def send_output_slacks(slackurl, message, outputpath, channel):

    attachments = []

    for filename in glob.glob(os.path.join(outputpath, '*.json')):
        data = json.load(open(filename))
        runid  = data['run_id']
        status = data['state']['result_state']
        state_message = data['state']['state_message']
        task = data['task']['notebook_task']['notebook_path']
        run_name = data['run_name']
        run_url = data['run_page_url']
        execution_duration = data['execution_duration']
        starttime = ['start_time']

        runstats = {
            "fallback": run_name + ' ' + run_url,
            "color": "#36a64f",
            "pretext": "Run ID:" + str(runid),
            "author_name": run_name,
            "author_link": run_url,
            "title": task,
            "title_link": run_url,
            "text": "Optional text that appears within the attachment",
            "fields": [
                {
                    "title": "Result State",
                    "value": status,
                    "short": "true"
                },
                {
                    "title": "Result State Message",
                    "value": state_message,
                    "short": "true"
                },
                {
                    "title": "Execution Duration",
                    "value": execution_duration,
                    "short": "true"
                }

            ],
            "footer": "Databricks CSE Team",
            "ts": starttime
        }
        attachments.append(runstats)
        send_slack(slackurl, message, attachments, channel)

def send_slack(slackurl, message, attachments, channel):
    values = {'channel': channel, 'username': 'webhookbot', 'text': message, 'attachments': attachments, 'icon_emoji': ':databricks:'}

    resp = requests.post(slackurl, data=json.dumps(values))
    notifyjson = resp.text
    print(notifyjson)


if __name__ == '__main__':
    main()
