# slack_integration.py allows the cluster to send messages to slack channels.

import json, requests, random
from time import ctime

from configs_parser import get_configs

success_emojis = [':slightly_smiling_face:', ':tada:', ':raised_hands:', ':+1:', ':party_parrot:', ':clap:',
                  ':robot_face:', ':checkered_flag:']
failure_emojis = [':disappointed:', ':face_with_raised_eyebrow:', ':confused:', ':rage:', ':skull_and_crossbones:',
                  ':no_mouth:', ':thinking_face:']


class SlackIntegration:

    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.slack_webhook = configs['slack_webhook']

    def notify_slack(self, data, message_type):
        payload = self._parse(data, message_type)
        response = requests.post(self.slack_webhook, json=payload)

    def _parse(self, data, message_type):

        if message_type == 'JOB_SUCCESS':
            job = data
            job_name = job['job_name']
            success_emoji = random.sample(success_emojis, 1)[0]
            s = 'SUCCESS: ' + job_name + ' ' + success_emoji + ' \n'
            for key in job:
                value = job[key]
                s = s + '\t' + key + ': ' + str(value) + '\n'

        elif message_type == 'JOB_FAILURE':
            job = data
            job_name = job['job_name']
            exception = job['exception']
            failure_emoji = random.sample(failure_emojis, 1)[0]
            s = 'FAILURE: ' + job_name + ' ' + failure_emoji + ' \n'
            for key in job:

                if key != 'exception':
                    value = job[key]
                    s = s + '\t' + key + ': ' + str(value) + '\n'

            s = s + 'EXCEPTION: ' + str(exception) + '\n'

        elif message_type == 'ATTEMPTING_JOB':
            job_name, timestamp = data
            s = 'ATTEMPTING JOB (' + timestamp + '): ' + job_name

        payload = {'text': s}

        return payload
