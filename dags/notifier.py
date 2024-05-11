from airflow.notifications.basenotifier import BaseNotifier
from airflow.models import Variable
import requests
import json


def send_message(title, message):
    user_id = Variable.get("line_user_id")
    channel_access_token = Variable.get("line_channel_access_token")
    payload = json.dumps({
      "to": user_id,
      "messages": [
        {
          "type": "text",
          "text": "[{}]\n{}".format(title,message)
        }
      ]
    })
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + channel_access_token
    }

    requests.request("POST", "https://api.line.me/v2/bot/message/push", headers=headers, data=payload)



class FailNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message):
        self.message = message

    def notify(self, context):
        # Send notification
        title = f"{context['dag'].dag_id} failed"
        send_message(title, self.message)


class SuccessNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message):
        self.message = message

    def notify(self, context):
        # Send notification
        title = f"{context['dag'].dag_id} success"
        send_message(title, self.message)
