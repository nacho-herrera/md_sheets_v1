#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests


class Telegram:
    API_URL = "https://api.telegram.org/bot{bot_id}/sendMessage?chat_id={chat_id}&text={message}"

    def __init__(self, bot_id, chat_id):
        self.bot_id = bot_id
        self.chat_id = chat_id

    def send(self, message):
        url = self.API_URL.format(
            bot_id=self.bot_id, chat_id=self.chat_id, message=message)
        requests.get(url)

    def alert_message(self, text):
        message = '⚠️ - ' + str(text)
        self.send(message)

    def error_message(self, text):
        message = '🛑 - ' + text
        self.send(message)

    def notification_message(self, text):
        message = '🔔 - ' + text
        self.send(message)
