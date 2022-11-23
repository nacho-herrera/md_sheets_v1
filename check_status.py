#!/usr/bin/env python3
import datetime
import requests
from telegram import Telegram
import parameters
import time
from os import kill
from os import getpid
from signal import SIGKILL
import psutil
fechas = None

sheets_workbook = parameters.sheets_workbook
sheets_worksheet = parameters.sheets_worksheet
sheets_ranges = parameters.sheets_ranges[0]



url = f"https://docs.google.com/spreadsheets/d/{sheets_workbook}/gviz/tq?tqx=out:csv&sheet={sheets_worksheet}&range={sheets_ranges}"
r = requests.get(url)
# start = datetime.datetime.now()

fechas = r.text
if fechas:
    actualizado = False
    fechas = [[x.strip('"') for x in fecha.split(",")][0] for fecha in fechas.splitlines()]
    fechas_unicas = list(set(fechas))

    ## feriados o cuando las fechas estasn en cero (todas en 31/12/1969)
    if len(fechas_unicas) == 1:
        actualizado = True
    else:
        ahora_dt = datetime.datetime.now()
        for fecha in fechas_unicas:
            f = datetime.datetime.strptime(fecha, "%d/%m/%Y %H:%M:%S")
            if (ahora_dt - f) < datetime.timedelta(hours=3, minutes=5):  ## 5 minutos sin actualizacion + GMT
                actualizado = True
                break

    if not actualizado:
        telegram = Telegram(parameters.telegram_bot_key, parameters.telegram_group_id)
        telegram.error_message("Stalled. Reiniciando")
        pid = getpid()
        for x in psutil.process_iter(['pid'], ['name']):
                if x.info['pid'] != pid and x.name() == 'python3':
                        kill(x.info['pid'], SIGKILL)

time.sleep(200)

