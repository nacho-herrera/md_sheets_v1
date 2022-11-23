# seleccion de modo. True no actualiza sheets y muestra el df en pantalla.
#DRY_RUN = True 
DRY_RUN = False 

# parametros xoms
api_url = # url api rest xoms 
ws_url =  # url api ws xoms
username = # username
password = # password
account = # nro de cuenta

# parametros sheets
sheets_credentials = # archivo json de sheets api
sheets_workbook =   # codigo de la planilla live
sheets_worksheet = # nombre de la hoja
sheets_ranges = ['A2:N1000', 'P2'] # rango a editar
sheets_workbook_test =   # codigo de la planilla testing

# notificaciones de Telegram
telegram_notifications = True
telegram_group_id = # id del grupo de telegram 
telegram_bot_key = # key del bot

# parametros logs
log_file = '/tmp/md.log' # logfile
log_rotation = True 
log_rotation_size = 1000000
log_rotation_backups = 3

# parametros horarios
WAIT_TIME = 5 # minutos
HORA_INICIO = "10:55:00" # inicio de rueda
HORA_FIN = "17:05:00"  # fin de rueda
