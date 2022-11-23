# md_sheets_v1

Script para suscribirse a MD y publicarla en Google Drive.

Lo tengo corriendo en una VM en linux, lo ejecuto desde crontab. 

```
 * 13-20 * * 1-5 /home/md_sheets_epbg/python/opciones/run.sh 2>&1 | tee /tmp/md_sheets.log
 * 14-20 * * 1-5 /home/md_sheets_epbg/python/opciones/run_check.sh
```

 
