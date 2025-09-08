# LIBRERIAS NECESARIAS
import os
from os import system
import shlex
import datetime  # OBTENER FECHA (MANEJO DEL TIEMPO)
import subprocess  # EJECUTAR SUBPROCESOS (EJECUTAR COMANDOS EN SHELL)
import psycopg2  # CONECTAR BASE DE DATOS
import smtplib
from email.message import EmailMessage
import sys  # Redirigir salida estándar

# === LOG GLOBAL ===
log_file_path = '/home/backups/resDocs_log.txt'

# Reiniciar logs (vaciar archivo antes de iniciar)
open(log_file_path, "w").close()

# Redirigir salida estándar a un archivo de log
log_file = open(log_file_path, 'a')
sys.stdout = log_file

def restoreDB(nomDatabase, archivo):
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = "User*2018"
    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

    # Drop y create DB
    dropCreateCmd = f'psql -h 172.16.10.200 -p 5555 -U postgres -d postgres -c "DROP DATABASE IF EXISTS \\"{nomDatabase}\\" WITH (FORCE);" -c "CREATE DATABASE \\"{nomDatabase}\\";"'
    print(subprocess.run(shlex.split(dropCreateCmd), env=env_vars))

    # 🔑 Restaurar leyendo el backup descifrado en streaming
    restoreCmd = f'openssl enc -d -aes-256-cbc -pbkdf2 -in "{archivo}" -k "{ENCRYPTION_KEY}" | docker exec -i produccion pg_restore -U postgres --verbose -d "{nomDatabase}"'
    print(subprocess.run(restoreCmd, shell=True, env=env_vars))


def findLast(directorio):
    archivos = [os.path.join(directorio, archivo) for archivo in os.listdir(directorio)]
    archivos = [archivo for archivo in archivos if os.path.isfile(archivo)]
    if not archivos:
        return None
    ultimo_archivo = max(archivos, key=os.path.getmtime)
    return ultimo_archivo

def sendEmail(horaIRes, horaFRes, log_file_path, result1):
    fecha = datetime.datetime.now().strftime('%d-%m-%y')
    contenido = f"""
    Cordial Saludo,

    Me permito enviar el estado de  restauracion de backups de documentos semanal, generada de manera automatica:
    
    Fecha: {fecha}
    
    Hora de inicio restauracion: {horaIRes}
    Hora de finalización restauracion: {horaFRes}

    Se adjuntan los logs.

    Muchas gracias.

    --------------------------------
    Espacio en la carpeta Restauracion: 
    --------------------------------
    
    {result1}
    
    --------------------------------
    """
    titulo = f"Informe Restauracion Documentos Backups {fecha}"
    msg = EmailMessage()
    msg['Subject'] = titulo
    msg['From'] = 'Informes Seguridad Informática'
    msg['To'] = 'mcasanova@compuconta.com'
    msg.set_content(contenido)

    try:
        with open(log_file_path, 'rb') as f:
            file_data = f.read()
            file_name = "res_log.txt"
        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
    except Exception as e:
        print(f'Error al adjuntar el archivo: {e}')
        return

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    email_user = 'ingenieria@compuconta.com'
    email_pass = 'qujxzylvpbfdajxi'

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)
            print('📩 Correo enviado con éxito.')
    except Exception as e:
        print(f'Error al enviar el correo: {e}')

# VARIABLES
DB_USER = 'postgres'
DB_PASSWORD = 'User*2018'
 
conn = psycopg2.connect(
    host="172.16.10.200",
    port="5433",
    database="FICompuconta",
    user="postgres",
    password="User*2018"
)

cursor = conn.cursor()
cursor.execute("SELECT id,nombre, ip, descripcion,port, db, url  FROM public.entidades WHERE version='CC3' and nube=TRUE ")
rows = cursor.fetchall()

horaInicio = datetime.datetime.now().strftime('%H:%M:%S')

print("================================================")
print("INICIA:", horaInicio)
print("================================================")

for row in rows:
    id_entidad, name, ip, descripcion, port, db, url = row
    DB_HOST = ip
    DB_PORT = port
    DB_NAME = db
    entidad = name
    print("================================================")
    print("ENTIDAD:", entidad)
    print("================================================")

    BACKUP_DIR = '/home/backups/' + entidad + "/Docs"

    if __name__ == '__main__':
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)

        archivo_reciente = findLast(BACKUP_DIR)
        restoreDB(entidad + "Docs", archivo_reciente)
    print("================================================")

horaFin = datetime.datetime.now().strftime('%H:%M:%S')

conn.close()

print("================================================")
print("TERMINA:", horaFin)
print("================================================")

resultado1 = subprocess.run(['df', '-h', '/dev/sdc1'], stdout=subprocess.PIPE, text=True)
salida1 = resultado1.stdout

sendEmail(horaInicio, horaFin, log_file_path, salida1)

# Cerrar log y restaurar stdout
log_file.close()
sys.stdout = sys.__stdout__

subprocess.run(['python3', '/var/www/html/verificacionBKDocs.py'], stdout=subprocess.PIPE, text=True)
