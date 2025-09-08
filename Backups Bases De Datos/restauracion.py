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
import gzip
import shutil
import time

# Redirigir salida estándar a un archivo de log
log_file_path = '/home/backups/res_log.txt'  # Ruta del archivo de log
log_file = open(log_file_path, 'a')  # Abrir el archivo en modo append
sys.stdout = log_file  # Redirigir salida estándar al archivo de log

def decryptBackup(file_path):
    """
    Descifra un archivo .enc generado con OpenSSL AES-256-CBC
    Retorna la ruta del archivo descifrado.
    """
    if not file_path.endswith(".enc"):
        return file_path  # No está cifrado, devolver original

    decrypted_file = file_path.replace(".enc", "")

    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
    if not ENCRYPTION_KEY:
        raise ValueError("⚠️ No se encontró la variable de entorno ENCRYPTION_KEY")

    try:
        decrypt_cmd = (
            f'openssl enc -d -aes-256-cbc -pbkdf2 -in "{file_path}" '
            f'-out "{decrypted_file}" -k "{ENCRYPTION_KEY}"'
        )
        subprocess.check_call(decrypt_cmd, shell=True)
        print(f"✅ Backup descifrado en: {decrypted_file}")
        return decrypted_file
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al descifrar backup: {e}")
        return None

def detectBackupType(file_path):
    """
    Detecta si un archivo de backup es SQL plano o dump de PostgreSQL (custom).
    """
    try:
        out = subprocess.check_output(["file", file_path], text=True)
        if "PostgreSQL custom database dump" in out:
            return "custom"
        elif "ASCII text" in out or file_path.endswith(".sql"):
            return "sql"
        else:
            return "desconocido"
    except Exception as e:
        print(f"⚠️ No se pudo detectar el tipo de backup: {e}")
        return "desconocido"

def restoreDB(nomDatabase, archivo, max_retries=5, wait_time=60):
    """
    Intenta restaurar la base de datos con un número de reintentos en caso de que el servidor esté en modo de recuperación.
    """
    if not archivo:
        print("❌ No se recibió un archivo válido para restaurar.")
        return

    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = "User*2018"

    # Paso 1: DROP + CREATE DATABASE
    retries = 0
    success = False

    while retries < max_retries and not success:
        dropCreateCmd = f'psql -h 172.16.10.200 -p 5555 -U postgres -d postgres -c "DROP DATABASE IF EXISTS \\"{nomDatabase}\\" WITH (FORCE);" -c "CREATE DATABASE \\"{nomDatabase}\\";"'
        dropCreateOutput = subprocess.run(shlex.split(dropCreateCmd), capture_output=True, text=True, env=env_vars)
        print(dropCreateOutput.stdout)
        print(dropCreateOutput.stderr)

        if "the database system is in recovery mode" in dropCreateOutput.stderr:
            print(f"⚠️ Servidor en modo recovery. Reintentando en {wait_time} segundos...")
            retries += 1
            time.sleep(wait_time)
        else:
            success = True

    if not success:
        print(f"❌ No se pudo crear la base de datos después de {max_retries} intentos.")
        return

    # Paso 2: Detectar tipo de backup y restaurar
    tipo = detectBackupType(archivo)
    if tipo == "custom":
        restoreCmd = f'pg_restore -h 172.16.10.200 -p 5555 -U postgres -d "{nomDatabase}" --verbose "{archivo}"'
    elif tipo == "sql":
        restoreCmd = f'psql -h 172.16.10.200 -p 5555 -U postgres -d "{nomDatabase}" -f "{archivo}"'
    else:
        print("❌ Tipo de backup desconocido, no se puede restaurar.")
        return

    retries = 0
    success = False
    while retries < max_retries and not success:
        restoreOutput = subprocess.run(restoreCmd, shell=True, capture_output=True, text=True, env=env_vars)
        print(restoreOutput.stdout)
        print(restoreOutput.stderr)

        if "the database system is in recovery mode" in restoreOutput.stderr:
            print(f"⚠️ Servidor en modo recovery. Reintentando en {wait_time} segundos...")
            retries += 1
            time.sleep(wait_time)
        else:
            success = True

    if not success:
        print(f"❌ No se pudo restaurar la base de datos después de {max_retries} intentos.")

def findLast(directorio):
    archivos = [os.path.join(directorio, archivo) for archivo in os.listdir(directorio)]
    archivos = [archivo for archivo in archivos if os.path.isfile(archivo)]
    if not archivos:
        return None
    return max(archivos, key=os.path.getmtime)

def compress_file(file_path):
    compressed_file = f"{file_path}.gz"
    with open(file_path, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return compressed_file

def sendEmail(horaIRes, horaFRes, log_file_path, result1):
    compressed_log_file = compress_file(log_file_path)
    fecha = datetime.datetime.now().strftime('%d-%m-%y')
    contenido = f"""
    Cordial Saludo,

    Me permito enviar el estado de restauración de backups diaria, generada de manera automática:
    
    Fecha: {fecha}
    
    Hora de inicio restauración: {horaIRes}
    Hora de finalización restauración: {horaFRes}

    Muchas gracias.

    --------------------------------
    Espacio en la carpeta Restauración: 
    --------------------------------
    
    {result1}
    
    --------------------------------
    """
    titulo = f"Informe Restauración Backups {fecha}"
    msg = EmailMessage()
    msg['Subject'] = titulo
    msg['From'] = 'Informes Seguridad Informática'
    msg['To'] = 'mcasanova@compuconta.com', 'jcasanova@compuconta.com', 'aleiva@compuconta.com'
    msg.set_content(contenido)

    try:
        with open(compressed_log_file, 'rb') as f:
            file_data = f.read()
            file_name = "res_log.txt.gz"
        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
    except Exception as e:
        print(f'Error al adjuntar el archivo: {e}')

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    email_user = 'ingenieria@compuconta.com'
    email_pass = 'qujxzylvpbfdajxi'

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)
            print('📧 Correo enviado con éxito.')
    except Exception as e:
        print(f'❌ Error al enviar el correo: {e}')

# ================================
# MAIN
# ================================
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
cursor.execute("SELECT id,nombre, ip, descripcion,port, db, url  FROM public.entidades WHERE backup=true ORDER BY id ASC  ")
rows = cursor.fetchall()

horaInicio = datetime.datetime.now().strftime('%H:%M:%S')

print("================================================")
print("INICIA:", horaInicio)
print("================================================")

system("echo "" > /home/backups/res_log.txt")  # Borrar Logs anteriores

for row in rows:
    id_entidad, name, ip, descripcion, port, db, url = row
    entidad = name
    print("================================================")
    print("ENTIDAD:", entidad)
    print("================================================")

    BACKUP_DIR = '/home/backups/' + entidad
    if __name__ == '__main__':
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)

        archivo_reciente = findLast(BACKUP_DIR)
        archivo_descifrado = decryptBackup(archivo_reciente)

        if archivo_descifrado:
            restoreDB(entidad, archivo_descifrado)

    print("================================================")

horaFin = datetime.datetime.now().strftime('%H:%M:%S')
conn.close()

print("================================================")
print("TERMINA:", horaFin)
print("================================================")

resultado1 = subprocess.run(['df', '-h', '/dev/sdc1'], stdout=subprocess.PIPE, text=True)
salida1 = resultado1.stdout
sendEmail(horaInicio, horaFin, log_file_path, salida1)

log_file.close()
sys.stdout = sys.__stdout__

#Verificación de bases de datos
subprocess.run(['python3', '/var/www/html/verificacionBK.py'], stdout=subprocess.PIPE, text=True)
