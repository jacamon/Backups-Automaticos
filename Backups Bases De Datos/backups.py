# LIBRERIAS NECESARIAS
import os
import sys
import datetime
import subprocess
import psycopg2
import smtplib
import requests
from email.message import EmailMessage
from os import system
import zipfile


def guardarRegistro(nombre, archivo, date):
    conn1 = psycopg2.connect(
        host="172.16.10.200",
        dbname="DashBoard",
        user="postgres",
        password="User*2018",
        port="5433"
    )
    cur1 = conn1.cursor()

    cur1.execute(f"SELECT id FROM copias_seguridad.entidades_nube WHERE nombre = %s", (nombre,))
    row = cur1.fetchone()
    if row:
        entidad = row[0]
    else:
        raise ValueError(f"No se encontró entidad con nombre: {nombre}")

    peso = os.path.getsize(archivo)

    cur1.execute(
        """
        INSERT INTO copias_seguridad.registro_backups
        (id_entidad_id, archivo, fecha, peso, encriptacion, log)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (entidad, archivo, date, peso, "OpenSSL", "CC")
    )

    conn1.commit()
    conn1.close()
    print(f"Insertado: {entidad}")


def encryptBackup(file_path):
    encrypted_file = f"{file_path}.enc"
    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
    if not ENCRYPTION_KEY:
        raise ValueError("⚠️ No se encontró la variable ENCRYPTION_KEY")

    try:
        encrypt_cmd = (
            f'openssl enc -aes-256-cbc -pbkdf2 -salt -in "{file_path}" '
            f'-out "{encrypted_file}" -k "{ENCRYPTION_KEY}"'
        )
        subprocess.check_call(encrypt_cmd, shell=True)
        os.remove(file_path)
        print(f"Backup cifrado correctamente: {encrypted_file}")
        return encrypted_file
    except subprocess.CalledProcessError as e:
        print(f"Error al cifrar el backup: {e}")
        return None


def createBackup():
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f'{entidad}_{current_time}.backup'
    backup_filepath = os.path.join(BACKUP_DIR, backup_filename)

    pg_dump_cmd = (
        f'PGPASSWORD="{DB_PASSWORD}" pg_dump --host {DB_HOST} --port {DB_PORT} '
        f'--username "{DB_USER}" --format custom --blobs --verbose '
        f'--file "{backup_filepath}" "{DB_NAME}"'
    )

    try:
        print("================================================")
        print(f'Respaldo iniciado en: {descripcion}')
        print("================================================")

        proceso=subprocess.check_call(pg_dump_cmd, shell=True)
        print(proceso)
       
        print("================================================")

        encryptBackup(backup_filepath)

        ruta = backup_filepath + ".enc"
        if version and (version == "CC3" or version == "CC2"):
            print("------------------------------------------------")
            guardarRegistro(entidad, ruta, current_time)

        print("================================================")

    except subprocess.CalledProcessError as e:
        print(f'!!!!! Error al crear el respaldo: {e}')


def sendEmail(horaIni, horaFin, log_file_path, result):
    fecha = datetime.datetime.now().strftime('%d-%m-%y')
    contenido = f"""
    Cordial Saludo,

    Me permito enviar el estado de backups diario generado de manera automática:
    
    Fecha: {fecha} 

    Hora de inicio backups: {horaIni}
    Hora de finalización backups: {horaFin}
    
    Se adjunta archivo comprimido con los logs.

    Muchas gracias.
    
    --------------------------------
    Espacio en la carpeta Backups: 
    --------------------------------
    
    {result}
    
    --------------------------------
    """
    titulo = f"Informe Generación Backups {fecha}"
    msg = EmailMessage()
    msg['Subject'] = titulo
    msg['From'] = 'Informes Seguridad Informática'
    msg['To'] = 'mcasanova@compuconta.com', 'jcasanova@compuconta.com', 'aleiva@compuconta.com'
    msg.set_content(contenido)

    # Comprimir el log en .zip
    zip_filename = "/tmp/result.zip"
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(log_file_path, arcname="result.txt")

        with open(zip_filename, 'rb') as f:
            file_data = f.read()
        msg.add_attachment(file_data, maintype='application', subtype='zip', filename="result.zip")

    except Exception as e:
        print(f'Error al preparar el archivo de log: {e}')
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
            print('Correo enviado con éxito.')
    except Exception as e:
        print(f'Error al enviar el correo: {e}')


# ===============================
# VARIABLES PRINCIPALES
# ===============================
DB_USER = 'postgres'
DB_PASSWORD = 'User*2018'

# Redirigir todos los print a result.txt
log_file_path = "/home/backups/result.txt"
sys.stdout = open(log_file_path, "w")

conn = psycopg2.connect(
    host="172.16.10.200",
    port="5433",
    database="FICompuconta",
    user="postgres",
    password="User*2018"
)
cursor = conn.cursor()

cursor.execute("""
    SELECT id, nombre, ip, descripcion, port, db, url, version
    FROM public.entidades
    WHERE backup = true 
    ORDER BY id ASC
""")

rows = cursor.fetchall()

horaInicio = datetime.datetime.now().strftime('%H:%M:%S')
print("================================================")
print("INICIA:", horaInicio)
print("================================================")

for row in rows:
    id_entidad, name, ip, descripcion, port, db, url, version = row
    DB_HOST = ip
    DB_PORT = port
    DB_NAME = db
    entidad = name
    BACKUP_DIR = '/home/backups/' + entidad

    if __name__ == '__main__':
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
        createBackup()

horaFin = datetime.datetime.now().strftime('%H:%M:%S')

resultado = subprocess.run(['du', '-h', '/home/backups'], stdout=subprocess.PIPE, text=True)
salida = resultado.stdout

conn.close()

print("================================================")
print("TERMINA:", horaFin)
print("================================================")

# Restaurar stdout a consola
sys.stdout.close()
sys.stdout = sys.__stdout__

# Enviar correo con log comprimido
sendEmail(horaInicio, horaFin, log_file_path, salida)

# Ejecutar restauración
subprocess.run(['python3', '/home/mcasanova/proyectos/restauracion.py'], stdout=subprocess.PIPE, text=True)
