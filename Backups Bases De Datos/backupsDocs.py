import os 
import datetime # OBTENER FECHA (MANEJO DEL TIEMPO)
import subprocess # EJECUTAR SUBPROCESOS (EJECUTAR COMANDOS EN SHELL)
import psycopg2 # CONECTAR BASE DE DATOS
import smtplib
import gzip
import shutil
import sys
from email.message import EmailMessage
from os import system

# === LOG GLOBAL ===
LOG_FILE = "/home/backups/docs_log.txt"

# Redirigir todos los print() al archivo
sys.stdout = open(LOG_FILE, "a")   # "a" = append, si quieres sobreescribir usa "w"

def compress_file(file_path):
    compressed_file = f"{file_path}.gz"
    with open(file_path, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return compressed_file

def guardarRegistro(nombre, archivo, date):
    conn1 = psycopg2.connect(
        host="172.16.10.200",
        dbname="DashBoard",
        user="postgres",
        password="User*2018",
        port="5433"
    )
    cur1 = conn1.cursor()

    cur1.execute(f"SELECT id FROM copias_seguridad.entidades_nube where nombre = '{nombre}'")
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
        (entidad, archivo, date, peso, "OpenSSL", "Docs")
    )
    conn1.commit()
    print(f"Insertado: {entidad}")

def encryptBackup(file_path):
    encrypted_file = file_path + ".enc"
    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
    if not ENCRYPTION_KEY:
        raise ValueError("⚠️ No se encontró la variable de entorno ENCRYPTION_KEY")
    try:
        encrypt_cmd = (
            f'openssl enc -aes-256-cbc -pbkdf2 -salt '
            f'-in "{file_path}" -out "{encrypted_file}" -k "{ENCRYPTION_KEY}"'
        )
        subprocess.check_call(encrypt_cmd, shell=True)
        print(f"✅ Backup cifrado en: {encrypted_file}")
        os.remove(file_path)  # eliminar original
        return encrypted_file
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al cifrar backup: {e}")
        return None

def createBackup():
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f'{entidad}_Docs_{current_time}.backup'
    backup_filepath = os.path.join(BACKUP_DIR, backup_filename)
     
    pg_dump_cmd = (
        f'PGPASSWORD="{DB_PASSWORD}" pg_dump --host {DB_HOST} --port {DB_PORT} '
        f'--username "{DB_USER}" --format custom --blobs --verbose '
        f'--file "{backup_filepath}" "{DB_NAME}"'
    )

    try:
        print("================================================")
        print(f'✅ Respaldo creado en: {backup_filepath}')
        print("================================================")

        subprocess.check_call(pg_dump_cmd, shell=True)

        encryptBackup(backup_filepath)
        ruta = backup_filepath + ".enc"
        guardarRegistro(entidad, ruta, current_time)

    except subprocess.CalledProcessError as e:
        print(f'❌ Error al crear el respaldo: {e}')

def sendEmail(horaIni, horaFin, log_file_path, result):
    compressed_log_file = compress_file(log_file_path)
    fecha = datetime.datetime.now().strftime('%d-%m-%y')
    contenido = f"""
    Cordial Saludo,

    Me permito enviar el estado de backups diario generado de manera automática:
    
    Fecha: {fecha} 

    Hora de inicio backups: {horaIni}
    Hora de finalización backups: {horaFin}
    
    Se adjuntan los logs.

    Muchas gracias.
    
    --------------------------------
    Espacio en la carpeta Backups: 
    --------------------------------
    
    {result}
    
    --------------------------------
    """
    titulo= f"Informe Generación Backups de Documentos {fecha}"
    msg = EmailMessage()
    msg['Subject'] = titulo
    msg['From'] = 'Informes Seguridad Informática'
    msg['To'] = 'mcasanova@compuconta.com'
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
print("INICIA:", horaInicio )
print("================================================")

# Reiniciar logs (vaciar archivo antes de iniciar)
open(LOG_FILE, "w").close()

for row in rows:
    id_entidad, name, ip, descripcion, port, db, url  = row
    DB_HOST = ip  
    DB_PORT = port       
    DB_NAME = "CompucontaDocs"     
    entidad = name 
    BACKUP_DIR = '/home/backups/'+entidad+'/Docs'
    
    if __name__ == '__main__':
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
        createBackup()
    
horaFin = datetime.datetime.now().strftime('%H:%M:%S')

resultado = subprocess.run(['du', '-h', '/home/backups'], stdout=subprocess.PIPE, text=True)
salida = resultado.stdout

conn.close()

print("================================================")
print("TERMINA:", horaFin )
print("================================================")

sendEmail(horaInicio, horaFin, LOG_FILE, salida)

subprocess.run(['python3', '/home/mcasanova/proyectos/prueba/restauracionDocs.py'], stdout=subprocess.PIPE, text=True)
