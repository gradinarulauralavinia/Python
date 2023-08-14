import os
import csv
import json
import requests
import threading
import schedule
import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request


class Conectare_la_baza_de_date:
    #metoda statica apelabila fara instantiere a clasei
    @staticmethod
    def _creeaza_conexiune():
        connection = None
        try:
            connection = mysql.connector.connect(
                host='localhost',
                database='grd_mydb',
                user='gradinaru',
                password='GradinaruLL123#'
            )
            if connection.is_connected():
                print('Conexiunea la baza de date a fost stabilita cu succes.')
                return connection
        except Error as e:
            print(f'Eroare la conectare baza date: {e}')

class Access:
    def __init__(self, id_persoana, ora_validare, sens):
        self.id_persoana = id_persoana
        self.ora_validare = ora_validare
        self.sens = sens

    def salveaza_acces_in_baza_de_date(self):
        connection = Conectare_la_baza_de_date._creeaza_conexiune()
        if connection:
            try:
                cursor = connection.cursor()
                query = "INSERT INTO access (id_persoana, ora_validare, sens) VALUES (%s, %s, %s)"
                cursor.execute(query, (self.id_persoana, self.ora_validare, self.sens))
                connection.commit()
                print('Datele au fost inserate in baza de date.')
            except Error as e:
                print(f'Eroare la inserare in baza de date: {e}')
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()


class Poarta:
    def __init__(self, numar, tip):
        self._numar = numar
        self._tip = tip
    #incapsulare cu getter si setter
    @property
    def numar(self):
        return self._numar

    @numar.setter
    def numar(self, numar):
        self._numar = numar

    @property
    def tip(self):
        return self._tip

    @tip.setter
    def tip(self, tip):
        self._tip = tip
    
    def scrie_in_fisier_txt(self, id_persoana, data_ora, tip):
        file_name = f"Poarta{self.numar}.txt"
        file_path = os.path.abspath(file_name)
        with open(file_name, 'a') as file:
            line = f"{id_persoana}, {data_ora}, {tip}\n"
            file.write(line)
        print(f'Fisierul {file_name} a fost scris cu succes aici: '+ file_path)
            
    def scrie_in_fisier_csv(self, id_persoana, data_ora, tip):
        file_name = f"Poarta{self.numar}.csv"
        file_path = os.path.abspath(file_name)
        with open(file_name, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([id_persoana, data_ora, tip])
        print(f'Fisierul {file_name} a fost scris cu succes aici: '+ file_path)
        
    def proceseaza_director(self, folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                self.proceseaza_fisier(file_path)

    def proceseaza_fisier(self, file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                id_persoana = row[0]
                ora_validare = row[1]
                sens = row[2]
                access = Access(id_persoana, ora_validare, sens)
                access.salveaza_acces_in_baza_de_date()
        
        file_name = os.path.basename(file_path)
        backup_dir = 'backup_intrari'
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Append the timestamp to the file name
        new_file_name = f"{timestamp}_{file_name}"
        new_file_path = os.path.join(backup_dir, new_file_name)
        os.rename(file_path, new_file_path)
        print(f'Fisierul {file_name} a fost procesat si mutat in directorul de backup:'+new_file_path)

#clasa Server mosteneste clasa Poarta
class Server(Poarta):
    def __init__(self, numar, tip):
        super().__init__(numar, tip)
    
    #polimorfism deoarece aceeasi metoda are implementari diferite in clase diferite
    def proceseaza_fisier(self, json_data):
        data = json.loads(json_data)
        id_persoana = data['idPersoana']
        ora_validare = data['data']
        sens = data['sens']
        access = Access(id_persoana, ora_validare, sens)
        access.salveaza_acces_in_baza_de_date()
        print('Datele in format JSON au fost procesate si salvate in baza de date.')

class Manager:
    def __init__(self):
        pass

    def calculeaza_ore_lucrate(self):
        connection = Conectare_la_baza_de_date._creeaza_conexiune()
        if connection:
            try:
                cursor = connection.cursor()
                query = "SELECT id_persoana, DATE(ora_validare), MIN(ora_validare) AS intrare, MAX(ora_validare) AS iesire FROM access GROUP BY id_persoana, DATE(ora_validare)"
                cursor.execute(query)
                result = cursor.fetchall()
                for row in result:
                    id_persoana = row[0]
                    data = row[1]
                    intrare = row[2]
                    iesire = row[3]
                    intrare=datetime.strptime(intrare, "%Y-%m-%dT%H:%M:%S.%fZ")
                    iesire=datetime.strptime(iesire, "%Y-%m-%dT%H:%M:%S.%fZ")
                    print(f'intrare: {intrare}')
                    print(f'iesire: {iesire}')
                    hours_worked = (iesire - intrare).total_seconds() / 3600
                    if hours_worked < 8:
                        self.trimite_email(id_persoana, data)
                    
            except Error as e:
                print(f'Eroare la calcularea orelor lucrate: {e}')
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()

    def trimite_email(self, id_persoana, data):
        connection = Conectare_la_baza_de_date._creeaza_conexiune()
        if connection:
            try:
                cursor = connection.cursor()
                query = "SELECT nume, prenume, email FROM persoane WHERE id = %s"
                cursor.execute(query, (id_persoana,))
                result = cursor.fetchone()
                if result is not None:
                    nume = result[0]
                    prenume = result[1]
                    email = result[2]

                    subject = f'Ore lucrate insuficiente {data}'
                    message = f'Dear {nume} {prenume},\n\nAti lucrat mai putin de 8 ore in data {data}. Asigurati-va ca lucrati timp de 8 ore zilnic.\n\nCu stima,\nManager'
                    msg = MIMEMultipart()
                    msg['From'] = 'manager@company.com'
                    msg['To'] = email
                    msg['Subject'] = subject
                    msg.attach(MIMEText(message, 'plain'))

                    try:
                        server = smtplib.SMTP('smtp.gmail.com', 587)
                        server.starttls()
                        server.login('your_email@gmail.com', 'your_password')
                        server.send_message(msg)
                        server.quit()
                        print(f'Notificare pe email trimisa catre {nume} {prenume} ({email})')
                    #interceptare erori de autentificare la serverul SMTP
                    except smtplib.SMTPAuthenticationError as e0:
                        print(f'Eroare la server-ul SMTP: {e0}')
            except Error as e:
                print(f'Eroare in trimiterea notificarii: {e}')
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()

#Creare endpoint cu server Flask   
app = Flask(__name__)

@app.route('/inregistrare', methods=['POST'])
def inregistrare():
    if request.method == 'POST':
        data = request.get_json()
        connection=Conectare_la_baza_de_date._creeaza_conexiune()
        cursor=connection.cursor()
        query = """INSERT INTO persoane (nume, prenume, companie, email, id_manager) VALUES (%s, %s, %s, %s, %s)"""
        values = (
            data['nume'],
            data['prenume'],
            data['companie'],
            data['email'],
            data['id_manager']
        )

        try:
            # Executa interogarea SQL
            cursor.execute(query, values)

            # Efectueaza modificarile in baza de date
            connection.commit()

            # Inchide cursor si conexiune la baza de date
            cursor.close()
            connection.close()

            return 'Inregistrare cu succes in baza de date'
        except Exception as e:
            # Interceptare erori in cursul derularii interogarii SQL
            print(f'Eroare la inserarea in baza de date: {str(e)}')
            connection.rollback()
            cursor.close()
            connection.close()
            return 'Eroare la inregistrarea persoanei'
    else:
        return 'Metoda de request invalida'
        
    

def inregistrare_utilizator():
    data = {
        "nume": "Ionescu",
        "prenume": "Petre",
        "companie": "Geriloss srl",
        "email": "ionescu.petre@gmail.com",
        "id_manager": 1
    }
    url = 'http://localhost:5000/inregistrare'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        print('Persoana inregistrata cu succes!')
    else:
        print('Eroare la inregistrarea persoanei!')
        # Iesire fortata daca request-ul are erori
        exit()

def start_server():
    app.run()

def rulare_periodica_automata():
    manager = Manager()
    manager.calculeaza_ore_lucrate()
    print("Rulez calcul ore lucrate zilnic la ora 20:00...")

if __name__ == '__main__':
    server_thread = threading.Thread(target=start_server)
    server_thread.start()
    inregistrare_utilizator()


#Exemplu utilizare

gate = Poarta(1, 'in')
#gate.scrie_in_fisier_txt(1,'2024-05-22T14:23:42.153Z','in')
gate.scrie_in_fisier_csv(1,'2024-05-22T14:23:42.153Z','in')
gate.proceseaza_director('intrari')

#server poate fi asimilat cu Poarta2
server = Server(1, 'in')
json_data = '{"idPersoana": 1, "data": "2024-05-22T14:23:42.153Z", "sens": "in"}'
server.proceseaza_fisier(json_data)

#functionalitate care ruleaza zilnic la ora 20:00
schedule.every().day.at("20:00").do(rulare_periodica_automata)
while True:
    schedule.run_pending()
    time.sleep(1)
