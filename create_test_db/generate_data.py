import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# ----------------------------------
# 1. DATENBANK-KONFIGURATION ANPASSEN
# ----------------------------------
HOST = "localhost"
USER = "root" # Ihr MySQL-Benutzername
PASSWORD = "" # Ihr MySQL-Passwort
DATABASE = "wws_test"
PORT = 3308 # Standard ist 3306, anpassen falls nötig
NUM_KUNDEN = 5000
NUM_PRODUKTE = 500
NUM_LIEFERANTEN = 50
NUM_BESTELLUNGEN = 5000
# ----------------------------------

fake = Faker('de_DE') 

# Feste PLZ/Ort Paare für konsistente Adressen
DE_ADDRESS_PAIRS = [
    ("10115", "Berlin"), ("80331", "München"), ("20095", "Hamburg"),
    ("50667", "Köln"), ("60306", "Frankfurt"), ("70173", "Stuttgart"),
    ("04103", "Leipzig"), ("40210", "Düsseldorf"), ("44135", "Dortmund"),
    ("30159", "Hannover"), ("90403", "Nürnberg"), ("28195", "Bremen")
]

def connect_db(db_name=DATABASE):
    """Stellt die Verbindung zur MySQL-Datenbank her."""
    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=db_name,
            port=PORT
        )
        return conn
    except mysql.connector.Error as err:
        # Nur printen, um den Ablauf nicht zu stoppen
        print(f"Fehler bei der Datenbankverbindung: {err}")
        return None

# --- Korrigierte Funktion zur Bereinigung und zum Zurücksetzen ---
def truncate_and_reset(conn):
    """Löscht alle Daten und setzt AUTO_INCREMENT-Zähler zurück. 
       Muss in der Reihenfolge Kind-Tabelle -> Eltern-Tabelle erfolgen."""
    cursor = conn.cursor()
    
    # KORREKTE Reihenfolge: Zuerst Kind-Tabellen, dann Eltern-Tabellen
    tables = [
        "bestellpositionen",     # Kind von bestellungen & produkte
        "bestellungen",          # Kind von kunden
        "produkt_lieferant",     # Kind von produkte & lieferanten
        "produkte",
        "kunden",
        "lieferanten"
    ]
    
    print("\n--- Datenbereinigung und Zähler-Reset ---")
    for table in tables:
        try:
            # TRUNCATE TABLE löscht Daten und setzt AUTO_INCREMENT zurück
            cursor.execute(f"TRUNCATE TABLE `{table}`")
            print(f"Tabelle '{table}' geleert und Zähler zurückgesetzt.")
        except mysql.connector.Error as err:
            print(f"Fehler beim Leeren von {table}: {err}")
            # Fahren Sie fort, um alle möglichen Tabellen zu leeren
            conn.rollback() # Rollback falls ein TRUNCATE fehlschlägt
    conn.commit()
    cursor.close()
    
# --- Generierungsfunktionen ---

def generate_kunden(conn, count):
    """Generiert Kundendaten."""
    cursor = conn.cursor()
    sql = "INSERT INTO kunden (vorname, nachname, email, strasse, plz, ort) VALUES (%s, %s, %s, %s, %s, %s)"
    data = []
    generated_emails = set()
    print(f"Generiere {count} Kunden...")
    
    while len(data) < count:
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Versuche, eine E-Mail zu generieren, bis sie einzigartig ist
        attempt_count = 0
        while True:
            unique_part = "" if attempt_count == 0 else str(random.randint(100, 999))
            email = f"{first_name}.{last_name}{unique_part}@{fake.domain_name()}".lower().replace(' ', '')
            
            if email not in generated_emails:
                generated_emails.add(email)
                break
            attempt_count += 1
            if attempt_count > 5: # Sicherheitsabbruch, falls Faker-Namen kollidieren
                email = fake.uuid4() + "@unique.com" # Notfall-Email
                generated_emails.add(email)
                break
        
        # Verwende konsistente Adresspaare
        plz, ort = random.choice(DE_ADDRESS_PAIRS)
        
        # Address-Teile (Straße mit Hausnummer)
        street = fake.street_name() + " " + fake.building_number()
        
        data.append((first_name, last_name, email, street, plz, ort))
    
    cursor.executemany(sql, data)
    conn.commit()
    print("Kunden-Daten erfolgreich generiert.")
    cursor.close()

# (Die Funktionen generate_produkte, generate_lieferanten, generate_produkt_lieferant_links 
# und generate_bestellungen bleiben wie zuvor, da sie funktional korrekt sind.)

def generate_produkte(conn, count):
    """Generiert Produktdaten."""
    cursor = conn.cursor()
    sql = "INSERT INTO produkte (produkt_name, beschreibung, ek_preis, vk_preis, lagerbestand) VALUES (%s, %s, %s, %s, %s)"
    data = []
    print(f"Generiere {count} Produkte...")
    for i in range(count):
        product_names = [fake.catch_phrase(), fake.bs(), f"Produkt {i+1} - {fake.word()}"]
        name = random.choice(product_names)
        ek_preis = round(random.uniform(1.5, 500.0), 2)
        vk_preis = round(ek_preis * random.uniform(1.2, 2.5), 2) 
        stock = random.randint(0, 500)
        
        data.append((name, fake.paragraph(nb_sentences=2), ek_preis, vk_preis, stock))
    
    cursor.executemany(sql, data)
    conn.commit()
    print("Produkt-Daten erfolgreich generiert.")
    cursor.close()

def generate_lieferanten(conn, count):
    """Generiert Lieferantendaten."""
    cursor = conn.cursor()
    sql = "INSERT INTO lieferanten (firmenname, kontaktperson, telefon, email) VALUES (%s, %s, %s, %s)"
    data = []
    print(f"Generiere {count} Lieferanten...")
    for _ in range(count):
        firm = fake.company()
        contact = fake.name()
        phone = fake.phone_number()
        email = fake.email()
        data.append((firm, contact, phone, email))
    
    cursor.executemany(sql, data)
    conn.commit()
    print("Lieferanten-Daten erfolgreich generiert.")
    cursor.close()

def generate_produkt_lieferant_links(conn, num_products, num_suppliers):
    """Verknüpft Produkte mit Lieferanten."""
    cursor = conn.cursor()
    sql = "INSERT IGNORE INTO produkt_lieferant (produkt_id, lieferant_id) VALUES (%s, %s)"
    links = set()
    print("Verknüpfe Produkte mit Lieferanten...")

    for prod_id in range(1, num_products + 1):
        num_links = random.randint(1, 3)
        for _ in range(num_links):
            supplier_id = random.randint(1, num_suppliers)
            links.add((prod_id, supplier_id))

    cursor.executemany(sql, list(links))
    conn.commit()
    print(f"{len(links)} Produkt-Lieferant-Verknüpfungen erfolgreich erstellt.")
    cursor.close()
    
def generate_bestellungen(conn, num_kunden, num_produkte, count):
    """Generiert Bestellungen und deren Positionen."""
    cursor = conn.cursor()
    
    product_prices = {}
    cursor.execute("SELECT produkt_id, vk_preis FROM produkte")
    for (prod_id, vk_preis) in cursor.fetchall():
        product_prices[prod_id] = float(vk_preis) 
        
    product_ids = list(product_prices.keys())
    
    if not product_ids:
        print("Keine Produkte gefunden. Breche Bestellungserstellung ab.")
        return
        
    sql_bestellung = "INSERT INTO bestellungen (kunde_id, bestelldatum, gesamtbetrag, lieferstatus) VALUES (%s, %s, %s, %s)"
    sql_position = "INSERT INTO bestellpositionen (bestellung_id, produkt_id, menge, einzelpreis) VALUES (%s, %s, %s, %s)"
    
    status_options = ['Geliefert'] * 80 + ['Versandt'] * 10 + ['Bearbeitung'] * 5 + ['Storniert'] * 5
    start_date = datetime.now() - timedelta(days=365)
    
    all_pos_data = [] 
    
    print(f"Generiere {count} Bestellungen...")
    for _ in range(count):
        kunde_id = random.randint(1, num_kunden)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        lieferstatus = random.choice(status_options)
        
        num_items = random.randint(1, 5)
        current_order_items = []
        total_amount = 0.0
        
        selected_product_ids = random.sample(product_ids, k=min(num_items, len(product_ids)))
        
        for prod_id in selected_product_ids:
            menge = random.randint(1, 10)
            einzelpreis = product_prices[prod_id]
            current_order_items.append((prod_id, menge, einzelpreis))
            total_amount += menge * einzelpreis
            
        cursor.execute(sql_bestellung, (kunde_id, order_date, round(total_amount, 2), lieferstatus))
        bestellung_id = cursor.lastrowid
        
        pos_data_for_order = [(bestellung_id, item[0], item[1], item[2]) for item in current_order_items]
        all_pos_data.extend(pos_data_for_order)

    if all_pos_data:
        cursor.executemany(sql_position, all_pos_data)

    conn.commit()
    print("Bestell- und Positionen-Daten erfolgreich generiert.")
    cursor.close()

def main():
    conn = connect_db()
    if conn is None:
        return

    try:
        # NEU: Alten Datenbestand bereinigen, um ID-Konflikte zu vermeiden
        truncate_and_reset(conn)
        
        # Generiere Basisdaten
        generate_kunden(conn, NUM_KUNDEN)
        generate_produkte(conn, NUM_PRODUKTE)
        generate_lieferanten(conn, NUM_LIEFERANTEN)
        
        # Erstelle Verknüpfungen
        generate_produkt_lieferant_links(conn, NUM_PRODUKTE, NUM_LIEFERANTEN)
        
        # Generiere abhängige Daten (Bestellungen)
        generate_bestellungen(conn, NUM_KUNDEN, NUM_PRODUKTE, NUM_BESTELLUNGEN)
        
        print("\n✅ Alle WWS-Testdaten erfolgreich generiert!")
        print(f"Datenbank: {DATABASE}")
        print(f"Kunden: {NUM_KUNDEN}, Produkte: {NUM_PRODUKTE}, Bestellungen: {NUM_BESTELLUNGEN}")

    except Exception as e:
        print(f"\nEin Fehler ist aufgetreten: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()