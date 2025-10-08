import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# ----------------------------------
# 1. DATENBANK-KONFIGURATION ANPASSEN
# ----------------------------------
HOST = "localhost"
USER = "root"  # Ihr MySQL-Benutzername
PASSWORD = ""  # Ihr MySQL-Passwort
DATABASE = "wws_test"
PORT = 3308  # Standard ist 3306, anpassen falls nötig
NUM_KUNDEN = 5000
NUM_PRODUKTE = 500
NUM_LIEFERANTEN = 50
NUM_BESTELLUNGEN = 5000
# ----------------------------------

fake = Faker('de_DE') # Realistischere deutsche Daten

def connect_db():
    """Stellt die Verbindung zur MySQL-Datenbank her."""
    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            port=PORT
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Fehler bei der Datenbankverbindung: {err}")
        return None

def generate_kunden(conn, count):
    """Generiert Kundendaten."""
    cursor = conn.cursor()
    sql = "INSERT INTO kunden (vorname, nachname, email, strasse, plz, ort) VALUES (%s, %s, %s, %s, %s, %s)"
    data = []
    print(f"Generiere {count} Kunden...")
    for _ in range(count):
        first_name = fake.first_name()
        last_name = fake.last_name()
        # Stellen Sie sicher, dass die E-Mail einzigartig ist, auch wenn Faker das meistens tut
        email = f"{first_name}.{last_name}.{random.randint(1, 999)}@{fake.domain_name()}".lower().replace(' ', '')
        
        # Address-Teile
        address_parts = fake.address().split('\n')
        street = address_parts[0] if len(address_parts) > 0 else "Musterstr. 1"
        city_zip = address_parts[1].split(' ') if len(address_parts) > 1 else ["12345", "Musterstadt"]
        
        plz = city_zip[0] if len(city_zip) > 0 else "12345"
        ort = ' '.join(city_zip[1:]) if len(city_zip) > 1 else "Musterstadt"
        
        data.append((first_name, last_name, email, street, plz, ort))
    
    cursor.executemany(sql, data)
    conn.commit()
    print("Kunden-Daten erfolgreich generiert.")
    cursor.close()

def generate_produkte(conn, count):
    """Generiert Produktdaten."""
    cursor = conn.cursor()
    sql = "INSERT INTO produkte (produkt_name, beschreibung, ek_preis, vk_preis, lagerbestand) VALUES (%s, %s, %s, %s, %s)"
    data = []
    print(f"Generiere {count} Produkte...")
    for i in range(count):
        # Realistische Produktnamen
        product_names = [fake.catch_phrase(), fake.bs(), f"Produkt {i+1} - {fake.word()}"]
        name = random.choice(product_names)
        
        # Preise
        ek_preis = round(random.uniform(1.5, 500.0), 2)
        vk_preis = round(ek_preis * random.uniform(1.2, 2.5), 2) # Verkaufspreis > Einkaufspreis
        
        # Lagerbestand
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

    # Jedes Produkt muss mindestens einen Lieferanten haben
    for prod_id in range(1, num_products + 1):
        num_links = random.randint(1, 3) # 1 bis 3 Lieferanten pro Produkt
        for _ in range(num_links):
            supplier_id = random.randint(1, num_suppliers)
            links.add((prod_id, supplier_id))

    cursor.executemany(sql, list(links))
    conn.commit()
    print(f"{len(links)} Produkt-Lieferant-Verknüpfungen erfolgreich erstellt.")
    cursor.close()

def generate_bestellungen(conn, num_kunden, num_produkte, count):
    """Generiert Bestellungen und deren Positionen."""
    cursor = conn.cursor(prepared=True)
    
    # 1. Alle Produkt-VK-Preise abrufen (für korrekte Berechnung)
    product_prices = {}
    cursor.execute("SELECT produkt_id, vk_preis FROM produkte")
    for (prod_id, vk_preis) in cursor.fetchall():
        product_prices[prod_id] = vk_preis
    product_ids = list(product_prices.keys())
    
    if not product_ids:
        print("Keine Produkte gefunden. Breche Bestellungserstellung ab.")
        return
        
    sql_bestellung = "INSERT INTO bestellungen (kunde_id, bestelldatum, gesamtbetrag, lieferstatus) VALUES (%s, %s, %s, %s)"
    sql_position = "INSERT INTO bestellpositionen (bestellung_id, produkt_id, menge, einzelpreis) VALUES (%s, %s, %s, %s)"
    
    status_options = ['Geliefert'] * 80 + ['Versandt'] * 10 + ['Bearbeitung'] * 5 + ['Storniert'] * 5 # Realistische Verteilung
    start_date = datetime.now() - timedelta(days=365) # Bestellungen der letzten 12 Monate
    
    print(f"Generiere {count} Bestellungen...")
    for _ in range(count):
        # Bestelldaten
        kunde_id = random.randint(1, num_kunden)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        lieferstatus = random.choice(status_options)
        
        # Positionen für diese Bestellung erstellen
        num_items = random.randint(1, 5) # 1 bis 5 verschiedene Produkte pro Bestellung
        order_items = []
        total_amount = 0.0
        
        # Produkte für diese Bestellung auswählen
        selected_product_ids = random.sample(product_ids, k=min(num_items, len(product_ids)))
        
        for prod_id in selected_product_ids:
            menge = random.randint(1, 10)
            einzelpreis = product_prices[prod_id]
            
            # Position hinzufügen
            order_items.append((prod_id, menge, einzelpreis))
            total_amount += menge * einzelpreis
            
        # 1. Bestellung einfügen
        cursor.execute(sql_bestellung, (kunde_id, order_date, round(total_amount, 2), lieferstatus))
        bestellung_id = cursor.lastrowid
        
        # 2. Bestellpositionen einfügen
        pos_data = [(bestellung_id, item[0], item[1], item[2]) for item in order_items]
        cursor.executemany(sql_position, pos_data)
        
    conn.commit()
    print("Bestell- und Positionen-Daten erfolgreich generiert.")
    cursor.close()

def main():
    conn = connect_db()
    if conn is None:
        return

    try:
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