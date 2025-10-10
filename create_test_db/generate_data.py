import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import threading
from queue import Queue
import time
from typing import List, Tuple

# ==================================
# 1. KONFIGURATION
# ==================================
HOST = "localhost"
USER = "root"
PASSWORD = ""
DATABASE = "wws_test"
PORT = 3306

# Mengen
NUM_KUNDEN = 500000 
NUM_PRODUKTE = 50000 
NUM_LIEFERANTEN = 20000
NUM_BESTELLUNGEN = 200000

# Steuerung
BATCH_SIZE = 5000 
NUM_THREADS = 8 

MIN_POSITIONEN_PRO_BESTELLUNG = 1
MAX_POSITIONEN_PRO_BESTELLUNG = 5
MIN_LIEFERANTEN_PRO_PRODUKT = 1
MAX_LIEFERANTEN_PRO_PRODUKT = 3
# ==================================

fake = Faker('de_DE')
DE_ADDRESS_PAIRS = [
    ("10115", "Berlin"), ("80331", "München"), ("20095", "Hamburg"),
    ("50667", "Köln"), ("60306", "Frankfurt"), ("70173", "Stuttgart"),
    ("04103", "Leipzig"), ("40210", "Düsseldorf"), ("44135", "Dortmund"),
    ("30159", "Hannover"), ("90403", "Nürnberg"), ("28195", "Bremen")
]

data_queue = Queue()

# --- DB-Funktionen ---

def connect_db(db_name=DATABASE):
    try:
        return mysql.connector.connect(
            host=HOST, user=USER, password=PASSWORD, database=db_name, port=PORT
        )
    except mysql.connector.Error as err:
        print(f"❌ Fehler bei der Datenbankverbindung: {err}")
        return None

def truncate_and_reset(conn):
    cursor = conn.cursor()
    tables = ["bestellpositionen", "bestellungen", "produkt_lieferant", "produkte", "kunden", "lieferanten"]
    
    print("\n--- Datenbereinigung und Zähler-Reset ---")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in tables:
        try:
            cursor.execute(f"TRUNCATE TABLE `{table}`")
            print(f"🗑️ Tabelle '{table}' geleert.")
        except mysql.connector.Error as err:
            print(f"❌ Fehler beim Leeren von {table}: {err}. Rollback.")
            conn.rollback() 
            
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    cursor.close()

def db_worker(queue: Queue, counter: dict, lock: threading.Lock):
    local_conn = connect_db()
    if not local_conn:
        return
        
    local_cursor = local_conn.cursor()
    
    while True:
        item = queue.get()
        
        if item is None:
            queue.task_done() 
            break

        sql, batch_data, table_name, total_count = item 
        
        try:
            local_cursor.executemany(sql, batch_data)
            local_conn.commit()
            
            with lock:
                counter[table_name] += len(batch_data)
                print(f"\r🚀 {table_name.capitalize():15}: {counter[table_name]:,} / {total_count:,} ({counter[table_name]*100/total_count:.1f}%)", end='', flush=True)

        except mysql.connector.Error as err:
            print(f"\n❌ DB-Fehler bei {table_name}: {err}")
            local_conn.rollback()
        except Exception as e:
            print(f"\n❌ Unbekannter Fehler in Worker bei {table_name}: {e}")
            local_conn.rollback()
            
        finally:
            queue.task_done() 

    local_cursor.close()
    local_conn.close()

def push_to_queue(all_data: List[Tuple], table_name: str, sql_insert: str, total_count: int):
    for i in range(0, len(all_data), BATCH_SIZE):
        batch = all_data[i:i + BATCH_SIZE]
        data_queue.put((sql_insert, batch, table_name, total_count))
    
    print(f"\n📦 Alle {table_name} Batches ({len(all_data) // BATCH_SIZE + 1} Stück) in die Queue gestellt.")

# --- Generierung primärer Daten (parallel) ---

def generate_kunden(count):
    # Korrektur: Nutzt INSERT IGNORE, um Duplicate-Entry-Fehler (1062) zu vermeiden
    sql = "INSERT IGNORE INTO kunden (vorname, nachname, email, strasse, plz, ort) VALUES (%s, %s, %s, %s, %s, %s)"
    data = []
    generated_emails = set()
    print(f"\nGeneriere {count} Kunden-Daten im Speicher...")
    for _ in range(count):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email_base = f"{first_name}.{last_name}@{fake.domain_name()}".lower().replace(' ', '')
        email = email_base
        attempt = 0
        while email in generated_emails:
            attempt += 1
            email = f"{first_name}.{last_name}{random.randint(100, 999)}@{fake.domain_name()}".lower().replace(' ', '')
        generated_emails.add(email)
        plz, ort = random.choice(DE_ADDRESS_PAIRS)
        street = fake.street_name() + " " + fake.building_number()
        data.append((first_name, last_name, email, street, plz, ort))
    
    push_to_queue(data, "kunden", sql, count)
    

def generate_produkte(count):
    sql = "INSERT INTO produkte (produkt_name, beschreibung, ek_preis, vk_preis, lagerbestand) VALUES (%s, %s, %s, %s, %s)"
    data = []
    print(f"\nGeneriere {count} Produkte-Daten im Speicher...")
    for i in range(count):
        name = f"Produkt {i+1}: {fake.catch_phrase()}"
        ek_preis = round(random.uniform(5.0, 1000.0), 2)
        vk_aufschlag = random.uniform(1.10, 3.0) 
        vk_preis = round(ek_preis * vk_aufschlag, 2) 
        stock = random.randint(0, 500)
        data.append((name, fake.paragraph(nb_sentences=2), ek_preis, vk_preis, stock))
    
    push_to_queue(data, "produkte", sql, count)


def generate_lieferanten(count):
    sql = "INSERT INTO lieferanten (firmenname, kontaktperson, telefon, email) VALUES (%s, %s, %s, %s)"
    data = []
    print(f"\nGeneriere {count} Lieferanten-Daten im Speicher...")
    for _ in range(count):
        firm = fake.company()
        contact = fake.name()
        phone = fake.phone_number()
        email = fake.email()
        data.append((firm, contact, phone, email))
    
    push_to_queue(data, "lieferanten", sql, count)

# --- Generierung abhängiger Daten (seriell) ---

def generate_produkt_lieferant_links_sequentially(conn, num_products, num_suppliers):
    """Generiert Produkt-Lieferant-Links seriell im Hauptthread, um Deadlocks zu vermeiden."""
    
    cursor = conn.cursor()
    sql = "INSERT IGNORE INTO produkt_lieferant (produkt_id, lieferant_id) VALUES (%s, %s)"
    links = set()
    
    print("\nGeneriere Produkt-Lieferant-Verknüpfungen (seriell/Batch)...")
    
    for prod_id in range(1, num_products + 1):
        num_links = random.randint(MIN_LIEFERANTEN_PRO_PRODUKT, MAX_LIEFERANTEN_PRO_PRODUKT)
        for _ in range(num_links):
            supplier_id = random.randint(1, num_suppliers)
            links.add((prod_id, supplier_id))
    
    all_links_list = list(links)
    total_count = len(all_links_list)
    
    for batch_index in range(0, total_count, BATCH_SIZE):
        batch = all_links_list[batch_index:batch_index + BATCH_SIZE]
        
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            
            inserted_count = batch_index + len(batch)
            print(f"\r📦 Produkt_lieferant: {inserted_count:,} / {total_count:,} eingefügt. ({inserted_count*100/total_count:.1f}%)", end='', flush=True)
        except mysql.connector.Error as err:
            print(f"\n❌ Fehler beim seriellen Einfügen von produkt_lieferant: {err}")
            conn.rollback()
            
    cursor.close()
    return total_count


def generate_bestellungen_and_positions_sequentially(conn, num_kunden, num_produkte, count):
    cursor = conn.cursor()
    
    # Korrektur: Max ID abfragen, um Foreign Key-Fehler (1452) durch fehlende Kunden zu vermeiden
    cursor.execute("SELECT MAX(kunde_id) FROM kunden")
    max_kunde_id = cursor.fetchone()[0]
    
    if max_kunde_id is None:
        print("❌ Keine Kunden-IDs in der Datenbank gefunden. Breche Bestellungserstellung ab.")
        return 0, 0
    
    product_prices = {}
    cursor.execute("SELECT produkt_id, vk_preis FROM produkte")
    for (prod_id, vk_preis) in cursor.fetchall():
        product_prices[prod_id] = float(vk_preis) 
    
    product_ids = list(product_prices.keys())
    if not product_ids:
        print("❌ Keine Produkte gefunden. Breche Bestellungserstellung ab.")
        return 0, 0
        
    sql_bestellung = "INSERT INTO bestellungen (kunde_id, bestelldatum, gesamtbetrag, lieferstatus) VALUES (%s, %s, %s, %s)"
    sql_position = "INSERT INTO bestellpositionen (bestellung_id, produkt_id, menge, einzelpreis) VALUES (%s, %s, %s, %s)"
    
    bestell_data = []
    pos_data_temp = [] 
    
    status_options = ['Geliefert'] * 70 + ['Versandt'] * 15 + ['Bearbeitung'] * 10 + ['Storniert'] * 5 
    start_date = datetime.now() - timedelta(days=730)
    
    print(f"\nGeneriere {count} Bestellungen und Positionen im Speicher (seriell/FK-abh.)...")
    
    for i in range(1, count + 1):
        kunde_id = random.randint(1, max_kunde_id) # Nutze die max ID
        order_date = fake.date_between(start_date=start_date, end_date='today')
        lieferstatus = random.choice(status_options)
        
        num_items = random.randint(MIN_POSITIONEN_PRO_BESTELLUNG, MAX_POSITIONEN_PRO_BESTELLUNG)
        total_amount = 0.0
        
        selected_product_ids = random.sample(product_ids, k=min(num_items, len(product_ids)))
        
        for prod_id in selected_product_ids:
            menge = random.randint(1, 10)
            einzelpreis = product_prices[prod_id]
            pos_data_temp.append((i, prod_id, menge, einzelpreis)) 
            total_amount += menge * einzelpreis
            
        bestell_data.append((kunde_id, order_date, round(total_amount, 2), lieferstatus))
        
    print(f"\nStarte Batch-Einfügung der {count:,} Bestellungen...")
    
    all_pos_data_final = []
    
    for batch_index in range(0, len(bestell_data), BATCH_SIZE):
        batch = bestell_data[batch_index:batch_index + BATCH_SIZE]
        
        cursor.executemany(sql_bestellung, batch)
        first_id = cursor.lastrowid
        
        if first_id is None: 
            conn.rollback()
            raise Exception("Fehler beim Abrufen der AUTO_INCREMENT ID nach Batch-Insert.")
        
        start_temp_id = batch_index + 1
        end_temp_id = batch_index + len(batch)
        
        pos_batch_data = [
            (first_id + temp_id - start_temp_id, prod_id, menge, einzelpreis)
            for temp_id, prod_id, menge, einzelpreis in pos_data_temp
            if start_temp_id <= temp_id <= end_temp_id
        ]
        all_pos_data_final.extend(pos_batch_data)
        
        conn.commit()
        
        inserted_count = batch_index + len(batch)
        print(f"\r📦 Bestellungen: {inserted_count:,} / {count:,} eingefügt. ({inserted_count*100/count:.1f}%)", end='', flush=True)

    print(f"\nStarte Batch-Einfügung der {len(all_pos_data_final):,} Positionen...")
    
    total_pos_count = len(all_pos_data_final)
    for batch_index in range(0, total_pos_count, BATCH_SIZE):
        batch = all_pos_data_final[batch_index:batch_index + BATCH_SIZE]
        cursor.executemany(sql_position, batch)
        conn.commit()
        inserted_count = batch_index + len(batch)
        print(f"\r📦 Positionen: {inserted_count:,} / {total_pos_count:,} eingefügt. ({inserted_count*100/total_pos_count:.1f}%)", end='', flush=True)

    cursor.close()
    return count, total_pos_count

# --- Hauptausführung ---
def main():
    start_time = time.time()
    conn = connect_db()
    if conn is None:
        return

    try:
        truncate_and_reset(conn)
        
        worker_threads = []
        inserted_counts = {"kunden": 0, "produkte": 0, "lieferanten": 0, "produkt_lieferant": 0, "bestellungen": 0, "bestellpositionen": 0}
        lock = threading.Lock()
        
        print(f"\n--- Starte {NUM_THREADS} Worker-Threads ---")
        for _ in range(NUM_THREADS):
            t = threading.Thread(target=db_worker, args=(data_queue, inserted_counts, lock))
            t.start()
            worker_threads.append(t)
            
        # 3. UNABHÄNGIGE TABELLEN (Parallel)
        generate_kunden(NUM_KUNDEN)
        generate_produkte(NUM_PRODUKTE)
        generate_lieferanten(NUM_LIEFERANTEN)
        
        # 4. Warten auf Abschluss der Parallel-Jobs
        data_queue.join() 
        print("\n\n✅ Primärdaten (Kunden, Produkte, Lieferanten) erfolgreich per Batch eingefügt.")

        # 4b. PRODUKT_LIEFERANT (Seriell, da hohe Deadlock-Gefahr)
        inserted_counts["produkt_lieferant"] = generate_produkt_lieferant_links_sequentially(conn, NUM_PRODUKTE, NUM_LIEFERANTEN)
        
        # 5. Abschluss der Worker-Threads
        for _ in range(NUM_THREADS):
            data_queue.put(None)
        for t in worker_threads:
            t.join() 
        print("✅ Alle Worker-Threads sauber beendet.")

        # 6. BESTELLUNGEN/POSITIONEN (Seriell/Batch, da FK-Abhängigkeit)
        inserted_counts["bestellungen"], inserted_counts["bestellpositionen"] = \
            generate_bestellungen_and_positions_sequentially(conn, NUM_KUNDEN, NUM_PRODUKTE, NUM_BESTELLUNGEN)
        print("\n✅ Bestellungen und Positionen erfolgreich sequenziell/Batch eingefügt.")

        # 7. Zählung
        check_counts(conn)
        
        end_time = time.time()
        print("\n=======================================================")
        print(f"✅ Gesamte Generierung abgeschlossen in: {end_time - start_time:.2f} Sekunden.")
        print(f"Eingefügte Datensätze (gezählt): {inserted_counts}")
        print("=======================================================")

    except Exception as e:
        print(f"\n❌ Ein schwerwiegender Fehler ist aufgetreten: {e}")
        conn.rollback()
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Datenbankverbindung geschlossen.")

def check_counts(conn):
    cursor = conn.cursor(dictionary=True)
    print("\n--- Überprüfung der generierten Datenmengen ---")
    
    expected_primary = {
        "kunden": NUM_KUNDEN,
        "produkte": NUM_PRODUKTE,
        "lieferanten": NUM_LIEFERANTEN,
        "bestellungen": NUM_BESTELLUNGEN
    }
    
    for table, expected in expected_primary.items():
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
            actual_count = cursor.fetchone()['count']
            print(f"✅ {table.capitalize():15}: {actual_count:,} (Ziel: {expected:,})")
        except mysql.connector.Error as err:
            print(f"❌ Fehler beim Zählen von {table}: {err}")

    expected_secondary = {
        "produkt_lieferant": NUM_PRODUKTE * MIN_LIEFERANTEN_PRO_PRODUKT,
        "bestellpositionen": NUM_BESTELLUNGEN * MIN_POSITIONEN_PRO_BESTELLUNG
    }
    
    for table, expected_min in expected_secondary.items():
          try:
            cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
            actual_count = cursor.fetchone()['count']
            print(f"✅ {table.capitalize():15}: {actual_count:,} (Min. Ziel: {expected_min:,})")
          except mysql.connector.Error as err:
            print(f"❌ Fehler beim Zählen von {table}: {err}")
    
    cursor.close()

if __name__ == "__main__":
    main()