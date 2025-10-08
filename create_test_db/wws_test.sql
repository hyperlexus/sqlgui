-- 1. Datenbank erstellen
CREATE DATABASE IF NOT EXISTS wws_test;
USE wws_test;

-- 2. Kunden-Tabelle
CREATE TABLE IF NOT EXISTS kunden (
    kunde_id INT AUTO_INCREMENT PRIMARY KEY,
    vorname VARCHAR(50) NOT NULL,
    nachname VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    strasse VARCHAR(100),
    plz VARCHAR(10),
    ort VARCHAR(100),
    erstellt_am TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Produkt-Tabelle
CREATE TABLE IF NOT EXISTS produkte (
    produkt_id INT AUTO_INCREMENT PRIMARY KEY,
    produkt_name VARCHAR(150) NOT NULL,
    beschreibung TEXT,
    ek_preis DECIMAL(10, 2) NOT NULL,
    vk_preis DECIMAL(10, 2) NOT NULL,
    lagerbestand INT DEFAULT 0
);

-- 4. Bestellungen-Tabelle
CREATE TABLE IF NOT EXISTS bestellungen (
    bestellung_id INT AUTO_INCREMENT PRIMARY KEY,
    kunde_id INT NOT NULL,
    bestelldatum DATE NOT NULL,
    gesamtbetrag DECIMAL(10, 2) NOT NULL,
    lieferstatus ENUM('Offen', 'Bearbeitung', 'Versandt', 'Geliefert', 'Storniert') DEFAULT 'Offen',
    FOREIGN KEY (kunde_id) REFERENCES kunden(kunde_id)
);

-- 5. Bestellpositionen-Tabelle (Detailtabelle)
CREATE TABLE IF NOT EXISTS bestellpositionen (
    position_id INT AUTO_INCREMENT PRIMARY KEY,
    bestellung_id INT NOT NULL,
    produkt_id INT NOT NULL,
    menge INT NOT NULL,
    einzelpreis DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (bestellung_id) REFERENCES bestellungen(bestellung_id),
    FOREIGN KEY (produkt_id) REFERENCES produkte(produkt_id)
);

-- 6. Lieferanten-Tabelle
CREATE TABLE IF NOT EXISTS lieferanten (
    lieferant_id INT AUTO_INCREMENT PRIMARY KEY,
    firmenname VARCHAR(100) NOT NULL,
    kontaktperson VARCHAR(100),
    telefon VARCHAR(50),
    email VARCHAR(100)
);

-- 7. Produkt-Lieferanten-Verbindung (Viele-zu-Viele)
CREATE TABLE IF NOT EXISTS produkt_lieferant (
    produkt_id INT NOT NULL,
    lieferant_id INT NOT NULL,
    PRIMARY KEY (produkt_id, lieferant_id),
    FOREIGN KEY (produkt_id) REFERENCES produkte(produkt_id),
    FOREIGN KEY (lieferant_id) REFERENCES lieferanten(lieferant_id)
);