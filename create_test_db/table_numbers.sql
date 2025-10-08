SELECT 'kunden' AS Tabelle, COUNT(*) AS Anzahl FROM kunden
UNION ALL
SELECT 'bestellungen', COUNT(*) FROM bestellungen
UNION ALL
SELECT 'bestellpositionen', COUNT(*) FROM bestellpositionen
UNION ALL
SELECT 'produkte', COUNT(*) FROM produkte
UNION ALL
SELECT 'lieferanten', COUNT(*) FROM lieferanten
UNION ALL
SELECT 'produkt_lieferant', COUNT(*) FROM produkt_lieferant
ORDER BY Anzahl DESC;