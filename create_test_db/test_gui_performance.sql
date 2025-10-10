SELECT 
    k.kunde_id, 
    k.vorname,
    k.nachname,
    YEAR(b.bestelldatum) AS Jahr,
    MONTH(b.bestelldatum) AS Monat,
    COUNT(b.bestellung_id) AS Anzahl_Bestellungen,
    SUM(b.gesamtbetrag) AS Gesamtumsatz_Monat
FROM 
    kunden k
JOIN 
    bestellungen b ON k.kunde_id = b.kunde_id
WHERE
    b.bestelldatum >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
    AND b.lieferstatus = 'Geliefert' 
GROUP BY 
    k.kunde_id, 
    k.vorname,
    k.nachname,
    Jahr,
    Monat
ORDER BY 
    Gesamtumsatz_Monat DESC,
    Jahr DESC,
    Monat DESC
