# Sistem za praćenje i kontrolu rezultata glasanja

Distribuirani sistem za praćenje i kontrolu rezultata glasanja sa podrškom za toleranciju na otkaze i oporavak sistema nakon pada servera.

## Funkcionalnosti

- Arhitektura sa liderom i pratiocima (3 replike)
- Automatski izbor lidera
- Preuzimanje uloge lidera nakon otkaza (fail-over)
- Oporavak replika
- Logovanje svih izmena
- Rekonstrukcija stanja nakon restarta
- Snapshot mehanizam za čuvanje stanja sistema
- Sinhronizacija replika i catch-up mehanizam
- Obrada GET zahteva na sinhronizovanim pratiocima
- Podrška za više tipova izbora:
  - parlamentarne
  - predsedničke
  - lokalne
- Validacija unetih rezultata glasanja
- Verifikacija rezultata od strane više kontrolora
- Označavanje biračkih mesta za ponovni unos
- Blokiranje biračkih mesta nakon ponovljenih neuspešnih verifikacija
- Statistika i izveštavanje o rezultatima glasanja
- Simulacija istovremenog rada više kontrolora

## Pokretanje

1. Pokrenuti ZooKeeper.
2. Pokrenuti tri serverske replike.
3. Pokrenuti klijentsku aplikaciju.
