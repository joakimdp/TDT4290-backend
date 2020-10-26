# [prosjektnavn]

## Virtuelt miljø
Prosjektet er lagt opp til at man skal kunne bruke et virtuelt miljø via
Pythons `venv`-modul.

### Oppsett
Første gang man skal bruke dette må man sette det
opp ved å kjøre denne kommandoen i prosjektets rotmappe (hvis man av en
eller annen grunn skulle ha den døde Python 2 som sin `python` må man
erstatte `python` med `python3` i kommandoen):
```
python -m venv .venv
```
Dette lager mappen `.venv` i prosjektroten, som er der det virtuelle miljøet
lever.

### Aktivering
For å aktivere miljøet må man kjøre en kommando som avhenger av
operativsystem:

#### POSIX (Linux, macOS, \*BSD osv)
```
source .venv/bin/activate
```

#### Windows
```
.venv\bin\activate.bat
```

### Deaktivering
For å deaktivere kjører man bare kommandoen (uavhengig av OS)
```
deactivate
```

### Bruk
Når miljøet er aktivert installerer man pakkene som prosjektet bruker ved
å kjøre kommandoen
```
pip install -r requirements-top-level.txt
```
Filen `requirements-top-level.txt` inneholder en liste over alle biblioteker
prosjektet bruker direkte. Husk å oppdatere denne filen hvis du legger til en
dependency! Når programmet har blitt kjørt og verifisert at virker som det
skal (passert tester osv.) kjører man kommandoen
```
pip freeze > requirements.txt
```
Dette oppdaterer filen `requirements.txt` til å inneholde alle
biblioteker/pakker som er installert i miljøet for en tilstand som er
bekreftet å virke, slik at man alltid har en baseline å sammenlikne med og å
sette ut i produksjon.

For å oppdatere pakkene til nyeste versjon kjører man
```
pip install -Ur requirements-top-level.txt
```
Og for å installere pakkene som sist ble bekreftet å virke kjører man
```
pip install -r requirements.txt
```


## Operasjon

### Oppsett
For at programmet skal utføre jobben sin trenger man en fil med miljøvariabler,
`.env`, for autentisering til database og Frost. Variablene er
 - `DATABASE_SERVER`: URI-en til databasen
 - `DATABASE_NAME`: Navnet til databasen på serveren
 - `DATABASE_USERNAME`: Brukernavnet for kontoen programmet bruker til
   å sette inn data
 - `DATABASE_PASSWORD`: Passordet for overnevnte konto
 - `FROST_CID`: Klient-ID for å hente data fra Frost

Videre er det nødvendig at databasen er satt opp slik det er spesifisert i
miljøvariablene. I tillegg må avhengighetene i `requirements-top-level.txt`
være installert. Blant disse er en backend-driver til SQLAlchemy for
databasesystemet som brukes. Koden er lagt opp for Microsoft SQL Server.
Backend-driveren trenger eventuelt en egen backend-driver igjen. For SQL
Server med pyodbc trenger man en ODBC-driver. Her er det anbefalt å bruke
[Micorsofts offisielle ODBC-driver](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server).

### Første kjøring/bygge opp fra grunnen
På dette er programmet klart for å kjøres. Dette gjøres ved å kjøre filen
`src/main.py` med kommandolinjeflagget `-f/--force-update`, altså blir
kommandoen
```
python src/main.py -f
```

Kjørt på en tom database vil dette lage alle nødvendige tabeller. På en
database som allerede har data i seg vil dette tømme alle tabellene
og fylle dem opp med all data fra grunnen av.

### Inkrementell oppdatering
Uten parametre vil programmet forsøke å kun legge inn data som er endret fra
forrige kjøring. Så lenge programmet kjøres jevnlig vil dette drastisk kutte
ned kjøretiden (fra ~4 timer til ~4 minutter), slik at programmet kan kjøres
hyppig for å oppdatere databasen.


## Hvordan kjøre tester
For å kjøre testene må du først være i rotmappen av prosjektet og kjøre kommandoen:

```
nosetests -s test
```

Dersom du ikke ønsker å kjøre alle testene kan du spesifisere en test-folder du vil bruke som dette:

```
nosetests -s ./test/xgeo_tests/
```

Obs: Hvis man ikke bruker det virtuelle miljøet må man først innstallere nosetests med `pip install nose` (ev. `pip3`) for å kunne kjøre
kommandoen.
