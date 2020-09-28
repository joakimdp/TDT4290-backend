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


## Hvordan kjøre tester
For å kjøre testene må du først være i rotmappen av prosjektet og kjøre kommandoen:

```
nosetests -s test
```

Dersom du ikke ønsker å kjøre alle testene kan du spesifisere en test-folder du vil bruke som dette:

```
nosetests -s ./test/xgeo_tests/
```

Obs: Hvis man ikke bruker det virtuelle miljøet må man først innstalere nosetests med `pip install nose` (ev. `pip3`) for å kunne bruke kjøre
kommandoen.
