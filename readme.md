## Hvordan kjøre tester
For å kjøre testene må du først være i rotmappen av prosjektet og kjøre kommandoen:

```
nosetests -s test
```

Dersom du ikke ønsker å kjøre alle testene kan du spesifisere en test-folder du vil bruke som dette:

```
nosetests -s ./test/xgeo_tests/
```

Obs: for å kunne gjøre dette må du først innstalere nosetests med `pip3 install nose`.
