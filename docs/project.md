# Projekt: Boligmarkedet

## Formål
Formålet med dette projekt er at skabe en omfattende database med data over det danske boligmarked. Databasen indeholder data om ejendomme som er til salg samt historisk data over ejendomme som er blevet solgt. Alle data hentes via scraping fra Boligas API.

## Teknisk arkitektur

### Database
- **Teknologi:** DuckDB
- **Struktur:** Separate tabeller for aktive salg og solgte ejendomme
- **Lokation:** Til start lokalt, senere eventuelt både lokalt og på server
- **Data retention:** Permanent opbevaring af alle historiske data
- **Versioning:** Ændringer i ejendommenes status/pris gemmes som separate versioner (begge versioner bevares)

### Scraping system
- **Kilde:** Boligas API
- **Sprog:** Python
- **Frekvens:** Hver time, muligvis hvert kvarter
- **Rate limiting:** Implementeres hvis nødvendigt for at undgå problemer

### Deployment
- **Analyse:** Kører lokalt for ad-hoc dataanalyse
- **Scraping updates:** Kører på server med Coolify for kontinuerlig dataopdatering
- **Sync-strategi:** TBD (hvis database skal være både lokalt og på server)

## Data struktur
- **Specifikke felter:** TBD (bliver bestemt når API-schema er undersøgt)
- **Primær nøgle:** TBD (skal undersøges om Boligas API har unikke IDs)

## Opdateringsrutine

### To-fase startup strategi
**Fase 1: Initial bulk-load (første gangs setup)**
- Scrape alle ~2+ millioner solgte boliger (kan tage timer/dage)
- Scrape alle ~50-60 tusinde aktive boliger
- Kører kun ved første initialisering af systemet

**Fase 2: Løbende opdatering (hver time)**
- **Aktive boliger:** Komplet refresh af alle 50-60k boliger hver time
- **Solgte boliger:** Incremental update - kun nye siden sidste kørsel

### Edge cases og fejlhåndtering
**API-relaterede:**
- Rate limiting med exponential backoff
  
**Data-konsistens:**
- Duplikerede annoncer med forskellige IDs
- Midlertidige fjernelser fra API
- Ændringer i værdier, som fx pris

**Performance optimering:**
- Checkpoint/resume for lange bulk-loads

## Funktionalitet

### Nuværende scope
- Backend-only system (ingen web interface til start)
- Kun adgang for projektejer
- Kontinuerlig dataopdatering via scraping

### Fremtidigt scope
- Muligt dashboard eller web interface

## Opdateringer
Dette dokument opdateres løbende efterhånden som projektet udvikles og nye beslutninger træffes.