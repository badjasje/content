# Voorstellen: betere layout-logica voor keywords per klant

## Context (huidige situatie)
- In de klassieke admin-pagina staan **formulier en grote tabel op één scherm**, met veel velden en kolommen tegelijk.
- In de moderne app-shell is de keyword-tabel nog vrij minimalistisch (zonder duidelijke groepering per klant, segmentatie en bulkbewerkingen).

## Verbeteringsvoorstellen

### 1) Klant-eerst structuur (master-detail)
- Maak links een **klantenlijst** (zoekbaar + aantallen keywords).
- Toon rechts alleen de keywords van de geselecteerde klant.
- Voeg bovenin per klant quick-stats toe: Active, Trashed, Queued, Avg priority.

**Waarom:** vermindert visuele overload en maakt eigenaarschap per klant direct duidelijk.

### 2) Segmenteren op intentie en lifecycle
- Introduceer tabs of swimlanes: `Active`, `Queue`, `In productie`, `Done`, `Trash`.
- Laat daarnaast een tweede filterlaag zien op `content_type` (pillar/supporting).

**Waarom:** gebruikers denken in workflowstappen, niet in één lange lijst.

### 3) Groeperen van primary + secondary keywords
- Toon primary keyword als “parent row”.
- Secondary keywords als inklapbare child-lijst.
- Geef visuele indicatoren: overlap, cannibalisatierisico, ontbrekende varianten.

**Waarom:** semantische clusters worden sneller te beoordelen en op te schonen.

### 4) Tabelkolommen op rol en prioriteit
- Maak kolommen configureerbaar per rol:
  - Strategisch: client, keyword, intent, cluster, priority.
  - Operationeel: status, queue-state, updated_at, acties.
  - SEO-analist: impressions/clicks/ctr/position + trend.
- Zet minder belangrijke kolommen achter een “meer” paneel.

**Waarom:** minder ruis, meer beslissingsrelevante informatie per persona.

### 5) Betere prioriteitslogica zichtbaar maken
- Vervang platte numerieke priority door een **scorekaart**:
  - Business value
  - Difficulty
  - Existing performance (GSC)
  - Seasonal relevance
- Toon totale score + reden (“waarom staat dit bovenaan?”).

**Waarom:** prioritering wordt uitlegbaar en consistenter tussen teamleden.

### 6) Bulkacties en snelle toetsen
- Bulkselectie met acties: move to trash, queue, status update, priority bump.
- Sneltoetsen voor veelgebruikte acties.

**Waarom:** grote keywordsets worden veel sneller beheerd.

### 7) Inline validatie en duplicaatpreventie
- Tijdens invoer direct signaleren:
  - bestaand keyword binnen dezelfde klant;
  - sterk lijkende variant;
  - conflict met bestaande cluster.
- Geef éénklik-opties: merge, link als secondary, of toch apart bewaren.

**Waarom:** voorkomt vervuiling van de dataset aan de voorkant.

### 8) Contextpanelen i.p.v. extra navigatie
- Rechter zijpaneel bij selectie van keyword met:
  - linked sites/categorieën;
  - laatste prestaties;
  - lifecycle note/history;
  - aanbevolen volgende actie.

**Waarom:** minder paginawissels, sneller beslissen.

### 9) Visual cues voor kwaliteit
- Gebruik badges/chips met duidelijke kleurcodering:
  - status,
  - lifecycle,
  - bron (manual / GSC),
  - confidence.
- Voeg tooltips toe met korte uitleg.

**Waarom:** snelle scanbaarheid voor teams met veel records.

### 10) Een concrete UX-flow om mee te starten (MVP)
1. Selecteer klant.
2. Zie direct keyword-clusters + statuskanban.
3. Filter op intentie/content type.
4. Bulk queue/trash.
5. Open detailpaneel voor fine-tuning.

**Waarom:** combineert overzicht + snelheid zonder meteen een zware redesign.

## Technische vertaling (klein, middel, groot)
- **Klein (1–2 iteraties):** filters, kolomselectie, bulkacties, badges.
- **Middel (2–4 iteraties):** master-detail per klant + detailpaneel.
- **Groot (4+ iteraties):** clusterweergave (parent/child), scoringmodel en explainability.

## Aanbevolen volgorde
1. Bulkacties + filters + badges (snelste impact).
2. Klant-eerst master-detail.
3. Clustering + scorekaart.
