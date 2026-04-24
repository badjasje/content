# SEO Cockpit als leidende laag – slim integratieplan

## Doel
De SEO Cockpit moet **niet** een extra dashboard zijn, maar de plek waar je dagelijks prioriteiten kiest:
1. Waar zit direct verkeer- en omzetwinst?
2. Welke taken hebben de hoogste impact per uur werk?
3. Welke risico’s (traffic drop, indexatie, cannibalisatie) moeten vandaag opgelost?

Daarom moet data uit losse tabs (intelligence + performance) niet 1-op-1 gekopieerd worden, maar vertaald naar:
- één gedeelde URL- en keyword-waarheid,
- één prioriteitsscore,
- één duidelijke actielijst.

## Wat ECHT handig is in de cockpit

## 1) Eén “SEO Priority Queue” i.p.v. losse tabellen
Toon per URL/cluster exact 1 regel met:
- **Impact score (0–100)**
- **Opportunity type** (Quick Win, Groei, Defensief, Technisch)
- **Confidence** (hoe zeker is de aanbeveling op basis van datapunt-volledigheid)
- **Next best action** (1 actieknop)

Dit voorkomt analyse-paralyse: teams hoeven niet meer tussen tabs te schakelen.

## 2) URL-cluster als kernentiteit
Gebruik clusterlogica als leidraad:
- **Page-level**: individuele URL metrics en signalen
- **Cluster-level**: thema/keywordgroep + intentie

Winst: je voorkomt dat je 20 kleine fixes doet op pagina’s die hetzelfde probleem delen.

## 3) “Delta-first” visualisatie
Niet alleen absolute metrics tonen, maar vooral:
- 7d/28d delta op clicks, impressions, CTR, avg position
- trend-breaks (plotselinge afwijking)
- indexatie-/techniek-events naast performance

SEO teams winnen vooral op tempo van signalering, niet op extra dashboards.

## 4) Actiegericht detailpaneel
Bij klikken op een rij:
- **Waarom nu** (welke data triggert de prioriteit)
- **Bewijs** (GSC/GA/SERP/signalen)
- **Kansscenario** (conservatief / expected / agressief)
- **Acties** (titel/meta, content expansion, interne links, cannibalisatie merge, techniek)
- **Owner + due date + status**

## Data-mapping: van losse tabs naar cockpit

| Bron | Wat je overneemt | Transformatie voor cockpit |
|---|---|---|
| GSC page/query metrics | clicks, impressions, CTR, positie, query mix | bereken momentum, opportunity gap, cannibalisatie-signaal |
| GA page metrics | sessions, engaged sessions, conversions | business-weight per URL (traffic zonder conversie lager prioriteren) |
| SERP signals | feature shifts, concurrentiebeweging | urgency-modifier op opportunity score |
| Feedback/intelligence signals | inhoudelijke en technische signalen | normaliseren naar vaste issue-taxonomie |
| Orchestrator opportunities/tasks | bestaande aanbevelingen en uitvoering | dedupliceren naar 1 “next best action” per URL/cluster |

## Scoremodel (praktisch en uitlegbaar)

## Prioriteit = Impact × Kans × Vertrouwen × Snelheid

Voorstel gewichten:
- **Impact (40%)**: potentieel extra clicks/conversies
- **Kans (30%)**: waarschijnlijkheid dat interventie ranking/CTR verbetert
- **Vertrouwen (20%)**: datakwaliteit + aantal bevestigende bronnen
- **Snelheid (10%)**: time-to-value (quick fix vs zware rebuild)

### Voorbeeldsubscores
- Impact: `impression_gap × verwachte_CTR_uplift × conversieratio`
- Kans: historische uplift van vergelijkbare taken + SERP competitie
- Vertrouwen: sample size, stabiliteit, bronconsensus
- Snelheid: inspanningsinschatting (S/M/L) naar score

Belangrijk: score moet altijd uitlegbaar zijn in de UI ("waarom 82/100?").

## Waar gaan we directe winst pakken (top 6)

1. **High impressions + lage CTR + positie 3–10**
   - Actie: title/meta test + snippet alignment met intentie
   - Waarom winst: snel effect zonder grote contentrebuild

2. **Positie 8–20 met hoge business-intentie queries**
   - Actie: content gap + interne link boosts + FAQ/entiteit verrijking
   - Waarom winst: relatief kleine lift kan veel extra clicks geven

3. **Cannibalisatieclusters**
   - Actie: consolideren/redirect/canonical/intentie-herpositionering
   - Waarom winst: ranking frictie wegnemen levert vaak directe stijging

4. **Historisch sterk, recent dalend (28d vs vorige 28d)**
   - Actie: defensieve refresh + SERP-delta analyse
   - Waarom winst: verloren verkeer terugpakken is vaak goedkoper dan nieuw verkeer creëren

5. **Pagina’s met verkeer maar lage engagement/conversie**
   - Actie: intentie mismatch fix + CTA/UX contentblokken
   - Waarom winst: SEO-output koppelen aan businessresultaat

6. **Technische blokkades op pagina’s met hoge SEO-waarde**
   - Actie: indexeerbaarheid/canonicals/schema/CWV prioriteren op waarde-URL’s
   - Waarom winst: technische fixes pas eerst op pagina’s met hoogste upside

## Cockpit-views die je dagelijks gebruikt

1. **Today Board**
   - top 15 acties met hoogste score en laagste effort
2. **Winners & Losers (7/28 dagen)**
   - automatische detectie van momentumwissels
3. **Risks Monitor**
   - indexatie, cannibalisatie, plotse CTR-drop, SERP feature loss
4. **Execution Funnel**
   - suggested → approved → in progress → done → measured uplift
5. **Business Lens**
   - SEO impact per cluster gekoppeld aan conversie/waarde

## Governance: wanneer is cockpit echt leidend?

## Werkafspraak
- Alle nieuwe SEO-taken starten vanuit de cockpit-prioriteiten.
- Losse tabs blijven “bewijslagen”, niet primaire sturing.
- Elke taak moet een verwachte uplift en meetmoment hebben.

## KPI’s op cockpitniveau
- % taken gestart vanuit cockpit (target >90%)
- Gemiddelde time-to-action (detectie → uitvoering)
- Uplift-hitrate (% taken met positieve 28d uplift)
- Net organische groei per cluster (niet alleen per URL)

## Implementatievolgorde (laag risico, snelle waarde)

### Fase A (2 weken): Aggregatie + score zichtbaar
- unified URL/cluster view
- prioriteitsscore + “waarom”-uitleg
- top opportunities board

### Fase B (2–4 weken): Actieflow en meting
- 1-click taakcreatie met owner/due date
- execution funnel + upliftmeting
- feedback loop op scoremodel

### Fase C (doorlopend): Model slimmer maken
- uplift per taaktype leren
- site-specifieke gewichten
- automatische anomaly detectie

## Niet doen (veel teams verliezen hier tijd)
- Te veel metrics tegelijk tonen zonder actiehiërarchie
- Taken prioriteren op “volume” i.p.v. business impact
- Technische issues gelijk behandelen zonder waarde-segmentatie
- Geen closed-loop meting (dan leer je niet welke acties werken)

## Conclusie
De winst zit niet in méér data, maar in:
- betere prioritering,
- snellere executie,
- meetbare uplift per actie.

Als de cockpit de **enige prioriteitslaag** wordt en alle tabs daar data aan leveren als bewijs, krijg je focus, snelheid en structurele SEO-groei.
