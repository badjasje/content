# Executive samenvatting

De SEO Cockpit wordt in 90 dagen de **enige prioriteitslaag** voor SEO-uitvoering: niet langer losse intelligence/performance-tabbladen, maar één dagelijkse stuurlaag op **impact, snelheid en meetbare uplift**. In v1 kiezen we bewust voor een pragmatische scope: één unified URL/cluster model, één prioriteitsscore, één end-to-end execution funnel en zes bewezen winst-playbooks. We optimaliseren voor **time-to-value binnen 2 weken** met een read-mostly cockpit en semi-automatische acties, vóórdat we geavanceerde ML of volledige automatisering toevoegen.

Kernkeuzes:
- **Single source of prioritization**: alle nieuwe SEO-taken starten vanuit de Cockpit.
- **URL + cluster dualiteit**: score en acties op URL-niveau, roll-up en governance op cluster-niveau.
- **Score = Impact × Kans × Vertrouwen × Snelheid** met uitlegbare componenten en anti-gaming-regels.
- **Closed loop**: elke taak krijgt expected uplift, meetvenster (7/28 dagen), owner en status in funnel.
- **V1 focus**: snelle winst op CTR, rank-lift, defensieve recuperatie en high-value technische fixes.

Wat we in v1 expliciet **niet** doen:
- Geen volledig geautomatiseerde publicatie/SEO wijzigingen zonder menselijke approval.
- Geen generatieve contentproductie in de cockpitflow.
- Geen real-time (sub-hour) scoring; dag-niveau refresh is voldoende.
- Geen volledige attributiemodellen (MMM/MTA); we gebruiken pragmatische business proxies.

---

# 90-dagen roadmap (sprint voor sprint)

> Ritme: 6 sprints van 2 weken + 1 week hardening/enablement. Fases: A (fundament), B (uitvoering), C (optimalisatie).

## Sprint 1 (week 1-2) — Fase A: Unified fundament + Today Board MVP

**Doel**
- Eén betrouwbare URL/cluster dataset + eerste prioriteitslijst live (read-only) voor dagelijkse triage.

**Scope**
- Canonical URL normalisatie + cluster-key afleiding.
- Inname van GSC/GA/SERP/signals/tasks naar één cockpit view.
- Today Board MVP met top 15 opportunities en basisfilters.
- Prioriteitsscore v1 (eerste versie) incl. explainability-regel.

**User stories**
- Als SEO lead wil ik elke ochtend topkansen zien, zodat ik binnen 15 minuten kan prioriteren.
- Als analist wil ik per regel zien waarom de score hoog is, zodat ik sneller kan valideren.
- Als content owner wil ik alleen kansen voor mijn cluster/site kunnen filteren.

**Acceptatiecriteria**
- 95% van actieve SEO-URL’s heeft canonical_url_id en cluster_id.
- Today Board toont maximaal 15 regels met score + next best action.
- Elke regel bevat minimaal 3 explainability bullets (data-driven).
- Refresh draait dagelijks automatisch vóór 08:00 UTC.

**Afhankelijkheden**
- Toegang tot dagelijkse GSC/GA extracts.
- Bestaande orchestrator-opportunities en tasks beschikbaar als bron.

**Risico’s + mitigatie**
- Risico: inconsistente URL-normalisatie per site. Mitigatie: centrale normalisatiefunctie + whitelist per domein.
- Risico: data-latency in GSC. Mitigatie: last_complete_date en confidence-penalty bij vertraagde bron.

## Sprint 2 (week 3-4) — Fase A: Delta-first inzichten + Winners/Losers + Risks basis

**Doel**
- Van statische score naar trendgedreven signalering (7d/28d delta’s).

**Scope**
- Winners & Losers scherm met trend-break detectie.
- Risks Monitor basis: indexatie-drop, CTR-drop, rank-drop, feature-loss.
- Edge-case handling voor nieuwe URL’s (cold-start) en seizoenspieken.

**User stories**
- Als SEO lead wil ik winners/losers direct zien, zodat ik momentum kan versnellen of verlies kan stoppen.
- Als tech SEO wil ik high-value technische risico’s bovenaan zien.

**Acceptatiecriteria**
- Delta’s beschikbaar op 7d en 28d voor clicks/impressions/CTR/avg position.
- Risk flags hebben severity (low/medium/high) en impact-estimate.
- False positive-rate op risk-flags <20% in steekproef van 50 cases.

**Afhankelijkheden**
- Historische tijdreeks van minimaal 56 dagen voor stabiele vergelijking.

**Risico’s + mitigatie**
- Risico: seizoensruis veroorzaakt verkeerde alerts. Mitigatie: week-over-week baseline + minimum threshold.

## Sprint 3 (week 5-6) — Fase B: Execution Funnel + taakcreatie

**Doel**
- Cockpit wordt operationeel: suggested → approved → in progress → done.

**Scope**
- 1-click taakcreatie vanuit cockpitregel.
- Verplichte velden: owner, due date, effort (S/M/L), expected uplift.
- Execution Funnel scherm met doorlooptijd per stap.

**User stories**
- Als SEO manager wil ik direct taken aan owners toewijzen zonder contextverlies.
- Als content/dev owner wil ik duidelijke scope en verwacht effect per taak.

**Acceptatiecriteria**
- 90% van goedgekeurde opportunities kan zonder handmatig kopiëren naar taak worden omgezet.
- Elke taak heeft lifecycle-status en timestamp per overgang.
- Funnel toont conversiepercentages per status.

**Afhankelijkheden**
- Rechtenmodel (wie mag approven/toewijzen/sluiten).

**Risico’s + mitigatie**
- Risico: adoption blijft laag. Mitigatie: cockpit-first werkafspraak + wekelijkse compliance review.

## Sprint 4 (week 7-8) — Fase B: Upliftmeting + Business Lens

**Doel**
- Meetbaar maken of taken echt SEO/business impact leveren.

**Scope**
- Measured uplift stap in funnel (7d en 28d na done).
- Business Lens: organisch verkeer × conversieproxy × clusterwaarde.
- Baseline-lock bij taakstart om eerlijke before/after te vergelijken.

**User stories**
- Als owner wil ik zien of mijn taak resultaat levert en hoe groot dat resultaat is.
- Als product lead wil ik budget sturen op bewezen playbooks.

**Acceptatiecriteria**
- Voor 100% van done-taken wordt automatisch een 7d/28d meetmoment aangemaakt.
- Upliftlabel: positive / neutral / negative met betrouwbaarheidsindicatie.
- Business Lens toont netto groei per cluster.

**Afhankelijkheden**
- GA conversie-events of bruikbare surrogate KPI.

**Risico’s + mitigatie**
- Risico: attributieconflicten bij meerdere parallelle wijzigingen. Mitigatie: overlap-flag + reduced confidence.

## Sprint 5 (week 9-10) — Fase C: Model tuning + anti-gaming + playbook automatisering

**Doel**
- Score betrouwbaarder maken op basis van eerste upliftdata.

**Scope**
- Gewicht-tuning per opportunity type (quick win/defensief/technisch/groei).
- Anti-gaming controles (effort inflation, cherry-picking, metric clipping).
- Automatische suggestie van top 6 playbooks met contextuele parameterisatie.

**User stories**
- Als SEO lead wil ik dat score realistischer wordt naarmate we leren.
- Als governance-owner wil ik manipulatie van score voorkomen.

**Acceptatiecriteria**
- Nieuwe scoreversie heeft changelog + vergelijkingsrapport met vorige versie.
- Minimaal 3 anti-gaming regels actief en zichtbaar in explainability.

**Afhankelijkheden**
- Minimaal 100 taken met status done + eerste meetresultaten.

**Risico’s + mitigatie**
- Risico: overfitting op kleine sample. Mitigatie: guardrails met capped gewichtswijziging per maand.

## Sprint 6 (week 11-12) — Fase C: Opschaling, SLA’s en adoption

**Doel**
- Cockpit als standaard werkwijze borgen over teams/sites.

**Scope**
- Team-SLA’s: triage, approval, uitvoering, meting.
- Site roll-out in waves + enablement (playbooks, training, checklists).
- KPI-dashboard voor cockpit-adoptie en resultaat.

**User stories**
- Als operations lead wil ik kunnen zien waar workflow vastloopt.
- Als management wil ik cockpit-bijdrage aan groei objectief kunnen volgen.

**Acceptatiecriteria**
- >90% van nieuwe SEO taken start vanuit cockpit.
- Median time-to-action <5 werkdagen.
- Uplift hitrate per maand inzichtelijk per playbook.

**Afhankelijkheden**
- Teamcapaciteit content/dev/analytics.

**Risico’s + mitigatie**
- Risico: verschillende volwassenheid per site. Mitigatie: pilot-tiering + gefaseerde onboarding.

## Week 13 — Hardening + go-live evaluatie

**Doel**
- Stabiliseren, opschonen, beslissen over v1.1 backlog.

**Scope**
- Datakwaliteitsaudit, performance tuning, governance review.
- Besluitdocument: wat naar v1.1 verschuift.

---

# Data model & integratie-architectuur

## Unified URL/cluster model

**Kernentiteiten (v1)**
1. `seo_url` (dagelijkse snapshots op canonical URL)
2. `seo_cluster` (topic/intentiegroep)
3. `seo_signal` (risico/opportunity events)
4. `seo_opportunity` (deduped suggestie met score)
5. `seo_task` (uitvoering + funnelstatus)
6. `seo_uplift_measurement` (7d/28d evaluatie)

**Relaties**
- `seo_url` N:1 `seo_cluster`
- `seo_signal` N:1 `seo_url` (optioneel direct op cluster)
- `seo_opportunity` 1:1 met actieve combinatie `(canonical_url_id, opportunity_type, lookback_window)`
- `seo_task` N:1 `seo_opportunity` (historiek), max 1 actieve taak per opportunity

## Mapping bronnen → cockpitvelden

| Bron | Inputvelden | Cockpitvelden | Transformatieregel |
|---|---|---|---|
| GSC | page, query, clicks, impressions, ctr, position | `clicks_7d`, `impr_7d`, `ctr_7d`, `pos_7d`, delta’s, query_mix | Aggregatie per canonical URL + intent-tag op queryset |
| GA | landing page, sessions, engaged_sessions, conversions | `organic_sessions`, `engagement_rate`, `conv_proxy`, `business_weight` | URL join + smoothing op low-volume data |
| SERP signals | feature presence, rank volatility, competitor moves | `serp_volatility`, `feature_loss_flag`, `urgency_modifier` | Dagelijkse event-normalisatie naar severity |
| Intelligence signals | content/tech flags | `issue_taxonomy`, `issue_severity`, `trust_modifier` | Mapping naar vaste taxonomie (content/tech/intent/internal links) |
| Tasks/opportunities | status, owner, due date, action type | `funnel_stage`, `cycle_time`, `playbook_type`, `execution_state` | Dedupe en lifecycle harmonisatie |

## Canonical identifiers

- `canonical_url_id = sha1(lower(host) + normalized_path)`
- `cluster_id = sha1(site_id + primary_topic + intent_type)`
- `opportunity_id = sha1(canonical_url_id + opportunity_type + lookback_window + rule_version)`
- `task_id` blijft systeem-native, maar krijgt `opportunity_id` als foreign key.

## Deduplicatie-logica

1. Normaliseer URL (protocol/host/case/slash/query params tracking verwijderen).
2. Match op canonical tags waar beschikbaar; anders heuristiek op path-equivalentie.
3. Per `canonical_url_id + opportunity_type` slechts één actieve opportunity.
4. Nieuwe signalen binnen cooldown (14 dagen) updaten bestaande opportunity i.p.v. nieuwe aanmaak.
5. Als taak actief is, nieuwe duplicate suggestions gaan naar evidence-log.

## Datakwaliteitschecks

- **Completeness**: % URL’s met GSC+GA data laatste 3 dagen.
- **Uniqueness**: duplicate ratio op canonical_url_id <1%.
- **Freshness**: bronvertraging per datasource in uren.
- **Consistency**: CTR = clicks/impressions binnen tolerantie.
- **Validity**: posities en CTR binnen domeinregels (0–100 voor CTR, positie >0).

## Refresh-frequentie

- GSC/GA ingestion: 1x per dag (nachtbatch).
- SERP/signals: 1–2x per dag (afhankelijk van kosten).
- Score recalculatie: dagelijks na laatste bron binnenkomst.
- Uplift updates: dagelijks voor open meetvensters.

## Fallback bij ontbrekende data

- Ontbrekende GA: gebruik clustergemiddelde conversieproxy met confidence-penalty.
- Ontbrekende GSC op URL: fallback naar clustertrend, maar max score cap 60.
- Ontbrekende SERP signalen: urgency_modifier = neutraal (1.0).
- Geen historische data (nieuwe URL): cold-start mode met beperkte opportunity types.

---

# Prioriteitsscore v1 + voorbeelden

## Formule

`Prioriteit (0–100) = round( 0.40*Impact + 0.30*Kans + 0.20*Vertrouwen + 0.10*Snelheid )`

> Productregel: de multiplicatieve strategie uit de visie vertalen we in v1 naar gewogen subscores voor stabiliteit, uitlegbaarheid en minder extreme uitschieters. We tonen beide in debug-view voor modelvergelijking.

## Subscore-definities (0–100)

### 1) Impact
- Input: `impression_gap`, `expected_ctr_uplift`, `conv_proxy`, `business_value_factor`.
- Formule (ruw): `impact_raw = impression_gap * expected_ctr_uplift * conv_proxy * business_value_factor`.
- Normalisatie: percentiel-schaal per site (P10=10, P50=50, P90=90, cap 100).

### 2) Kans
- Input: historisch succes van playbook-type, huidige positieband, SERP-competitie, content-fit.
- Formule: gewogen kansindex waarbij positie 4-12 en bewezen playbook uplift bonus krijgen.
- Normalisatie: min-max op rolling 90 dagen + stabiliteitsbonus bij lage volatiliteit.

### 3) Vertrouwen
- Input: datacompleteness, bronconsensus, sample size, trendstabiliteit.
- Score start op 100 en krijgt penalties (bijv. -20 voor ontbrekende GA, -15 voor hoge SERP-ruis).
- Floor op 20 om cold-start kansen zichtbaar maar lager geprioriteerd te houden.

### 4) Snelheid
- Input: effortklasse (S/M/L), dependency count, expected time-to-live.
- Mapping v1: S=90, M=60, L=30; -10 bij cross-team dependency.
- Doel: quick wins eerder in top 15 bij gelijke impact.

## Gewichten + motivatie

- **Impact 40%**: primaire bedrijfswaarde.
- **Kans 30%**: voorkomt dat theoretische impact zonder uitvoerbaarheid bovenaan komt.
- **Vertrouwen 20%**: beschermt tegen ruis/slechte data.
- **Snelheid 10%**: stuurt op tempo zonder strategische impact te verdringen.

Trade-off:
- Hogere snelheid-gewicht zou te veel cosmetische quick wins stimuleren.
- Lagere vertrouwen-gewicht verhoogt risico op verkeerde prioriteiten.

## Explainability-output (“waarom score X?”)

Per opportunity tonen we 5 regels:
1. `Topdriver`: “Hoge impressie-gap in positie 5–8 segment”.
2. `Business`: “Cluster heeft bovengemiddelde conversieproxy (+32%).”
3. `Confidence`: “Data compleetheid 4/5 bronnen; lichte GSC-latency.”
4. `Execution`: “Effort S, geen dev dependency.”
5. `Constraint`: “Score gecapt op 85 wegens actieve vergelijkbare taak.”

## Anti-gaming regels

1. **Effort integrity check**: structureel te laag/hoog effort label per owner triggert audit.
2. **Cooldown anti-spam**: zelfde URL/opportunity niet opnieuw pushen binnen 14 dagen zonder nieuw bewijs.
3. **Attribution guardrail**: uplift claim ongeldig bij overlap met >2 majeure wijzigingen zonder isolatie.
4. **Score cap bij incomplete data**: vertrouwen <40 limiteert totaalscore op max 70.
5. **No vanity inflation**: opportunities met hoge impressies maar zeer lage business_weight krijgen impact-demping.

## Rekenvoorbeelden

### Voorbeeld A — Quick Win (CTR optimalisatie, positie 6)
- Impact 78, Kans 72, Vertrouwen 85, Snelheid 90
- Score = 0.40*78 + 0.30*72 + 0.20*85 + 0.10*90
- Score = 31.2 + 21.6 + 17 + 9 = **78.8 → 79**
- Waarom hoog: sterk volume, goede uitvoerbaarheid, snel live.

### Voorbeeld B — Defensief (28d traffic drop op historisch sterke pagina)
- Impact 82, Kans 64, Vertrouwen 88, Snelheid 60
- Score = 32.8 + 19.2 + 17.6 + 6 = **75.6 → 76**
- Waarom net onder quick win: herstelkans hoog, maar iets meer uitwerktijd.

### Voorbeeld C — Technisch (indexatie/canonical issue op money page)
- Impact 92, Kans 58, Vertrouwen 70, Snelheid 35
- Score = 36.8 + 17.4 + 14 + 3.5 = **71.7 → 72**
- Waarom niet #1: impact enorm, maar complexere uitvoering en afhankelijkheden.

---

# UX & operationele workflow

## Scherm 1: Today Board

**KPI’s**
- Aantal high-priority open opportunities.
- Gemiddelde score top 15.
- Time-to-first-action.

**Filters**
- Site, cluster, opportunity type, owner, effort, confidence band.

**Primaire acties**
- Approve, assign owner, set due date, create task.

**Edge cases**
- Geen data vandaag: fallback naar laatste complete dag + waarschuwing.
- Veel gelijke scores: sorteer op hoogste business_weight en laagste effort.

## Scherm 2: Winners & Losers

**KPI’s**
- Netto clicks delta 7d/28d.
- Aantal trend-breaks.

**Filters**
- Delta type, merk/non-merk, cluster intentie.

**Primaire acties**
- Promote winner (opschalen playbook), create defensive task voor losers.

**Edge cases**
- Nieuwe URL zonder baseline: label “insufficient history”.

## Scherm 3: Risks Monitor

**KPI’s**
- # kritieke risico’s open.
- Gemiddelde tijd tot mitigatie.

**Filters**
- Risicotype (indexatie, cannibalisatie, CTR, SERP feature loss), severity.

**Primaire acties**
- Escalate naar dev/content, link naar evidence.

**Edge cases**
- Tijdelijke trackingfout: auto-suppress na validatie door analytics.

## Scherm 4: Execution Funnel

**KPI’s**
- Conversie per funnelstap.
- Doorlooptijd per stap.
- WIP per team.

**Filters**
- Team, owner, playbook, maand.

**Primaire acties**
- Bottleneck oplossen (herverdeling capaciteit), SLA waarschuwing.

**Edge cases**
- Taak teruggezet van done naar in progress: meetvenster herstart met auditlog.

## Scherm 5: Business Lens

**KPI’s**
- Netto organische groei per cluster.
- Uplift hitrate per playbook.
- Waarde-output per uur inspanning.

**Filters**
- Cluster, funnel status, device, markt.

**Primaire acties**
- Herprioriteren budget/capaciteit naar best presterende clusters.

**Edge cases**
- Lage traffic, hoge conversiewaarde: minimum-volume uitzondering met handmatige review.

## End-to-end flow

1. **Suggested**: engine genereert deduped opportunities.
2. **Approved**: SEO lead valideert, voegt owner/due/effort toe.
3. **In progress**: owner voert actie uit met subtaken.
4. **Done**: wijziging live + baseline lock.
5. **Measured uplift**: automatisch 7d/28d evaluatie en score-feedback.

SLA’s (v1):
- Suggested → Approved: <48 uur.
- Approved → In progress: <3 werkdagen.
- Done → Measured(7d): automatisch op dag 7.

---

# Playbooks per kans-type

## 1) High impressions + lage CTR + positie 3–10
- **Trigger-criteria**: impressions > P70, CTR < benchmark -20%, positie 3–10.
- **Aanbevolen actie**: title/meta rewrite + snippet intent alignment.
- **Expected impact**: +10% tot +25% clicks in 28 dagen.
- **Effort**: S.
- **Meetplan**: 7d early CTR check, 28d clicks uplift.
- **Stop/rollback**: rollback als CTR na 14 dagen >10% daalt.

## 2) Positie 8–20 met hoge business-intentie
- **Trigger-criteria**: positie 8–20 + business_weight >1.2.
- **Aanbevolen actie**: content gap aanvullen + interne link boost.
- **Expected impact**: +15% tot +40% clicks.
- **Effort**: M.
- **Meetplan**: 7d ranking trend, 28d verkeer + conv_proxy.
- **Stop/rollback**: stop als na 28 dagen geen positiewinst en engagement verslechtert.

## 3) Cannibalisatieclusters
- **Trigger-criteria**: >2 URL’s op overlappende queryset met wisselende ranking.
- **Aanbevolen actie**: consolidatie, canonical/redirect, intentie-herpositionering.
- **Expected impact**: +10% tot +30% cluster clicks.
- **Effort**: M/L.
- **Meetplan**: 7d ranking-stabiliteit, 28d cluster netto clicks.
- **Stop/rollback**: rollback redirect/canonical bij indexverlies op primaire URL.

## 4) Historisch sterk, recent dalend
- **Trigger-criteria**: 28d clicks -20% vs vorige 28d, historisch topquartiel.
- **Aanbevolen actie**: defensieve content refresh + SERP delta analyse.
- **Expected impact**: terugwinnen 30%–60% van verlies.
- **Effort**: M.
- **Meetplan**: 7d trend stabilisatie, 28d herstelratio.
- **Stop/rollback**: stop als daling door externe factor (seizoen/nieuws) wordt bevestigd.

## 5) Verkeer hoog, engagement/conversie laag
- **Trigger-criteria**: sessions > P70, engagement < P30 of conv_proxy < P30.
- **Aanbevolen actie**: intent-mismatch fix, CTA- en contentblokken optimaliseren.
- **Expected impact**: +5% tot +20% conv_proxy uplift.
- **Effort**: M.
- **Meetplan**: 7d engagement shift, 28d conversieproxy.
- **Stop/rollback**: rollback UX wijziging bij significante bounce stijging.

## 6) Technische blokkades op high-value URL’s
- **Trigger-criteria**: indexeerbaarheid/canonical/schema/CWV issue + high business_weight.
- **Aanbevolen actie**: gefocuste technische fix op waarde-URL set.
- **Expected impact**: +10% tot +50% afhankelijk van blokkade.
- **Effort**: M/L.
- **Meetplan**: 7d index status/validatie, 28d verkeerstrend.
- **Stop/rollback**: rollback bij onverwachte de-indexatie of performance regressie.

---

# Governance, KPI’s en ritme

## Rollen en verantwoordelijkheden

- **SEO Lead**: prioritering, approvals, model-tuningbeslissingen.
- **Content team**: contentgerelateerde uitvoering en QA.
- **Dev/Tech SEO**: technische fixes, releaseplanning, validatie.
- **Analytics/Data PM**: datakwaliteit, metric-definities, upliftmeting.
- **Business owner**: waardeweging, capaciteitsbeslissingen, escalaties.

## Beslisritme

- **Dagelijks (15 min triage)**: Today Board review, top 15 beslissen.
- **Wekelijks (45 min prioritering)**: capaciteit vs backlog, blokkades, SLA review.
- **Maandelijks (60 min model tuning)**: scorekalibratie, playbook performance, roadmap keuzes.

## KPI-framework + targets

1. **% taken gestart vanuit cockpit**
   - Doel: >90% vanaf week 10.
2. **Median time-to-action (suggested → in progress)**
   - Doel: <5 werkdagen vanaf week 8.
3. **Uplift hitrate (28d positive uplift)**
   - Doel: >55% in eerste 90 dagen, >65% na 6 maanden.
4. **Netto organische groei per cluster**
   - Doel: positief in >70% van strategische clusters per maand.

---

# Backlog met implementatietickets

## Epic 1 — Unified data foundation

### Feature 1.1 URL canonicalization service
- Ticket: parser + normalisatie regels
- Ticket: canonical mapping job
- Ticket: duplicate monitor report
- **Definition of Done**: >95% URL mapping, <1% duplicates, monitoring actief.

### Feature 1.2 Cluster model
- Ticket: cluster key generator
- Ticket: intent classification v1
- Ticket: cluster roll-up metrics view
- **Definition of Done**: alle actieve URL’s gekoppeld aan cluster + valide rollups.

## Epic 2 — Scoring & explainability

### Feature 2.1 Priority engine v1
- Ticket: subscore calculators
- Ticket: weighting config + versioning
- Ticket: score recompute scheduler
- **Definition of Done**: score dagelijks beschikbaar + versiebeheer.

### Feature 2.2 Explainability panel
- Ticket: topdrivers formatter
- Ticket: evidence links per bron
- Ticket: score constraints weergave
- **Definition of Done**: elke opportunity heeft uitlegbare score-output.

## Epic 3 — Cockpit UX

### Feature 3.1 Today Board
- Ticket: top 15 list + filters
- Ticket: action buttons (approve/assign/create task)
- **Definition of Done**: triage volledig mogelijk zonder externe tab.

### Feature 3.2 Winners/Losers + Risks
- Ticket: delta widgets 7d/28d
- Ticket: risk severity cards
- **Definition of Done**: trend/risk signalen in één workflow.

### Feature 3.3 Execution Funnel + Business Lens
- Ticket: funnel stage timeline
- Ticket: uplift status badges
- Ticket: cluster business KPI kaart
- **Definition of Done**: end-to-end status + waardeweergave beschikbaar.

## Epic 4 — Measurement & experimentation

### Feature 4.1 Uplift tracker
- Ticket: baseline snapshot on done
- Ticket: day7/day28 evaluator
- Ticket: uplift label engine
- **Definition of Done**: done-taken krijgen automatische outcome.

### Feature 4.2 Model tuning loop
- Ticket: monthly performance report
- Ticket: weight adjustment guardrails
- **Definition of Done**: maandelijks tuneproces reproduceerbaar.

## Event tracking plan (v1)

- `cockpit_view_loaded` (user, site, filters)
- `opportunity_approved` (opportunity_id, score, owner)
- `task_created_from_cockpit` (task_id, playbook_type)
- `task_stage_changed` (from_stage, to_stage, latency)
- `uplift_measured` (task_id, day_window, uplift_label, uplift_value)
- `score_version_changed` (old_version, new_version, reason)

## SQL/metric views (conceptueel)

1. `vw_seo_cockpit_daily`
   - sleutel: canonical_url_id, date
   - metrics: score, subscores, deltas, risk flags, next_action
2. `vw_execution_funnel`
   - sleutel: task_id
   - metrics: stage timestamps, cycle times, owner/team
3. `vw_uplift_outcomes`
   - sleutel: task_id, day_window
   - metrics: baseline/current, uplift_abs, uplift_pct, confidence
4. `vw_cluster_business_lens`
   - sleutel: cluster_id, date
   - metrics: organic_sessions, conv_proxy, net_growth, value_per_effort_hour

## Dashboard-spec voor BI

- Pagina 1: Adoption & SLA (cockpit-start-rate, time-to-action, WIP)
- Pagina 2: Outcome (hitrate, uplift distributie, top playbooks)
- Pagina 3: Cluster performance (net growth, risico’s, investering vs resultaat)
- Pagina 4: Data quality (freshness, completeness, duplicate ratio)

## Testplan

### Functioneel
- Opportunity lifecycle transitions + rechten.
- Filterlogica en sortering op score.
- Task creation vanuit cockpit zonder dataverlies.

### Datakwaliteit
- URL dedupe tests op edge-case URL’s.
- Delta-berekening validatie (7d/28d).
- Score reproducerbaarheid per versie.

### SEO-validatie
- Handmatige review op top 50 kansen: relevantie en uitvoerbaarheid.
- SERP/CTR impact check op quick-win cohort.
- Indexatie-validatie na technische fixes.

## Rollout plan (pilot → gefaseerde uitrol)

1. **Pilot (2 sites, 4 weken)**
   - Focus: data integrity + adoption.
2. **Wave 1 (top 20% verkeer-sites)**
   - Focus: maximale zichtbare uplift.
3. **Wave 2 (restportfolio)**
   - Focus: standaardisatie + teamtraining.
4. **Stabilisatiefase**
   - Focus: model tuning, governance aanscherping.

---

# Risico’s, aannames en open beslissingen

## Toprisico’s

1. Lage databetrouwbaarheid in bronfeeds.
2. Lage adoptie door parallelle werkwijzen buiten cockpit.
3. Onjuiste upliftconclusies door overlap in veranderingen.
4. Capaciteitsknelpunten bij content/dev teams.

## Aannames

- Dagelijkse brondata blijft beschikbaar met beperkte latency.
- Teams accepteren cockpit-first werkwijze binnen 6–8 weken.
- Baseline lock + overlap flags zijn voldoende voor v1-outcome meting.

## Open beslissingen

1. Standaard business_value_factor per site of centraal model?
2. Eén globale score of site-specifieke gewichten vanaf sprint 5?
3. Effort-inschatting door owner of door centrale triage?
4. Definitie van “high business intent” (rule-based vs model-based).

---

# Week-1 actieplan (dag 1 t/m 5)

## Dag 1 — Scope lock & metric contract
- Finaliseer v1 scope + expliciete “not in v1” lijst.
- Leg metric definities vast voor impact/kans/vertrouwen/snelheid.
- Bepaal cockpit owner per team.

## Dag 2 — Data mapping workshop
- Mapping van GSC/GA/SERP/signals/tasks naar cockpitvelden valideren.
- Edge cases verzamelen (canonical conflicts, ontbrekende data, nieuwe URL’s).

## Dag 3 — URL/cluster pipeline setup
- Implementatie canonicalisatie + cluster-key generatie in batch.
- Eerste datakwaliteitsrapport (completeness/duplicates/freshness).

## Dag 4 — Scoring prototype + explainability
- Bereken eerste score op subset van URL’s.
- Handmatige QA op top 30 kansen met SEO lead.

## Dag 5 — Today Board dry run
- Simuleer dagelijkse triage met echte stakeholders.
- Log blockers, pas prioriteitsregels aan, plan sprint-2 backlog.

**Binnen 2 weken zichtbaar resultaat (time-to-value):**
- Top 15 prioriteiten dagelijks zichtbaar.
- Eerste quick-win taken aangemaakt vanuit cockpit.
- Eerste score-uitleg beschikbaar per opportunity.
