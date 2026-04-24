# SEO Cockpit – Fase 1 Analyse & implementatieplan

## Huidige plugin-architectuur (samenvatting)
- De plugin draait grotendeels vanuit één hoofdklasse `SCH_Orchestrator` in `content-hub-orchestrator.php`.
- Er bestaan al tabellen voor klanten, sites, artikelen, jobs, logs, GSC/GA page metrics, overlay-data, signalen, kansen en taken.
- De admin heeft al veel submenu’s (Dashboard, Klanten, Blogs, Keywords, Intelligence, Page Intelligence, enz.), plus een frontend app (`sch-app`) met REST endpoints.
- Cron en sync flows zijn aanwezig voor worker, GSC, SERP, GA en feedback.

## Relevante bestaande tabellen voor SEO cockpit
- `sch_clients`, `sch_sites`, `sch_articles`
- `sch_gsc_page_metrics`, `sch_gsc_query_metrics`, `sch_gsc_query_page_metrics`
- `sch_ga_page_metrics`, `sch_page_overlay_daily`
- `sch_feedback_signals`, `sch_serp_signals`, `sch_serp_recommendations`
- `sch_orchestrator_page_metrics_daily`, `sch_orchestrator_opportunities`, `sch_orchestrator_tasks`

## Kernbeslissingen voor uitbreiding
1. **Geen rewrite, wel uitbreiding**: bestaande tabellen en flows blijven intact.
2. **Nieuwe centrale pagina-entiteit**: toegevoegd als `sch_seo_pages`.
3. **Nieuwe SEO-taakentiteit**: toegevoegd als `sch_seo_page_tasks` met dedupe hash.
4. **Idempotente migratie**: via bestaande `create_tables()` + `dbDelta()` strategie.
5. **Backwards compatibility**: geen bestaande adminpagina verwijderd of flow aangepast.

## Gefaseerd vervolg (hoog niveau)
- **Fase 2**: database + helpers (upsert/read) voor pages en tasks. ✅ (eerste veilige implementatie gezet)
- **Fase 3**: nieuwe adminpagina “SEO Cockpit” met filters/cards/tabellen.
- **Fase 4**: pagina detailweergave met tabbladen + taakstatus updates.
- **Fase 5**: modulaire recommendation engine + detector registry.
- **Fase 6**: koppeling GSC paginiveau metrics aan `sch_seo_pages`.
- **Fase 7**: handmatige recalculatie actie + logging + testchecklist.

## Risico’s en migratiepunten
- Grote monolithische pluginfile verhoogt regressierisico; uitbreidingen moeten klein en geïsoleerd blijven.
- Datakwaliteit van URL/path matching kan variëren per site; normalisatie moet consistent blijven.
- Deduplicatie voor taken vereist stabiele metadata-structuur.
- Nieuwe tabellen moeten zonder bestaande data te beïnvloeden uitrolbaar zijn.

## Productie-aandacht
- DB upgrade draait wanneer `DB_VERSION` wijzigt; eerst testen op staging met realistische dataset.
- Bij hogere volumes indexgebruik monitoren op `client_id + path` en `status + priority` queries.
- Aanbevolen: eerst read-only cockpit-views live, daarna pas geautomatiseerde taakgeneratie activeren.
