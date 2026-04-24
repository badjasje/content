# SEO Cockpit hardening & kwaliteitsronde (Sprint 1 t/m 6)

Datum: 2026-04-24

## Uitgevoerde technische hardening

1. **DB-upgrade idempotentie verbeterd**
   - Nieuwe `maybe_add_index()` helper toegevoegd.
   - Kritieke indexen worden nu conditioneel (idempotent) toegevoegd op `seo_task`, `seo_uplift_measurement`, `seo_signal` en `seo_url`.

2. **Cron refresh fail-safe bij ontbrekende tabellen**
   - `run_seo_cockpit_daily_refresh()` stopt nu gecontroleerd met status `blocked_missing_tables` en logging als vereiste tabellen ontbreken.
   - Hiermee voorkomen we stille partial runs en inconsistente state bij onvolledige upgrades.

3. **Admin performance verbeterd (tab lazy loading)**
   - `render_seo_cockpit()` laadt alleen nog data voor de actieve tab.
   - Voorkomt onnodige zware query-uitvoer op iedere paginalaad.

4. **Execution Funnel query-schaalbaarheid**
   - Toegevoegde paginering (`funnel_page`, 50 records per pagina).
   - Query is opgesplitst in `COUNT(*)` + paginated select, met veilige prepared statements.

5. **JSON robustness**
   - Explainability JSON in Today Board gebruikt nu `decode_json_array()` helper voor veilige fallback naar lege array.

## Geconstateerde risico’s / open aandachtspunten

- Nog niet alle SEO cockpit datasets gebruiken expliciete paginering (sommige schermen werken met vaste `LIMIT` waardes).
- Sommige tabellen zijn historisch gegroeid (`seo_page_tasks` legacy + `seo_task` cockpit) en vragen expliciete lifecycle-documentatie om verwarring te beperken.
- End-to-end compatibiliteit met oudere dataformaten is functioneel aanwezig, maar heeft nog geen geautomatiseerde regressietests.

## v1.1 backlog voorstel (kort)

1. Uniforme paginering op alle grote admin-overzichten.
2. Centrale JSON decode/encode utility gebruiken in alle cockpit paden.
3. Migratie-auditrapport in admin (incl. index/kolom status + herstelacties).
4. E2E smoke tests voor alle cockpit tabs (met lege dataset en met gesimuleerde grote dataset).
5. Extra observability:
   - structured run-id per refresh
   - counters voor skipped/failed opportunity updates
   - query-timing logging per cockpit tab.

## Definition-of-Done mapping

- Fatal error hardening: verbeterd via table guard in cron flow.
- Admin UX zonder data: aanwezig (empty states behouden).
- Database upgrades idempotent: uitgebreid met idempotente index-migraties.
- Cron/logging/scoring robuuster: cron pre-check + logging toegevoegd.
- v1.1 backlog: opgenomen in dit document.
