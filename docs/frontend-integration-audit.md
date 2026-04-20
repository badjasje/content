# Content Hub Frontend Integration Audit

## Existing integration surface (kept intact)

### REST endpoints already present
- `GET /wp-json/sch/v1/intelligence/opportunities`
- `GET /wp-json/sch/v1/intelligence/url-detail`
- `POST /wp-json/sch/v1/intelligence/tasks`

### Admin-post actions already present
- CRUD and orchestration actions via `admin-post.php` (e.g. `sch_save_client`, `sch_save_site`, `sch_save_keyword`, `sch_run_now`, `sch_retry_job`, `sch_approve_publish`, `sch_bulk_approve_publish`).
- SEO intelligence lifecycle actions (e.g. `sch_mark_signal_resolved`, `sch_mark_serp_signal_ignored`, `sch_run_intelligence_ingest`, task status actions).
- Integrations and OAuth actions for GSC/GA (connect, callback, disconnect, fetch/save property, sync).

### Nonce/auth model
- Existing admin-post actions use `check_admin_referer(...)` and `current_user_can('manage_options')`.
- Existing intelligence REST routes use capability checks (`manage_options`).
- New app layer REST uses the same capability model, authenticated via WP cookie + `X-WP-Nonce` (`wp_rest`).

### Data models and storage used by current plugin
- Core tables: `sch_clients`, `sch_sites`, `sch_keywords`, `sch_jobs`, `sch_articles`, `sch_logs`.
- Intelligence/performance tables include: `sch_feedback_signals`, `sch_serp_signals`, `sch_refresh_candidates`, `sch_orchestrator_opportunities`, `sch_orchestrator_tasks`, plus GSC/GA metric tables.
- Existing options retained and reused (OpenAI, discovery, random-machine, GSC/GA, SERP provider, etc.).

## New additive frontend adapter endpoints
- `GET /wp-json/sch/v1/app/bootstrap`
- `GET /wp-json/sch/v1/app/keywords`
- `POST /wp-json/sch/v1/app/keywords/{id}`
- `GET /wp-json/sch/v1/app/issues`
- `POST /wp-json/sch/v1/app/issues/{type}/{id}`
- `GET /wp-json/sch/v1/app/queue`
- `POST /wp-json/sch/v1/app/queue/run-worker`
- `GET /wp-json/sch/v1/app/settings`
- `POST /wp-json/sch/v1/app/settings`

These endpoints are additive wrappers over existing plugin data structures and capabilities.
