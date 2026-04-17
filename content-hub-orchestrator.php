<?php
/*
Plugin Name: Shortcut Content Hub Orchestrator
Description: Centrale content orchestrator voor klanten, keyword discovery, jobs en distributie naar externe WordPress blogs via een receiver plugin. Inclusief AI schrijf- en redactieflow, website research, Unsplash featured images en bulk blog import.
Version: 0.5.5
Author: OpenAI
*/

if (!defined('ABSPATH')) {
    exit;
}

final class SCH_Orchestrator {
    const VERSION = '0.5.5';
    const CRON_HOOK = 'sch_orchestrator_minute_worker';
    const GSC_CRON_HOOK = 'sch_orchestrator_gsc_sync_worker';
    const REGISTRATION_ACTION = 'sch_register_receiver_blog';
    const OPTION_DB_VERSION = 'sch_orchestrator_db_version';
    const DB_VERSION = '0.5.9';
    const EXACT_MATCH_THRESHOLD_PERCENT = 30;

    const OPTION_OPENAI_API_KEY = 'sch_openai_api_key';
    const OPTION_OPENAI_MODEL = 'sch_openai_model';
    const OPTION_OPENAI_TEMPERATURE = 'sch_openai_temperature';
    const OPTION_UNSPLASH_ACCESS_KEY = 'sch_unsplash_access_key';
    const OPTION_ENABLE_FEATURED_IMAGES = 'sch_enable_featured_images';
    const OPTION_ENABLE_SUPPORTING = 'sch_enable_supporting';
    const OPTION_ENABLE_AUTO_DISCOVERY = 'sch_enable_auto_discovery';
    const OPTION_MAX_RESEARCH_PAGES = 'sch_max_research_pages';
    const OPTION_MAX_DISCOVERY_KEYWORDS = 'sch_max_discovery_keywords';
    const OPTION_ENABLE_VERBOSE_LOGS = 'sch_enable_verbose_logs';
    const OPTION_TRUSTED_SOURCE_DOMAIN = 'sch_trusted_source_domain';
    const OPTION_RANDOM_MACHINE_ENABLED = 'sch_random_machine_enabled';
    const OPTION_RANDOM_DAILY_MAX = 'sch_random_daily_max';
    const OPTION_RANDOM_STATUS = 'sch_random_status';
    const OPTION_RANDOM_MIN_WORDS = 'sch_random_min_words';
    const OPTION_RANDOM_MAX_WORDS = 'sch_random_max_words';
    const OPTION_RANDOM_MAX_PER_SITE_PER_DAY = 'sch_random_max_per_site_per_day';
    const OPTION_RANDOM_ONLY_ACTIVE_SITES = 'sch_random_only_active_sites';
    const OPTION_RANDOM_ALLOWED_CATEGORIES = 'sch_random_allowed_categories';
    const OPTION_RANDOM_DUPLICATE_WINDOW_DAYS = 'sch_random_duplicate_window_days';
    const OPTION_GSC_ENABLED = 'sch_gsc_enabled';
    const OPTION_GSC_CLIENT_ID = 'sch_gsc_client_id';
    const OPTION_GSC_CLIENT_SECRET = 'sch_gsc_client_secret';
    const OPTION_GSC_DEFAULT_SYNC_RANGE = 'sch_gsc_default_sync_range';
    const OPTION_GSC_DEFAULT_ROW_LIMIT = 'sch_gsc_default_row_limit';
    const OPTION_GSC_DEFAULT_TOP_N_CLICKS = 'sch_gsc_default_top_n_clicks';
    const OPTION_GSC_DEFAULT_MIN_IMPRESSIONS = 'sch_gsc_default_min_impressions';
    const OPTION_GSC_AUTO_SYNC = 'sch_gsc_auto_sync';


    private static ?SCH_Orchestrator $instance = null;
    private wpdb $db;

    public static function instance(): self {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function __construct() {
        global $wpdb;
        $this->db = $wpdb;

        $current_db_version = (string) get_option(self::OPTION_DB_VERSION, '');
        if ($current_db_version !== self::DB_VERSION) {
            $this->create_tables();
        }

        register_activation_hook(__FILE__, [$this, 'activate']);
        register_deactivation_hook(__FILE__, [$this, 'deactivate']);

        add_filter('cron_schedules', [$this, 'add_cron_schedule']);
        add_action(self::CRON_HOOK, [$this, 'run_worker']);
        add_action(self::GSC_CRON_HOOK, [$this, 'run_gsc_auto_sync']);

        add_action('admin_menu', [$this, 'admin_menu']);
        add_action('admin_post_sch_save_client', [$this, 'handle_save_client']);
        add_action('admin_post_sch_save_site', [$this, 'handle_save_site']);
        add_action('admin_post_sch_bulk_save_sites', [$this, 'handle_bulk_save_sites']);
        add_action('admin_post_sch_bulk_update_sites_status', [$this, 'handle_bulk_update_sites_status']);
        add_action('admin_post_sch_save_keyword', [$this, 'handle_save_keyword']);
        add_action('admin_post_sch_save_settings', [$this, 'handle_save_settings']);
        add_action('admin_post_sch_run_now', [$this, 'handle_run_now']);
        add_action('admin_post_sch_retry_job', [$this, 'handle_retry_job']);
        add_action('admin_post_sch_approve_publish', [$this, 'handle_approve_publish']);
        add_action('admin_post_sch_bulk_approve_publish', [$this, 'handle_bulk_approve_publish']);
        add_action('admin_post_sch_delete_client', [$this, 'handle_delete_client']);
        add_action('admin_post_sch_delete_site', [$this, 'handle_delete_site']);
        add_action('admin_post_sch_delete_keyword', [$this, 'handle_delete_keyword']);
        add_action('admin_post_sch_discover_keywords', [$this, 'handle_discover_keywords']);
        add_action('admin_post_sch_gsc_connect', [$this, 'handle_gsc_connect']);
        add_action('admin_post_sch_gsc_oauth_callback', [$this, 'handle_gsc_oauth_callback']);
        add_action('admin_post_sch_gsc_disconnect', [$this, 'handle_gsc_disconnect']);
        add_action('admin_post_sch_gsc_fetch_properties', [$this, 'handle_gsc_fetch_properties']);
        add_action('admin_post_sch_gsc_save_property', [$this, 'handle_gsc_save_property']);
        add_action('admin_post_sch_gsc_sync_keywords', [$this, 'handle_gsc_sync_keywords']);
        add_action('admin_post_' . self::REGISTRATION_ACTION, [$this, 'handle_register_receiver_blog']);
        add_action('admin_post_nopriv_' . self::REGISTRATION_ACTION, [$this, 'handle_register_receiver_blog']);

        add_action('admin_enqueue_scripts', [$this, 'admin_assets']);
        $this->schedule_gsc_cron();
    }

    public function activate(): void {
        $this->create_tables();
        $this->schedule_cron();
        $this->schedule_gsc_cron();
    }

    public function deactivate(): void {
        wp_clear_scheduled_hook(self::CRON_HOOK);
        wp_clear_scheduled_hook(self::GSC_CRON_HOOK);
    }

    public function add_cron_schedule(array $schedules): array {
        if (!isset($schedules['every_minute'])) {
            $schedules['every_minute'] = [
                'interval' => 60,
                'display'  => 'Every Minute',
            ];
        }
        return $schedules;
    }

    public function admin_assets(string $hook): void {
        if (strpos($hook, 'sch-') === false && strpos($hook, 'sch_content_hub') === false) {
            return;
        }

        $css = '
        .sch-card{background:#fff;border:1px solid #dcdcde;padding:20px;max-width:1240px}
        .sch-repeater{display:grid;gap:12px;margin-top:8px}
        .sch-repeater-row{display:grid;grid-template-columns:1fr 1fr auto;gap:10px;align-items:center;padding:12px;background:#f6f7f7;border:1px solid #dcdcde}
        .sch-repeater-row.single{grid-template-columns:1fr auto}
        .sch-grid-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;max-width:1000px}
        .sch-stat{background:#fff;border:1px solid #ddd;padding:18px}
        .sch-stat-label{font-size:14px;color:#666}
        .sch-stat-value{font-size:28px;font-weight:700}
        .sch-actions a{margin-right:8px}
        .sch-log-payload{max-width:560px;white-space:pre-wrap;word-break:break-word;font-family:monospace;font-size:12px}
        .sch-notice{margin:12px 0;padding:10px 12px;background:#fff;border-left:4px solid #72aee6}
        .sch-two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px;align-items:start}
        .sch-inline-form{display:inline}
        .sch-muted{color:#646970}
        .sch-code{font-family:monospace}
        .sch-editorial-content{max-height:320px;overflow:auto;padding:10px;background:#fff;border:1px solid #dcdcde}
        .sch-editorial-content details{margin:0}
        @media (max-width:1100px){.sch-two-col{grid-template-columns:1fr}.sch-grid-stats{grid-template-columns:1fr 1fr}}
        ';
        wp_register_style('sch-admin-inline', false);
        wp_enqueue_style('sch-admin-inline');
        wp_add_inline_style('sch-admin-inline', $css);

        $js = '
        document.addEventListener("click",function(e){
            const addBtn=e.target.closest("[data-sch-add-row]");
            const removeBtn=e.target.closest("[data-sch-remove-row]");
            if(addBtn){
                e.preventDefault();
                const target=document.querySelector(addBtn.getAttribute("data-sch-add-row"));
                if(!target)return;
                const template=target.querySelector("template");
                if(!template)return;
                target.querySelector(".sch-repeater-rows").appendChild(template.content.cloneNode(true));
                return;
            }
            if(removeBtn){
                e.preventDefault();
                const row=removeBtn.closest(".sch-repeater-row");
                if(row)row.remove();
            }
        });
        ';
        wp_register_script('sch-admin-inline', '', [], false, true);
        wp_enqueue_script('sch-admin-inline');
        wp_add_inline_script('sch-admin-inline', $js);
    }

    private function schedule_cron(): void {
        if (!wp_next_scheduled(self::CRON_HOOK)) {
            wp_schedule_event(time() + 60, 'every_minute', self::CRON_HOOK);
        }
    }

    private function schedule_gsc_cron(): void {
        if (!wp_next_scheduled(self::GSC_CRON_HOOK)) {
            wp_schedule_event(time() + HOUR_IN_SECONDS, 'daily', self::GSC_CRON_HOOK);
        }
    }

    private function table(string $name): string {
        return $this->db->prefix . 'sch_' . $name;
    }

    private function maybe_add_column(string $table, string $column, string $sql): void {
        $exists = $this->db->get_var($this->db->prepare(
            "SHOW COLUMNS FROM {$table} LIKE %s",
            $column
        ));

        if (!$exists) {
            $result = $this->db->query($sql);

            if ($result === false) {
                $this->log('error', 'migration', 'Kolom toevoegen mislukt', [
                    'table' => $table,
                    'column' => $column,
                    'sql' => $sql,
                    'db_error' => $this->db->last_error,
                ]);
            } else {
                $this->log('info', 'migration', 'Kolom toegevoegd', [
                    'table' => $table,
                    'column' => $column,
                ]);
            }
        }
    }

    private function create_tables(): void {
        require_once ABSPATH . 'wp-admin/includes/upgrade.php';

        $charset  = $this->db->get_charset_collate();
        $clients  = $this->table('clients');
        $sites    = $this->table('sites');
        $keywords = $this->table('keywords');
        $jobs     = $this->table('jobs');
        $articles = $this->table('articles');
        $anchor_history = $this->table('anchor_history');
        $logs     = $this->table('logs');

        dbDelta("CREATE TABLE {$clients} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(191) NOT NULL,
            website_url VARCHAR(255) NOT NULL DEFAULT '',
            default_anchor VARCHAR(191) NOT NULL DEFAULT '',
            link_targets LONGTEXT NULL,
            research_urls LONGTEXT NULL,
            max_posts_per_month INT UNSIGNED NOT NULL DEFAULT 0,
            gsc_property VARCHAR(255) NOT NULL DEFAULT '',
            gsc_token_data LONGTEXT NULL,
            gsc_token_expires_at DATETIME NULL,
            gsc_connected_email VARCHAR(191) NOT NULL DEFAULT '',
            gsc_last_synced_at DATETIME NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id)
        ) {$charset};");

        dbDelta("CREATE TABLE {$sites} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(191) NOT NULL,
            base_url VARCHAR(255) NOT NULL,
            receiver_secret VARCHAR(255) NOT NULL DEFAULT '',
            default_status VARCHAR(20) NOT NULL DEFAULT 'draft',
            default_category VARCHAR(191) NOT NULL DEFAULT '',
            max_posts_per_day INT UNSIGNED NOT NULL DEFAULT 3,
            publish_priority INT NOT NULL DEFAULT 10,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY base_url (base_url(191))
        ) {$charset};");

        dbDelta("CREATE TABLE {$keywords} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            client_id BIGINT UNSIGNED NOT NULL,
            main_keyword VARCHAR(191) NOT NULL,
            secondary_keywords LONGTEXT NULL,
            target_site_ids LONGTEXT NULL,
            target_site_categories LONGTEXT NULL,
            content_type VARCHAR(50) NOT NULL DEFAULT 'pillar',
            tone_of_voice VARCHAR(100) NOT NULL DEFAULT 'deskundig maar menselijk',
            target_word_count INT UNSIGNED NOT NULL DEFAULT 1200,
            priority INT NOT NULL DEFAULT 10,
            status VARCHAR(50) NOT NULL DEFAULT 'queued',
            source VARCHAR(50) NOT NULL DEFAULT 'manual',
            source_context LONGTEXT NULL,
            last_processed_at DATETIME NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY client_id (client_id),
            KEY status (status),
            KEY source (source)
        ) {$charset};");

        dbDelta("CREATE TABLE {$jobs} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            keyword_id BIGINT UNSIGNED NOT NULL,
            client_id BIGINT UNSIGNED NOT NULL,
            site_id BIGINT UNSIGNED NULL,
            job_type VARCHAR(50) NOT NULL DEFAULT 'write_publish',
            status VARCHAR(50) NOT NULL DEFAULT 'queued',
            attempts INT UNSIGNED NOT NULL DEFAULT 0,
            payload LONGTEXT NULL,
            result LONGTEXT NULL,
            locked_at DATETIME NULL,
            started_at DATETIME NULL,
            finished_at DATETIME NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY keyword_id (keyword_id),
            KEY status (status),
            KEY site_id (site_id)
        ) {$charset};");

        dbDelta("CREATE TABLE {$articles} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            job_id BIGINT UNSIGNED NOT NULL,
            keyword_id BIGINT UNSIGNED NOT NULL,
            client_id BIGINT UNSIGNED NOT NULL,
            site_id BIGINT UNSIGNED NULL,
            article_type VARCHAR(50) NOT NULL DEFAULT 'pillar',
            title VARCHAR(255) NOT NULL,
            slug VARCHAR(255) NOT NULL DEFAULT '',
            content LONGTEXT NOT NULL,
            meta_title VARCHAR(255) NOT NULL DEFAULT '',
            meta_description TEXT NULL,
            canonical_url VARCHAR(255) NOT NULL DEFAULT '',
            source_article_id BIGINT UNSIGNED NULL,
            remote_post_id VARCHAR(100) NOT NULL DEFAULT '',
            remote_url VARCHAR(255) NOT NULL DEFAULT '',
            publish_status VARCHAR(50) NOT NULL DEFAULT 'draft',
            backlinks_data LONGTEXT NULL,
            featured_image_data LONGTEXT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY keyword_id (keyword_id),
            KEY site_id (site_id)
        ) {$charset};");

        dbDelta("CREATE TABLE {$anchor_history} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            client_id BIGINT UNSIGNED NOT NULL,
            keyword_id BIGINT UNSIGNED NULL,
            job_id BIGINT UNSIGNED NULL,
            article_id BIGINT UNSIGNED NULL,
            target_url VARCHAR(255) NOT NULL,
            anchor_text VARCHAR(255) NOT NULL,
            anchor_type VARCHAR(20) NOT NULL DEFAULT 'generic',
            created_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY client_id (client_id),
            KEY target_url (target_url(191)),
            KEY anchor_type (anchor_type),
            KEY created_at (created_at)
        ) {$charset};");

        dbDelta("CREATE TABLE {$logs} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            level VARCHAR(20) NOT NULL DEFAULT 'info',
            context VARCHAR(100) NOT NULL DEFAULT '',
            message TEXT NOT NULL,
            payload LONGTEXT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (id),
            KEY level (level),
            KEY context (context)
        ) {$charset};");

        $this->maybe_add_column($clients, 'research_urls', "ALTER TABLE {$clients} ADD COLUMN research_urls LONGTEXT NULL AFTER link_targets");
        $this->maybe_add_column($clients, 'max_posts_per_month', "ALTER TABLE {$clients} ADD COLUMN max_posts_per_month INT UNSIGNED NOT NULL DEFAULT 0 AFTER research_urls");
        $this->maybe_add_column($clients, 'gsc_property', "ALTER TABLE {$clients} ADD COLUMN gsc_property VARCHAR(255) NOT NULL DEFAULT '' AFTER max_posts_per_month");
        $this->maybe_add_column($clients, 'gsc_token_data', "ALTER TABLE {$clients} ADD COLUMN gsc_token_data LONGTEXT NULL AFTER gsc_property");
        $this->maybe_add_column($clients, 'gsc_token_expires_at', "ALTER TABLE {$clients} ADD COLUMN gsc_token_expires_at DATETIME NULL AFTER gsc_token_data");
        $this->maybe_add_column($clients, 'gsc_connected_email', "ALTER TABLE {$clients} ADD COLUMN gsc_connected_email VARCHAR(191) NOT NULL DEFAULT '' AFTER gsc_token_expires_at");
        $this->maybe_add_column($clients, 'gsc_last_synced_at', "ALTER TABLE {$clients} ADD COLUMN gsc_last_synced_at DATETIME NULL AFTER gsc_connected_email");
        $this->maybe_add_column($sites, 'default_status', "ALTER TABLE {$sites} ADD COLUMN default_status VARCHAR(20) NOT NULL DEFAULT 'draft' AFTER receiver_secret");
        $this->maybe_add_column($sites, 'default_category', "ALTER TABLE {$sites} ADD COLUMN default_category VARCHAR(191) NOT NULL DEFAULT '' AFTER default_status");
        $this->maybe_add_column($sites, 'max_posts_per_day', "ALTER TABLE {$sites} ADD COLUMN max_posts_per_day INT UNSIGNED NOT NULL DEFAULT 3 AFTER default_category");
        $this->maybe_add_column($sites, 'publish_priority', "ALTER TABLE {$sites} ADD COLUMN publish_priority INT NOT NULL DEFAULT 10 AFTER max_posts_per_day");
        $this->maybe_add_column($sites, 'is_active', "ALTER TABLE {$sites} ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1 AFTER publish_priority");
        $this->maybe_add_column($keywords, 'source', "ALTER TABLE {$keywords} ADD COLUMN source VARCHAR(50) NOT NULL DEFAULT 'manual' AFTER status");
        $this->maybe_add_column($keywords, 'source_context', "ALTER TABLE {$keywords} ADD COLUMN source_context LONGTEXT NULL AFTER source");
        $this->maybe_add_column($keywords, 'target_site_categories', "ALTER TABLE {$keywords} ADD COLUMN target_site_categories LONGTEXT NULL AFTER target_site_ids");
        $this->maybe_add_column($jobs, 'attempts', "ALTER TABLE {$jobs} ADD COLUMN attempts INT UNSIGNED NOT NULL DEFAULT 0 AFTER status");
        $this->maybe_add_column($articles, 'backlinks_data', "ALTER TABLE {$articles} ADD COLUMN backlinks_data LONGTEXT NULL AFTER publish_status");
        $this->maybe_add_column($articles, 'featured_image_data', "ALTER TABLE {$articles} ADD COLUMN featured_image_data LONGTEXT NULL AFTER publish_status");

        add_option(self::OPTION_OPENAI_MODEL, 'gpt-5.4-mini');
        add_option(self::OPTION_OPENAI_TEMPERATURE, '0.6');
        add_option(self::OPTION_ENABLE_FEATURED_IMAGES, '1');
        add_option(self::OPTION_ENABLE_SUPPORTING, '1');
        add_option(self::OPTION_ENABLE_AUTO_DISCOVERY, '0');
        add_option(self::OPTION_MAX_RESEARCH_PAGES, '5');
        add_option(self::OPTION_MAX_DISCOVERY_KEYWORDS, '10');
        add_option(self::OPTION_ENABLE_VERBOSE_LOGS, '1');
        add_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, 'https://shortcut.nl');
        add_option(self::OPTION_RANDOM_MACHINE_ENABLED, '0');
        add_option(self::OPTION_RANDOM_DAILY_MAX, '10');
        add_option(self::OPTION_RANDOM_STATUS, 'draft');
        add_option(self::OPTION_RANDOM_MIN_WORDS, '900');
        add_option(self::OPTION_RANDOM_MAX_WORDS, '1400');
        add_option(self::OPTION_RANDOM_MAX_PER_SITE_PER_DAY, '2');
        add_option(self::OPTION_RANDOM_ONLY_ACTIVE_SITES, '1');
        add_option(self::OPTION_RANDOM_ALLOWED_CATEGORIES, wp_json_encode([]));
        add_option(self::OPTION_RANDOM_DUPLICATE_WINDOW_DAYS, '30');
        add_option(self::OPTION_GSC_ENABLED, '0');
        add_option(self::OPTION_GSC_CLIENT_ID, '');
        add_option(self::OPTION_GSC_CLIENT_SECRET, '');
        add_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28');
        add_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, '250');
        add_option(self::OPTION_GSC_AUTO_SYNC, '0');

        update_option(self::OPTION_DB_VERSION, self::DB_VERSION);
    }

    public function admin_menu(): void {
        add_menu_page('Content Hub', 'Content Hub', 'manage_options', 'sch-content-hub', [$this, 'render_dashboard'], 'dashicons-admin-site-alt3', 56);
        add_submenu_page('sch-content-hub', 'Dashboard', 'Dashboard', 'manage_options', 'sch-content-hub', [$this, 'render_dashboard']);
        add_submenu_page('sch-content-hub', 'Klanten', 'Klanten', 'manage_options', 'sch-clients', [$this, 'render_clients']);
        add_submenu_page('sch-content-hub', 'Blogs', 'Blogs', 'manage_options', 'sch-sites', [$this, 'render_sites']);
        add_submenu_page('sch-content-hub', 'Keywords', 'Keywords', 'manage_options', 'sch-keywords', [$this, 'render_keywords']);
        add_submenu_page('sch-content-hub', 'Jobs', 'Jobs', 'manage_options', 'sch-jobs', [$this, 'render_jobs']);
        add_submenu_page('sch-content-hub', 'Conflicten', 'Conflicten', 'manage_options', 'sch-conflicts', [$this, 'render_conflicts']);
        add_submenu_page('sch-content-hub', 'Redactie', 'Redactie', 'manage_options', 'sch-editorial', [$this, 'render_editorial']);
        add_submenu_page('sch-content-hub', 'Rapportage', 'Rapportage', 'manage_options', 'sch-reporting', [$this, 'render_reporting']);
        add_submenu_page('sch-content-hub', 'Logs', 'Logs', 'manage_options', 'sch-logs', [$this, 'render_logs']);
        add_submenu_page('sch-content-hub', 'Instellingen', 'Instellingen', 'manage_options', 'sch-settings', [$this, 'render_settings']);
    }

    private function render_admin_notice(): void {
        $message = isset($_GET['sch_message']) ? sanitize_text_field(wp_unslash($_GET['sch_message'])) : '';
        $message_type = isset($_GET['sch_message_type']) ? sanitize_key(wp_unslash($_GET['sch_message_type'])) : 'success';
        $notice_class = $message_type === 'error' ? 'notice notice-error' : 'notice notice-success';

        if ($message !== '') {
            echo '<div class="' . esc_attr($notice_class) . '"><p>' . esc_html($message) . '</p></div>';
        }
    }

    public function render_dashboard(): void {
        $queued  = (int) $this->db->get_var("SELECT COUNT(*) FROM {$this->table('jobs')} WHERE status='queued'");
        $running = (int) $this->db->get_var("SELECT COUNT(*) FROM {$this->table('jobs')} WHERE status='running'");
        $ready_for_review = (int) $this->db->get_var("SELECT COUNT(*) FROM {$this->table('jobs')} WHERE status='awaiting_approval'");
        $done    = (int) $this->db->get_var("SELECT COUNT(*) FROM {$this->table('jobs')} WHERE status='published'");
        $failed  = (int) $this->db->get_var("SELECT COUNT(*) FROM {$this->table('jobs')} WHERE status='failed'");
        ?>
        <div class="wrap">
            <h1>Content Hub</h1>
            <?php $this->render_admin_notice(); ?>
            <div class="sch-grid-stats">
                <?php foreach (['Queued jobs' => $queued, 'Running jobs' => $running, 'Wachten op redactie' => $ready_for_review, 'Published jobs' => $done, 'Failed jobs' => $failed] as $label => $value) : ?>
                    <div class="sch-stat">
                        <div class="sch-stat-label"><?php echo esc_html($label); ?></div>
                        <div class="sch-stat-value"><?php echo (int) $value; ?></div>
                    </div>
                <?php endforeach; ?>
            </div>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" style="margin-top:20px;">
                <?php wp_nonce_field('sch_run_now'); ?>
                <input type="hidden" name="action" value="sch_run_now">
                <button class="button button-primary">Worker nu draaien</button>
            </form>
            <p style="margin-top:20px;">Gebruik daarnaast een echte server cron die iedere minuut wp-cron.php aanroept.</p>
        </div>
        <?php
    }

    public function render_clients(): void {
        $edit_id = isset($_GET['edit']) ? (int) $_GET['edit'] : 0;
        $edit = $edit_id ? $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", $edit_id)) : null;
        $targets = $edit ? $this->get_client_link_targets($edit) : [];
        $research_urls = $edit ? $this->get_client_research_urls($edit) : [];
        if (!$targets) {
            $targets = [['url' => '', 'anchor' => '']];
        }
        if (!$research_urls) {
            $research_urls = [['url' => '']];
        }
        $rows = $this->db->get_results("SELECT * FROM {$this->table('clients')} ORDER BY id DESC");
        ?>
        <div class="wrap">
            <h1>Klanten</h1>
            <?php $this->render_admin_notice(); ?>

            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card">
                <?php wp_nonce_field('sch_save_client'); ?>
                <input type="hidden" name="action" value="sch_save_client">
                <input type="hidden" name="id" value="<?php echo (int) ($edit->id ?? 0); ?>">

                <table class="form-table">
                    <tr><th>Naam</th><td><input type="text" name="name" class="regular-text" required value="<?php echo esc_attr($edit->name ?? ''); ?>"></td></tr>
                    <tr><th>Website URL</th><td><input type="url" name="website_url" class="regular-text" value="<?php echo esc_attr($edit->website_url ?? ''); ?>"></td></tr>
                    <tr><th>Default anchor</th><td><input type="text" name="default_anchor" class="regular-text" value="<?php echo esc_attr($edit->default_anchor ?? ''); ?>"></td></tr>
                    <tr>
                        <th>Max blogs per maand</th>
                        <td>
                            <input type="number" name="max_posts_per_month" value="<?php echo esc_attr((string) ($edit->max_posts_per_month ?? 0)); ?>" min="0">
                            <p class="description">0 = onbeperkt. Bij een waarde &gt; 0 worden jobs verdeeld over de maand op basis van dag/maand-ratio.</p>
                        </td>
                    </tr>

                    <tr>
                        <th>Link targets</th>
                        <td>
                            <div id="sch-link-targets" class="sch-repeater">
                                <div class="sch-repeater-rows">
                                    <?php foreach ($targets as $target) : ?>
                                        <div class="sch-repeater-row">
                                            <input type="url" name="link_target_url[]" placeholder="https://klantsite.nl/pagina" value="<?php echo esc_attr($target['url'] ?? ''); ?>">
                                            <input type="text" name="link_target_anchor[]" placeholder="Anchor tekst" value="<?php echo esc_attr($target['anchor'] ?? ''); ?>">
                                            <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                        </div>
                                    <?php endforeach; ?>
                                </div>
                                <template>
                                    <div class="sch-repeater-row">
                                        <input type="url" name="link_target_url[]" placeholder="https://klantsite.nl/pagina">
                                        <input type="text" name="link_target_anchor[]" placeholder="Anchor tekst">
                                        <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                    </div>
                                </template>
                            </div>
                            <p><button class="button" type="button" data-sch-add-row="#sch-link-targets">Link target toevoegen</button></p>
                        </td>
                    </tr>

                    <tr>
                        <th>Research URLs</th>
                        <td>
                            <div id="sch-research-urls" class="sch-repeater">
                                <div class="sch-repeater-rows">
                                    <?php foreach ($research_urls as $research) : ?>
                                        <div class="sch-repeater-row single">
                                            <input type="url" name="research_url[]" placeholder="https://klantsite.nl/diensten" value="<?php echo esc_attr($research['url'] ?? ''); ?>">
                                            <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                        </div>
                                    <?php endforeach; ?>
                                </div>
                                <template>
                                    <div class="sch-repeater-row single">
                                        <input type="url" name="research_url[]" placeholder="https://klantsite.nl/diensten">
                                        <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                    </div>
                                </template>
                            </div>
                            <p><button class="button" type="button" data-sch-add-row="#sch-research-urls">Research URL toevoegen</button></p>
                            <p class="sch-muted">Deze pagina’s worden via GET opgehaald voor keyword discovery en diepere contentaansturing.</p>
                        </td>
                    </tr>
                    <tr>
                        <th>Google Search Console</th>
                        <td>
                            <?php if ($edit && $this->is_gsc_integration_enabled()) : ?>
                                <p>
                                    Status:
                                    <?php if ($this->client_has_gsc_connection($edit)) : ?>
                                        <strong>Verbonden</strong>
                                        <?php if (!empty($edit->gsc_connected_email)) : ?>
                                            (<?php echo esc_html((string) $edit->gsc_connected_email); ?>)
                                        <?php endif; ?>
                                    <?php else : ?>
                                        <strong>Niet verbonden</strong>
                                    <?php endif; ?>
                                </p>
                                <p>Property: <code><?php echo esc_html((string) ($edit->gsc_property ?: 'Nog niet geselecteerd')); ?></code></p>
                                <p>Laatste sync: <?php echo esc_html((string) ($edit->gsc_last_synced_at ?: 'Nog nooit')); ?></p>
                            <?php elseif (!$edit) : ?>
                                <p class="description">Sla de klant eerst op om Search Console te koppelen.</p>
                            <?php else : ?>
                                <p class="description">Schakel eerst Google Search Console in bij Instellingen.</p>
                            <?php endif; ?>
                        </td>
                    </tr>
                </table>

                <p><button class="button button-primary"><?php echo $edit ? 'Klant bijwerken' : 'Klant opslaan'; ?></button></p>
            </form>

            <?php if ($edit && $this->is_gsc_integration_enabled()) : ?>
                <?php $properties_cache = $this->get_cached_gsc_properties_for_user((int) $edit->id); ?>
                <div class="sch-card" style="margin-top:20px;">
                    <h2 style="margin-top:0;">Google Search Console koppeling</h2>
                    <p>Klant: <strong><?php echo esc_html((string) $edit->name); ?></strong></p>
                    <p>
                        <a class="button button-secondary" href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_gsc_connect&client_id=' . (int) $edit->id), 'sch_gsc_connect_' . (int) $edit->id)); ?>">Connect Google Search Console</a>
                        <?php if ($this->client_has_gsc_connection($edit)) : ?>
                            <a class="button" href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_gsc_disconnect&client_id=' . (int) $edit->id), 'sch_gsc_disconnect_' . (int) $edit->id)); ?>" onclick="return confirm('Koppeling verbreken?');">Disconnect</a>
                        <?php endif; ?>
                    </p>

                    <?php if ($this->client_has_gsc_connection($edit)) : ?>
                        <p>
                            <a class="button" href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_gsc_fetch_properties&client_id=' . (int) $edit->id), 'sch_gsc_fetch_properties_' . (int) $edit->id)); ?>">Fetch properties</a>
                        </p>
                    <?php else : ?>
                        <p class="description">Rond eerst de Google Search Console koppeling af voordat je properties ophaalt.</p>
                    <?php endif; ?>

                    <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" style="margin-top:15px;">
                        <?php wp_nonce_field('sch_gsc_save_property'); ?>
                        <input type="hidden" name="action" value="sch_gsc_save_property">
                        <input type="hidden" name="client_id" value="<?php echo (int) $edit->id; ?>">
                        <label for="sch-gsc-property"><strong>Selecteer property</strong></label><br>
                        <select id="sch-gsc-property" name="gsc_property" style="min-width:360px;">
                            <option value="">-- kies property --</option>
                            <?php foreach ($properties_cache as $property) : ?>
                                <option value="<?php echo esc_attr($property); ?>" <?php selected((string) $edit->gsc_property, (string) $property); ?>><?php echo esc_html($property); ?></option>
                            <?php endforeach; ?>
                        </select>
                        <button class="button button-primary" type="submit">Property opslaan</button>
                    </form>

                    <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" style="margin-top:15px;">
                        <?php wp_nonce_field('sch_gsc_sync_keywords'); ?>
                        <input type="hidden" name="action" value="sch_gsc_sync_keywords">
                        <input type="hidden" name="client_id" value="<?php echo (int) $edit->id; ?>">
                        <label><strong>Periode</strong></label>
                        <select name="range_days">
                            <?php foreach ([7, 28, 90] as $range_days) : ?>
                                <option value="<?php echo (int) $range_days; ?>" <?php selected((string) $range_days, (string) get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28')); ?>>Laatste <?php echo (int) $range_days; ?> dagen</option>
                            <?php endforeach; ?>
                        </select>
                        <label style="margin-left:12px;"><strong>Row limit</strong></label>
                        <input type="number" name="row_limit" min="1" max="25000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, '250')); ?>">
                        <label style="margin-left:12px;"><strong>Top N op clicks</strong></label>
                        <input type="number" name="top_n_clicks" min="0" max="25000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_TOP_N_CLICKS, '0')); ?>">
                        <label style="margin-left:12px;"><strong>Min. impressions</strong></label>
                        <input type="number" name="min_impressions" min="0" max="100000000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_MIN_IMPRESSIONS, '0')); ?>">
                        <button class="button button-primary" type="submit">Sync keywords</button>
                    </form>
                </div>
            <?php endif; ?>

            <h2 style="margin-top:30px;">Bestaande klanten</h2>
            <table class="widefat striped">
                <thead><tr><th>ID</th><th>Naam</th><th>Website</th><th>Max/maand</th><th>Research URLs</th><th>GSC property</th><th>Links</th><th>Acties</th></tr></thead>
                <tbody>
                <?php if ($rows) : foreach ($rows as $row) : ?>
                    <?php $client_research = $this->get_client_research_urls($row); ?>
                    <tr>
                        <td><?php echo (int) $row->id; ?></td>
                        <td><?php echo esc_html($row->name); ?></td>
                        <td><?php echo esc_html($row->website_url); ?></td>
                        <td><?php echo (int) ($row->max_posts_per_month ?? 0); ?></td>
                        <td><?php echo esc_html(implode(' | ', array_map(static function ($v) { return (string) ($v['url'] ?? ''); }, $client_research))); ?></td>
                        <td><code><?php echo esc_html((string) ($row->gsc_property ?: '-')); ?></code></td>
                        <td><?php echo esc_html($this->implode_target_strings($this->get_client_link_targets($row))); ?></td>
                        <td class="sch-actions">
                            <a href="<?php echo esc_url(admin_url('admin.php?page=sch-clients&edit=' . (int) $row->id)); ?>">Bewerken</a>
                            <a href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_delete_client&id=' . (int) $row->id), 'sch_delete_client')); ?>" onclick="return confirm('Zeker weten?');">Verwijderen</a>

                            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-inline-form">
                                <?php wp_nonce_field('sch_discover_keywords'); ?>
                                <input type="hidden" name="action" value="sch_discover_keywords">
                                <input type="hidden" name="client_id" value="<?php echo (int) $row->id; ?>">
                                <button class="button button-secondary" type="submit">Keywords ontdekken</button>
                            </form>
                        </td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="8">Nog geen klanten.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
        <?php
    }

    public function render_sites(): void {
        $edit_id = isset($_GET['edit']) ? (int) $_GET['edit'] : 0;
        $edit = $edit_id ? $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('sites')} WHERE id=%d", $edit_id)) : null;
        $rows = $this->db->get_results("SELECT * FROM {$this->table('sites')} ORDER BY publish_priority ASC, id DESC");
        $bulk_result = get_transient('sch_bulk_sites_result_' . get_current_user_id());
        if ($bulk_result) {
            delete_transient('sch_bulk_sites_result_' . get_current_user_id());
        }
        ?>
        <div class="wrap">
            <h1>Blogs</h1>
            <?php $this->render_admin_notice(); ?>

            <?php if (!empty($bulk_result)) : ?>
                <div class="sch-notice">
                    <strong>Bulk import afgerond.</strong><br>
                    Toegevoegd: <?php echo (int) ($bulk_result['created'] ?? 0); ?> |
                    Bijgewerkt: <?php echo (int) ($bulk_result['updated'] ?? 0); ?> |
                    Overgeslagen: <?php echo (int) ($bulk_result['skipped'] ?? 0); ?>
                    <?php if (!empty($bulk_result['messages'])) : ?>
                        <div style="margin-top:8px;">
                            <?php foreach ((array) $bulk_result['messages'] as $message) : ?>
                                <div><?php echo esc_html($message); ?></div>
                            <?php endforeach; ?>
                        </div>
                    <?php endif; ?>
                </div>
            <?php endif; ?>

            <div class="sch-two-col">
                <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card">
                    <h2 style="margin-top:0;"><?php echo $edit ? 'Blog bewerken' : 'Enkele blog toevoegen'; ?></h2>
                    <?php wp_nonce_field('sch_save_site'); ?>
                    <input type="hidden" name="action" value="sch_save_site">
                    <input type="hidden" name="id" value="<?php echo (int) ($edit->id ?? 0); ?>">
                    <table class="form-table">
                        <tr><th>Naam</th><td><input type="text" name="name" class="regular-text" required value="<?php echo esc_attr($edit->name ?? ''); ?>"></td></tr>
                        <tr><th>Base URL</th><td><input type="url" name="base_url" class="regular-text" required value="<?php echo esc_attr($edit->base_url ?? ''); ?>"></td></tr>
                        <tr><th>Default status</th><td><select name="default_status"><option value="draft" <?php selected(($edit->default_status ?? ''), 'draft'); ?>>draft</option><option value="publish" <?php selected(($edit->default_status ?? ''), 'publish'); ?>>publish</option></select></td></tr>
                        <tr>
                            <th>Default category</th>
                            <td>
                                <select name="default_category">
                                    <option value="">Automatisch bepalen</option>
                                    <?php foreach ($this->allowed_blog_categories() as $category) : ?>
                                        <option value="<?php echo esc_attr($category); ?>" <?php selected(($edit->default_category ?? ''), $category); ?>><?php echo esc_html($category); ?></option>
                                    <?php endforeach; ?>
                                </select>
                            </td>
                        </tr>
                        <tr><th>Max posts per dag</th><td><input type="number" name="max_posts_per_day" value="<?php echo esc_attr($edit->max_posts_per_day ?? 3); ?>" min="1"></td></tr>
                        <tr><th>Prioriteit</th><td><input type="number" name="publish_priority" value="<?php echo esc_attr($edit->publish_priority ?? 10); ?>"></td></tr>
                    </table>
                    <p class="sch-muted">Authenticatie loopt via het trusted source domein uit de instellingen. Per blog hoeft geen secret meer ingesteld te worden.</p>
                    <p><button class="button button-primary"><?php echo $edit ? 'Blog bijwerken' : 'Blog opslaan'; ?></button></p>
                </form>

                <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card">
                    <h2 style="margin-top:0;">Bulk blogs toevoegen</h2>
                    <?php wp_nonce_field('sch_bulk_save_sites'); ?>
                    <input type="hidden" name="action" value="sch_bulk_save_sites">

                    <p>Plak per regel een blog. Ondersteunde formaten:</p>
                    <pre style="white-space:pre-wrap;">Naam | https://site.nl
Naam | https://site.nl | publish | nieuws | 3 | 10
Naam,https://site.nl,publish,nieuws,3,10
https://site.nl
https://site.nl | publish | nieuws | 3 | 10

Legacy regels met een secret als extra veld worden ook nog gelezen, maar dat veld wordt genegeerd.</pre>

                    <table class="form-table">
                        <tr>
                            <th>Bulk invoer</th>
                            <td>
                                <textarea name="bulk_sites" rows="14" class="large-text code" placeholder="Blog A | https://bloga.nl&#10;Blog B | https://blogb.nl | publish | nieuws | 5 | 20"></textarea>
                            </td>
                        </tr>
                        <tr>
                            <th>Fallback status</th>
                            <td>
                                <select name="bulk_default_status">
                                    <option value="draft">draft</option>
                                    <option value="publish">publish</option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <th>Fallback category</th>
                            <td>
                                <select name="bulk_default_category">
                                    <option value="">Automatisch bepalen</option>
                                    <?php foreach ($this->allowed_blog_categories() as $category) : ?>
                                        <option value="<?php echo esc_attr($category); ?>"><?php echo esc_html($category); ?></option>
                                    <?php endforeach; ?>
                                </select>
                            </td>
                        </tr>
                        <tr><th>Fallback max posts per dag</th><td><input type="number" name="bulk_max_posts_per_day" value="3" min="1"></td></tr>
                        <tr><th>Fallback prioriteit</th><td><input type="number" name="bulk_publish_priority" value="10"></td></tr>
                        <tr>
                            <th>Bestaande URL</th>
                            <td><label><input type="checkbox" name="bulk_update_existing" value="1" checked> Bestaande blog bijwerken als URL al bestaat</label></td>
                        </tr>
                    </table>

                    <p><button class="button button-primary">Bulk blogs verwerken</button></p>
                </form>
            </div>

            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card" style="margin-top:20px;max-width:620px;">
                <h2 style="margin-top:0;">Bestaande blogs in bulk bijwerken</h2>
                <?php wp_nonce_field('sch_bulk_update_sites_status'); ?>
                <input type="hidden" name="action" value="sch_bulk_update_sites_status">
                <p>Pas in één keer de default status van alle bestaande blogs aan.</p>
                <table class="form-table">
                    <tr>
                        <th>Nieuwe status</th>
                        <td>
                            <select name="default_status">
                                <option value="draft">draft</option>
                                <option value="publish">publish</option>
                            </select>
                        </td>
                    </tr>
                </table>
                <p><button class="button">Status voor alle blogs bijwerken</button></p>
            </form>

            <h2 style="margin-top:30px;">Bestaande blogs</h2>
            <table class="widefat striped">
                <thead><tr><th>ID</th><th>Naam</th><th>URL</th><th>Status</th><th>Categorie</th><th>Max/dag</th><th>Prio</th><th>Acties</th></tr></thead>
                <tbody>
                <?php if ($rows) : foreach ($rows as $row) : ?>
                    <tr>
                        <td><?php echo (int) $row->id; ?></td>
                        <td><?php echo esc_html($row->name); ?></td>
                        <td><?php echo esc_html($row->base_url); ?></td>
                        <td><?php echo esc_html($row->default_status); ?></td>
                        <td><?php echo esc_html($row->default_category); ?></td>
                        <td><?php echo (int) $row->max_posts_per_day; ?></td>
                        <td><?php echo (int) $row->publish_priority; ?></td>
                        <td class="sch-actions">
                            <a href="<?php echo esc_url(admin_url('admin.php?page=sch-sites&edit=' . (int) $row->id)); ?>">Bewerken</a>
                            <a href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_delete_site&id=' . (int) $row->id), 'sch_delete_site')); ?>" onclick="return confirm('Zeker weten?');">Verwijderen</a>
                        </td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="8">Nog geen blogs.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
        <?php
    }

    public function render_keywords(): void {
        $clients = $this->db->get_results("SELECT id, name FROM {$this->table('clients')} WHERE is_active=1 ORDER BY name ASC");
        $sites   = $this->db->get_results("SELECT id, name, default_category FROM {$this->table('sites')} WHERE is_active=1 ORDER BY publish_priority ASC, name ASC");
        $edit_id = isset($_GET['edit']) ? (int) $_GET['edit'] : 0;
        $edit = $edit_id ? $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('keywords')} WHERE id=%d", $edit_id)) : null;
        $secondary = $edit ? $this->get_secondary_keywords_list($edit) : [''];
        if (!$secondary) {
            $secondary = [''];
        }
        $selected_site_ids = $edit ? $this->decode_json_array($edit->target_site_ids) : [];
        $selected_site_categories = $edit ? $this->decode_json_array($edit->target_site_categories) : [];
        $rows = $this->db->get_results("SELECT k.*, c.name AS client_name FROM {$this->table('keywords')} k LEFT JOIN {$this->table('clients')} c ON c.id = k.client_id ORDER BY k.id DESC");
        ?>
        <div class="wrap">
            <h1>Keywords</h1>
            <?php $this->render_admin_notice(); ?>

            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card">
                <?php wp_nonce_field('sch_save_keyword'); ?>
                <input type="hidden" name="action" value="sch_save_keyword">
                <input type="hidden" name="id" value="<?php echo (int) ($edit->id ?? 0); ?>">
                <table class="form-table">
                    <tr><th>Klant</th><td><select name="client_id" required><option value="">Kies klant</option><?php foreach ($clients as $client) : ?><option value="<?php echo (int) $client->id; ?>" <?php selected((int) ($edit->client_id ?? 0), (int) $client->id); ?>><?php echo esc_html($client->name); ?></option><?php endforeach; ?></select></td></tr>
                    <tr><th>Hoofdkeyword</th><td><input type="text" name="main_keyword" class="regular-text" required value="<?php echo esc_attr($edit->main_keyword ?? ''); ?>"></td></tr>
                    <tr><th>Secondary keywords</th><td>
                        <div id="sch-secondary-keywords" class="sch-repeater">
                            <div class="sch-repeater-rows">
                                <?php foreach ($secondary as $item) : ?>
                                    <div class="sch-repeater-row single">
                                        <input type="text" name="secondary_keywords[]" value="<?php echo esc_attr(is_array($item) ? ($item['keyword'] ?? '') : $item); ?>" placeholder="Bijvoorbeeld: kunststof kozijnen prijs">
                                        <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                            <template>
                                <div class="sch-repeater-row single">
                                    <input type="text" name="secondary_keywords[]" placeholder="Bijvoorbeeld: kunststof kozijnen prijs">
                                    <button class="button" type="button" data-sch-remove-row>Verwijderen</button>
                                </div>
                            </template>
                        </div>
                        <p><button class="button" type="button" data-sch-add-row="#sch-secondary-keywords">Secondary keyword toevoegen</button></p>
                    </td></tr>
                    <tr><th>Type</th><td><select name="content_type"><option value="pillar" <?php selected(($edit->content_type ?? ''), 'pillar'); ?>>pillar</option><option value="supporting" <?php selected(($edit->content_type ?? ''), 'supporting'); ?>>supporting</option></select></td></tr>
                    <tr><th>Tone of voice</th><td><input type="text" name="tone_of_voice" value="<?php echo esc_attr($edit->tone_of_voice ?? 'deskundig maar menselijk'); ?>" class="regular-text"></td></tr>
                    <tr><th>Woordaantal</th><td><input type="number" name="target_word_count" value="<?php echo esc_attr($edit->target_word_count ?? 1200); ?>" min="300"></td></tr>
                    <tr><th>Prioriteit</th><td><input type="number" name="priority" value="<?php echo esc_attr($edit->priority ?? 10); ?>"></td></tr>
                    <tr>
                        <th>Filter op blog-categorie</th>
                        <td>
                            <?php foreach ($this->allowed_blog_categories() as $category) : ?>
                                <label style="display:block;margin-bottom:6px;">
                                    <input type="checkbox" name="target_site_categories[]" value="<?php echo esc_attr($category); ?>" <?php checked(in_array($category, $selected_site_categories, true)); ?>>
                                    <?php echo esc_html($category); ?>
                                </label>
                            <?php endforeach; ?>
                            <p class="description">Optioneel: als je categorieën selecteert, worden alleen jobs aangemaakt voor blogs in deze categorieën.</p>
                        </td>
                    </tr>
                    <tr><th>Doelblogs</th><td><?php foreach ($sites as $site) : ?><label style="display:block;margin-bottom:6px;"><input type="checkbox" name="target_site_ids[]" value="<?php echo (int) $site->id; ?>" <?php checked(in_array((int) $site->id, array_map('intval', $selected_site_ids), true)); ?>> <?php echo esc_html($site->name); ?> <span class="sch-muted">(<?php echo esc_html($site->default_category ?: 'onbekend'); ?>)</span></label><?php endforeach; ?></td></tr>
                </table>
                <p><button class="button button-primary"><?php echo $edit ? 'Keyword bijwerken' : 'Keyword opslaan'; ?></button></p>
            </form>

            <h2 style="margin-top:30px;">Bestaande keywords</h2>
            <table class="widefat striped">
                <thead><tr><th>ID</th><th>Klant</th><th>Keyword</th><th>Type</th><th>Bron</th><th>Impressions</th><th>Clicks</th><th>CTR</th><th>Positie</th><th>Status</th><th>Acties</th></tr></thead>
                <tbody>
                <?php if ($rows) : foreach ($rows as $row) : ?>
                    <?php
                    $metrics = ['impressions' => null, 'clicks' => null, 'ctr' => null, 'position' => null];
                    $context = json_decode((string) ($row->source_context ?? ''), true);
                    if (is_array($context)) {
                        $metrics['impressions'] = isset($context['impressions']) ? (float) $context['impressions'] : null;
                        $metrics['clicks'] = isset($context['clicks']) ? (float) $context['clicks'] : null;
                        $metrics['ctr'] = isset($context['ctr']) ? (float) $context['ctr'] : null;
                        $metrics['position'] = isset($context['position']) ? (float) $context['position'] : null;
                    }
                    ?>
                    <tr>
                        <td><?php echo (int) $row->id; ?></td>
                        <td><?php echo esc_html($row->client_name ?: ''); ?></td>
                        <td><?php echo esc_html($row->main_keyword); ?></td>
                        <td><?php echo esc_html($row->content_type); ?></td>
                        <td><?php echo esc_html($row->source ?: 'manual'); ?></td>
                        <td><?php echo $metrics['impressions'] === null ? '&mdash;' : esc_html(number_format_i18n($metrics['impressions'], 0)); ?></td>
                        <td><?php echo $metrics['clicks'] === null ? '&mdash;' : esc_html(number_format_i18n($metrics['clicks'], 0)); ?></td>
                        <td><?php echo $metrics['ctr'] === null ? '&mdash;' : esc_html(number_format_i18n($metrics['ctr'] * 100, 2) . '%'); ?></td>
                        <td><?php echo $metrics['position'] === null ? '&mdash;' : esc_html(number_format_i18n($metrics['position'], 2)); ?></td>
                        <td><?php echo esc_html($row->status); ?></td>
                        <td class="sch-actions">
                            <a href="<?php echo esc_url(admin_url('admin.php?page=sch-keywords&edit=' . (int) $row->id)); ?>">Bewerken</a>
                            <a href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_delete_keyword&id=' . (int) $row->id), 'sch_delete_keyword')); ?>" onclick="return confirm('Zeker weten?');">Verwijderen</a>
                        </td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="11">Nog geen keywords.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
        <?php
    }

    public function render_jobs(): void {
        $rows = $this->db->get_results("SELECT j.*, k.main_keyword, s.name AS site_name FROM {$this->table('jobs')} j LEFT JOIN {$this->table('keywords')} k ON k.id = j.keyword_id LEFT JOIN {$this->table('sites')} s ON s.id = j.site_id ORDER BY j.id DESC LIMIT 200");
        ?>
        <div class="wrap">
            <h1>Jobs</h1>
            <?php $this->render_admin_notice(); ?>
            <table class="widefat striped">
                <thead><tr><th>ID</th><th>Keyword</th><th>Blog</th><th>Type</th><th>Status</th><th>Pogingen</th><th>Gestart</th><th>Klaar</th><th>Actie</th></tr></thead>
                <tbody>
                <?php if ($rows) : foreach ($rows as $row) : ?>
                    <tr>
                        <td><?php echo (int) $row->id; ?></td>
                        <td><?php echo esc_html($row->main_keyword ?: ''); ?></td>
                        <td><?php echo esc_html($row->site_name ?: ''); ?></td>
                        <td><?php echo esc_html($row->job_type); ?></td>
                        <td><?php echo esc_html($row->status); ?></td>
                        <td><?php echo (int) $row->attempts; ?></td>
                        <td><?php echo esc_html($row->started_at ?: ''); ?></td>
                        <td><?php echo esc_html($row->finished_at ?: ''); ?></td>
                        <td>
                            <?php if ($row->status === 'awaiting_approval') : ?>
                                <a href="<?php echo esc_url(admin_url('admin.php?page=sch-editorial')); ?>">Naar redactie</a>
                            <?php elseif (in_array($row->status, ['failed', 'published'], true)) : ?>
                                <a href="<?php echo esc_url(wp_nonce_url(admin_url('admin-post.php?action=sch_retry_job&id=' . (int) $row->id), 'sch_retry_job')); ?>">Retry</a>
                            <?php endif; ?>
                        </td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="9">Nog geen jobs.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
        <?php
    }

    public function render_conflicts(): void {
        $rows = $this->db->get_results("
            SELECT
                j.id,
                j.client_id,
                j.keyword_id,
                j.site_id,
                j.payload,
                j.created_at,
                c.name AS client_name,
                k.main_keyword,
                s.name AS site_name
            FROM {$this->table('jobs')} j
            LEFT JOIN {$this->table('clients')} c ON c.id = j.client_id
            LEFT JOIN {$this->table('keywords')} k ON k.id = j.keyword_id
            LEFT JOIN {$this->table('sites')} s ON s.id = j.site_id
            WHERE j.status = 'blocked_cannibalization'
            ORDER BY c.name ASC, j.created_at DESC, j.id DESC
            LIMIT 500
        ");

        $grouped = [];
        foreach ($rows as $row) {
            $client_name = (string) ($row->client_name ?: 'Onbekende klant');
            if (!isset($grouped[$client_name])) {
                $grouped[$client_name] = [];
            }
            $grouped[$client_name][] = $row;
        }
        ?>
        <div class="wrap">
            <h1>Cannibalisatie-conflicten</h1>
            <?php $this->render_admin_notice(); ?>

            <?php if (!$grouped) : ?>
                <p>Geen conflicten gevonden.</p>
            <?php else : ?>
                <?php foreach ($grouped as $client_name => $client_rows) : ?>
                    <h2><?php echo esc_html($client_name); ?> (<?php echo (int) count($client_rows); ?>)</h2>
                    <table class="widefat striped" style="margin-bottom:24px;">
                        <thead>
                        <tr><th>Job ID</th><th>Nieuw keyword</th><th>Blog</th><th>Conflict met</th><th>Suggestie</th><th>Aangemaakt</th></tr>
                        </thead>
                        <tbody>
                        <?php foreach ($client_rows as $row) : ?>
                            <?php
                            $payload = json_decode((string) $row->payload, true);
                            $payload = is_array($payload) ? $payload : [];
                            $conflicts = isset($payload['conflicts']) && is_array($payload['conflicts']) ? $payload['conflicts'] : [];
                            $suggestion = sanitize_text_field((string) ($payload['suggestion'] ?? 'Consolideer met bestaand artikel of voeg interne link toe.'));
                            $conflict_labels = [];
                            foreach ($conflicts as $conflict) {
                                $existing_keyword = sanitize_text_field((string) ($conflict['existing_keyword'] ?? ''));
                                $article_title = sanitize_text_field((string) ($conflict['article_title'] ?? ''));
                                $matched_terms = isset($conflict['matched_terms']) && is_array($conflict['matched_terms']) ? array_map('sanitize_text_field', $conflict['matched_terms']) : [];
                                $parts = array_filter([$existing_keyword, $article_title], static fn ($value): bool => $value !== '');
                                $line = implode(' / ', $parts);
                                if ($line === '') {
                                    $line = 'Bestaand artikel';
                                }
                                if ($matched_terms) {
                                    $line .= ' (match: ' . implode(', ', $matched_terms) . ')';
                                }
                                $conflict_labels[] = $line;
                            }
                            ?>
                            <tr>
                                <td><?php echo (int) $row->id; ?></td>
                                <td><?php echo esc_html((string) ($row->main_keyword ?: '')); ?></td>
                                <td><?php echo esc_html((string) ($row->site_name ?: '')); ?></td>
                                <td><?php echo esc_html($conflict_labels ? implode(' | ', $conflict_labels) : 'Onbekend conflict'); ?></td>
                                <td><?php echo esc_html($suggestion); ?></td>
                                <td><?php echo esc_html((string) $row->created_at); ?></td>
                            </tr>
                        <?php endforeach; ?>
                        </tbody>
                    </table>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
        <?php
    }

    public function render_editorial(): void {
        $available_sites = $this->db->get_results("
            SELECT id, name, base_url
            FROM {$this->table('sites')}
            ORDER BY is_active DESC, publish_priority ASC, name ASC
        ");
        $rows = $this->db->get_results("
            SELECT
                a.id,
                a.title,
                a.slug,
                a.meta_description,
                a.content,
                a.created_at,
                a.updated_at,
                a.site_id,
                k.main_keyword,
                c.name AS client_name,
                s.name AS site_name,
                s.base_url AS site_base_url,
                j.id AS job_id
            FROM {$this->table('articles')} a
            INNER JOIN {$this->table('jobs')} j ON j.id = a.job_id
            LEFT JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
            LEFT JOIN {$this->table('clients')} c ON c.id = a.client_id
            LEFT JOIN {$this->table('sites')} s ON s.id = a.site_id
            WHERE j.status = 'awaiting_approval'
            ORDER BY a.created_at ASC
            LIMIT 500
        ");
        ?>
        <div class="wrap">
            <h1>Redactionele approval</h1>
            <?php $this->render_admin_notice(); ?>
            <p>Kies hieronder welke artikelen gepubliceerd mogen worden op de remote blogs.</p>

            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <?php wp_nonce_field('sch_bulk_approve_publish'); ?>
                <input type="hidden" name="action" value="sch_bulk_approve_publish">
                <table class="widefat striped">
                    <thead>
                    <tr>
                        <th style="width:32px;"><input type="checkbox" onclick="document.querySelectorAll('.sch-article-check').forEach((el)=>{el.checked=this.checked;});"></th>
                        <th>Titel</th>
                        <th>Klant</th>
                        <th>Blog</th>
                        <th>Keyword</th>
                        <th>Omschrijving</th>
                        <th>Content check</th>
                        <th>Aangemaakt</th>
                        <th>Actie</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php if ($rows) : foreach ($rows as $row) : ?>
                        <tr>
                            <td><input class="sch-article-check" type="checkbox" name="article_ids[]" value="<?php echo (int) $row->id; ?>"></td>
                            <td><strong><?php echo esc_html((string) $row->title); ?></strong><br><span class="sch-muted sch-code"><?php echo esc_html((string) $row->slug); ?></span></td>
                            <td><?php echo esc_html((string) ($row->client_name ?: '')); ?></td>
                            <td>
                                <strong><?php echo esc_html((string) ($row->site_name ?: 'Onbekend')); ?></strong>
                                <?php if (!empty($row->site_base_url)) : ?>
                                    <br><span class="sch-muted sch-code"><?php echo esc_html((string) $row->site_base_url); ?></span>
                                <?php endif; ?>
                                <br>
                                <label class="screen-reader-text" for="sch-site-<?php echo (int) $row->id; ?>">Publiceer op blog</label>
                                <select id="sch-site-<?php echo (int) $row->id; ?>" name="article_sites[<?php echo (int) $row->id; ?>]">
                                    <?php foreach ((array) $available_sites as $site_option) : ?>
                                        <option value="<?php echo (int) $site_option->id; ?>" <?php selected((int) $row->site_id, (int) $site_option->id); ?>>
                                            <?php echo esc_html((string) $site_option->name . ' (' . $site_option->base_url . ')'); ?>
                                        </option>
                                    <?php endforeach; ?>
                                </select>
                            </td>
                            <td><?php echo esc_html((string) ($row->main_keyword ?: '')); ?></td>
                            <td><?php echo esc_html((string) $row->meta_description); ?></td>
                            <td>
                                <?php if (!empty($row->content)) : ?>
                                    <details>
                                        <summary>Bekijk artikel</summary>
                                        <div class="sch-editorial-content"><?php echo wp_kses_post((string) $row->content); ?></div>
                                    </details>
                                <?php else : ?>
                                    <span class="sch-muted">Geen content gevonden.</span>
                                <?php endif; ?>
                            </td>
                            <td><?php echo esc_html((string) $row->created_at); ?></td>
                            <td>
                                <button class="button button-primary" type="submit" name="publish_now_article_id" value="<?php echo (int) $row->id; ?>">Publiceren</button>
                            </td>
                        </tr>
                    <?php endforeach; else : ?>
                        <tr><td colspan="9">Geen artikelen die op redactionele approval wachten.</td></tr>
                    <?php endif; ?>
                    </tbody>
                </table>
                <?php if ($rows) : ?>
                    <p style="margin-top:12px;">
                        <button class="button button-primary" type="submit">Geselecteerde artikelen publiceren</button>
                    </p>
                <?php endif; ?>
            </form>
        </div>
        <?php
    }

    public function render_reporting(): void {
        $selected_month = isset($_GET['month']) ? sanitize_text_field((string) wp_unslash($_GET['month'])) : gmdate('Y-m');
        if (!preg_match('/^\d{4}\-\d{2}$/', $selected_month)) {
            $selected_month = gmdate('Y-m');
        }

        $client_filter = isset($_GET['client_id']) ? max(0, (int) $_GET['client_id']) : 0;
        $month_start = $selected_month . '-01 00:00:00';
        $month_end = gmdate('Y-m-d H:i:s', strtotime($selected_month . '-01 +1 month'));
        $clients = $this->db->get_results("SELECT id, name FROM {$this->table('clients')} ORDER BY name ASC");
        $clients_by_id = [];
        $clients_by_name = [];
        foreach ($clients as $client) {
            $clients_by_id[(int) $client->id] = (string) $client->name;
            $clients_by_name[(string) $client->name] = (int) $client->id;
        }

        $trend_months = [];
        for ($i = 11; $i >= 0; $i--) {
            $month_key = gmdate('Y-m', strtotime($selected_month . '-01 -' . $i . ' month'));
            $trend_months[] = [
                'key' => $month_key,
                'label' => gmdate('M Y', strtotime($month_key . '-01')),
            ];
        }
        $trend_start = $trend_months[0]['key'] . '-01 00:00:00';
        $trend_end = gmdate('Y-m-d H:i:s', strtotime($trend_months[count($trend_months) - 1]['key'] . '-01 +1 month'));

        $trend_query = "
            SELECT a.client_id, DATE_FORMAT(a.created_at, '%%Y-%%m') AS month_key, COUNT(*) AS article_count
            FROM {$this->table('articles')} a
            WHERE a.created_at >= %s AND a.created_at < %s
            GROUP BY a.client_id, DATE_FORMAT(a.created_at, '%%Y-%%m')
        ";
        $trend_rows = $this->db->get_results($this->db->prepare($trend_query, $trend_start, $trend_end));
        $trend_by_client = [];
        foreach ($trend_rows as $trend_row) {
            $trend_client_id = (int) ($trend_row->client_id ?? 0);
            if ($trend_client_id <= 0 || !isset($clients_by_id[$trend_client_id])) {
                continue;
            }

            if (!isset($trend_by_client[$trend_client_id])) {
                $trend_by_client[$trend_client_id] = [];
            }
            $trend_by_client[$trend_client_id][(string) $trend_row->month_key] = (int) $trend_row->article_count;
        }

        $query = "
            SELECT a.*, c.name AS client_name, s.name AS site_name, k.main_keyword
            FROM {$this->table('articles')} a
            LEFT JOIN {$this->table('clients')} c ON c.id = a.client_id
            LEFT JOIN {$this->table('sites')} s ON s.id = a.site_id
            LEFT JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
            WHERE a.created_at >= %s AND a.created_at < %s
        ";
        $params = [$month_start, $month_end];
        if ($client_filter > 0) {
            $query .= " AND a.client_id = %d";
            $params[] = $client_filter;
        }
        $query .= " ORDER BY a.created_at DESC";

        $articles = $this->db->get_results($this->db->prepare($query, ...$params));
        $by_client = [];
        $anchor_counts = [];
        foreach ($articles as $article) {
            $client_name = (string) ($article->client_name ?: 'Onbekende klant');
            if (!isset($by_client[$client_name])) {
                $by_client[$client_name] = [
                    'count' => 0,
                    'backlinks' => 0,
                ];
            }

            $backlinks = $this->decode_json_array($article->backlinks_data);
            $by_client[$client_name]['count']++;
            $by_client[$client_name]['backlinks'] += count($backlinks);

            foreach ($backlinks as $backlink) {
                $anchor = sanitize_text_field((string) ($backlink['anchor'] ?? ''));
                if ($anchor === '') {
                    $anchor = '(leeg)';
                }
                if (!isset($anchor_counts[$anchor])) {
                    $anchor_counts[$anchor] = 0;
                }
                $anchor_counts[$anchor]++;
            }
        }
        arsort($anchor_counts);
        ?>
        <div class="wrap">
            <h1>Rapportage</h1>
            <?php $this->render_admin_notice(); ?>

            <form method="get" style="margin-bottom:16px;">
                <input type="hidden" name="page" value="sch-reporting">
                <label for="sch-report-month"><strong>Maand</strong></label>
                <input id="sch-report-month" type="month" name="month" value="<?php echo esc_attr($selected_month); ?>">
                <label for="sch-report-client" style="margin-left:12px;"><strong>Klant</strong></label>
                <select id="sch-report-client" name="client_id">
                    <option value="0">Alle klanten</option>
                    <?php foreach ($clients as $client) : ?>
                        <option value="<?php echo (int) $client->id; ?>" <?php selected($client_filter, (int) $client->id); ?>>
                            <?php echo esc_html($client->name); ?>
                        </option>
                    <?php endforeach; ?>
                </select>
                <button class="button button-primary">Filter</button>
            </form>

            <h2>Overzicht per klant</h2>
            <table class="widefat striped">
                <thead><tr><th>Klant</th><th>Artikelen</th><th>Backlinks</th><th>Rapport</th></tr></thead>
                <tbody>
                <?php if ($by_client) : foreach ($by_client as $client_name => $stats) : ?>
                    <tr>
                        <td><?php echo esc_html($client_name); ?></td>
                        <td><?php echo (int) $stats['count']; ?></td>
                        <td><?php echo (int) $stats['backlinks']; ?></td>
                        <td>
                            <?php if (isset($clients_by_name[$client_name])) : ?>
                                <a href="<?php echo esc_url(admin_url('admin.php?page=sch-reporting&month=' . rawurlencode($selected_month) . '&client_id=' . (int) $clients_by_name[$client_name])); ?>">Open klantrapport</a>
                            <?php else : ?>
                                <span class="sch-muted">n.v.t.</span>
                            <?php endif; ?>
                        </td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="4">Geen artikelen gevonden voor deze selectie.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>

            <h2 style="margin-top:24px;">Per klant per maand (laatste 12 maanden)</h2>
            <table class="widefat striped">
                <thead>
                <tr>
                    <th>Klant</th>
                    <?php foreach ($trend_months as $trend_month) : ?>
                        <th><?php echo esc_html($trend_month['label']); ?></th>
                    <?php endforeach; ?>
                    <th>Totaal</th>
                </tr>
                </thead>
                <tbody>
                <?php if ($clients) : foreach ($clients as $client) : ?>
                    <?php
                    $client_month_counts = $trend_by_client[(int) $client->id] ?? [];
                    $client_total = 0;
                    ?>
                    <tr>
                        <td>
                            <strong><?php echo esc_html((string) $client->name); ?></strong><br>
                            <a href="<?php echo esc_url(admin_url('admin.php?page=sch-reporting&month=' . rawurlencode($selected_month) . '&client_id=' . (int) $client->id)); ?>">Klantrapport openen</a>
                        </td>
                        <?php foreach ($trend_months as $trend_month) : ?>
                            <?php
                            $month_key = (string) $trend_month['key'];
                            $month_count = (int) ($client_month_counts[$month_key] ?? 0);
                            $client_total += $month_count;
                            ?>
                            <td>
                                <?php if ($month_count > 0) : ?>
                                    <a href="<?php echo esc_url(admin_url('admin.php?page=sch-reporting&month=' . rawurlencode($month_key) . '&client_id=' . (int) $client->id)); ?>"><?php echo (int) $month_count; ?></a>
                                <?php else : ?>
                                    0
                                <?php endif; ?>
                            </td>
                        <?php endforeach; ?>
                        <td><strong><?php echo (int) $client_total; ?></strong></td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="<?php echo (int) (count($trend_months) + 2); ?>">Nog geen klanten gevonden.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>

            <h2 style="margin-top:24px;">Top gebruikte anchor teksten</h2>
            <table class="widefat striped">
                <thead><tr><th>Anchor tekst</th><th>Aantal keer gebruikt</th></tr></thead>
                <tbody>
                <?php if ($anchor_counts) : foreach ($anchor_counts as $anchor => $count) : ?>
                    <tr>
                        <td><?php echo esc_html($anchor); ?></td>
                        <td><?php echo (int) $count; ?></td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="2">Geen anchors beschikbaar in deze maand.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>

            <h2 style="margin-top:24px;">Artikelen met backlinks</h2>
            <table class="widefat striped">
                <thead><tr><th>Datum</th><th>Klant</th><th>Blog</th><th>Keyword</th><th>Titel</th><th>Artikel URL</th><th>Backlinks</th></tr></thead>
                <tbody>
                <?php if ($articles) : foreach ($articles as $article) : ?>
                    <?php $backlinks = $this->decode_json_array($article->backlinks_data); ?>
                    <tr>
                        <td><?php echo esc_html((string) $article->created_at); ?></td>
                        <td><?php echo esc_html((string) ($article->client_name ?: '')); ?></td>
                        <td><?php echo esc_html((string) ($article->site_name ?: '')); ?></td>
                        <td><?php echo esc_html((string) ($article->main_keyword ?: '')); ?></td>
                        <td><?php echo esc_html((string) $article->title); ?></td>
                        <td>
                            <?php if (!empty($article->remote_url)) : ?>
                                <a href="<?php echo esc_url((string) $article->remote_url); ?>" target="_blank" rel="noopener noreferrer"><?php echo esc_html((string) $article->remote_url); ?></a>
                            <?php else : ?>
                                <span class="sch-muted">Nog niet gepubliceerd</span>
                            <?php endif; ?>
                        </td>
                        <td><?php echo esc_html($this->implode_target_strings($backlinks)); ?></td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="7">Geen artikelen gevonden voor deze selectie.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
            <p class="description" style="margin-top:12px;">Backlinks en anchor teksten worden per artikel opgeslagen bij het aanmaken en blijven hierdoor historisch per maand beschikbaar.</p>
        </div>
        <?php
    }

    public function render_logs(): void {
        $rows = $this->db->get_results("SELECT * FROM {$this->table('logs')} ORDER BY id DESC LIMIT 300");
        ?>
        <div class="wrap">
            <h1>Logs</h1>
            <?php $this->render_admin_notice(); ?>
            <table class="widefat striped">
                <thead><tr><th>Tijd</th><th>Level</th><th>Context</th><th>Bericht</th><th>Payload</th></tr></thead>
                <tbody>
                <?php if ($rows) : foreach ($rows as $row) : ?>
                    <tr>
                        <td><?php echo esc_html($row->created_at); ?></td>
                        <td><?php echo esc_html($row->level); ?></td>
                        <td><?php echo esc_html($row->context); ?></td>
                        <td><?php echo esc_html($row->message); ?></td>
                        <td><div class="sch-log-payload"><?php echo esc_html((string) $row->payload); ?></div></td>
                    </tr>
                <?php endforeach; else : ?>
                    <tr><td colspan="5">Nog geen logs.</td></tr>
                <?php endif; ?>
                </tbody>
            </table>
        </div>
        <?php
    }

    public function render_settings(): void {
        ?>
        <div class="wrap">
            <h1>Instellingen</h1>
            <?php $this->render_admin_notice(); ?>

            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" class="sch-card">
                <?php wp_nonce_field('sch_save_settings'); ?>
                <input type="hidden" name="action" value="sch_save_settings">
                <table class="form-table">
                    <tr><th>OpenAI API key</th><td><input type="password" name="openai_api_key" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_OPENAI_API_KEY, '')); ?>"></td></tr>
                    <tr><th>OpenAI model</th><td><input type="text" name="openai_model" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_OPENAI_MODEL, 'gpt-5.4-mini')); ?>"></td></tr>
                    <tr><th>Temperature</th><td><input type="number" step="0.1" min="0" max="2" name="openai_temperature" value="<?php echo esc_attr((string) get_option(self::OPTION_OPENAI_TEMPERATURE, '0.6')); ?>"></td></tr>
                    <tr><th>Unsplash access key</th><td><input type="password" name="unsplash_access_key" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_UNSPLASH_ACCESS_KEY, '')); ?>"></td></tr>
                    <tr>
                        <th>Trusted source domein</th>
                        <td>
                            <input type="url" name="trusted_source_domain" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, 'https://shortcut.nl')); ?>">
                            <p class="description">De orchestrator stuurt dit domein mee als bron. De receiver plugin moet alleen requests accepteren wanneer dit exact matcht.</p>
                        </td>
                    </tr>
                    <tr><th>Featured images</th><td><label><input type="checkbox" name="enable_featured_images" value="1" <?php checked(get_option(self::OPTION_ENABLE_FEATURED_IMAGES, '1'), '1'); ?>> Inschakelen</label></td></tr>
                    <tr><th>Supporting content</th><td><label><input type="checkbox" name="enable_supporting" value="1" <?php checked(get_option(self::OPTION_ENABLE_SUPPORTING, '1'), '1'); ?>> Automatisch jobs maken</label></td></tr>
                    <tr><th>Auto discovery</th><td><label><input type="checkbox" name="enable_auto_discovery" value="1" <?php checked(get_option(self::OPTION_ENABLE_AUTO_DISCOVERY, '0'), '1'); ?>> Laat worker keyword discovery draaien als er geen queued jobs zijn</label></td></tr>
                    <tr><th>Max research pages</th><td><input type="number" name="max_research_pages" value="<?php echo esc_attr((string) get_option(self::OPTION_MAX_RESEARCH_PAGES, '5')); ?>" min="1" max="20"></td></tr>
                    <tr><th>Max discovery keywords</th><td><input type="number" name="max_discovery_keywords" value="<?php echo esc_attr((string) get_option(self::OPTION_MAX_DISCOVERY_KEYWORDS, '10')); ?>" min="1" max="50"></td></tr>
                    <tr><th>Verbose logs</th><td><label><input type="checkbox" name="enable_verbose_logs" value="1" <?php checked(get_option(self::OPTION_ENABLE_VERBOSE_LOGS, '1'), '1'); ?>> Veel extra logregels wegschrijven</label></td></tr>
                    <tr><th colspan="2"><h2 style="margin:10px 0 0;">Google Search Console</h2></th></tr>
                    <tr><th>Enable GSC integratie</th><td><label><input type="checkbox" name="gsc_enabled" value="1" <?php checked(get_option(self::OPTION_GSC_ENABLED, '0'), '1'); ?>> Inschakelen</label></td></tr>
                    <tr><th>Google OAuth client ID</th><td><input type="text" name="gsc_client_id" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_CLIENT_ID, '')); ?>"></td></tr>
                    <tr><th>Google OAuth client secret</th><td><input type="password" name="gsc_client_secret" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_CLIENT_SECRET, '')); ?>"></td></tr>
                    <tr><th>Default sync range (dagen)</th><td><select name="gsc_default_sync_range"><option value="7" <?php selected(get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28'), '7'); ?>>7</option><option value="28" <?php selected(get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28'), '28'); ?>>28</option><option value="90" <?php selected(get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28'), '90'); ?>>90</option></select></td></tr>
                    <tr><th>Default row limit</th><td><input type="number" name="gsc_default_row_limit" min="1" max="25000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, '250')); ?>"></td></tr>
                    <tr><th>Default top N op clicks</th><td><input type="number" name="gsc_default_top_n_clicks" min="0" max="25000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_TOP_N_CLICKS, '0')); ?>"><p class="description">0 = uitgeschakeld (alle rows binnen row limit).</p></td></tr>
                    <tr><th>Default min impressions</th><td><input type="number" name="gsc_default_min_impressions" min="0" max="100000000" value="<?php echo esc_attr((string) get_option(self::OPTION_GSC_DEFAULT_MIN_IMPRESSIONS, '0')); ?>"></td></tr>
                    <tr><th>Auto sync</th><td><label><input type="checkbox" name="gsc_auto_sync" value="1" <?php checked(get_option(self::OPTION_GSC_AUTO_SYNC, '0'), '1'); ?>> Dagelijks gekoppelde klanten syncen</label></td></tr>

                    <tr><th colspan="2"><h2 style="margin:10px 0 0;">Random Content Machine</h2></th></tr>
                    <tr><th>Enable random content machine</th><td><label><input type="checkbox" name="random_machine_enabled" value="1" <?php checked(get_option(self::OPTION_RANDOM_MACHINE_ENABLED, '0'), '1'); ?>> Dagelijks automatisch random content jobs genereren</label></td></tr>
                    <tr><th>Max random articles per day</th><td><input type="number" name="random_daily_max" value="<?php echo esc_attr((string) get_option(self::OPTION_RANDOM_DAILY_MAX, '10')); ?>" min="1" max="100"></td></tr>
                    <tr><th>Random content status</th><td><select name="random_status"><option value="draft" <?php selected((string) get_option(self::OPTION_RANDOM_STATUS, 'draft'), 'draft'); ?>>draft</option><option value="publish" <?php selected((string) get_option(self::OPTION_RANDOM_STATUS, 'draft'), 'publish'); ?>>publish</option></select></td></tr>
                    <tr><th>Min word count</th><td><input type="number" name="random_min_words" value="<?php echo esc_attr((string) get_option(self::OPTION_RANDOM_MIN_WORDS, '900')); ?>" min="400" max="5000"></td></tr>
                    <tr><th>Max word count</th><td><input type="number" name="random_max_words" value="<?php echo esc_attr((string) get_option(self::OPTION_RANDOM_MAX_WORDS, '1400')); ?>" min="500" max="6000"></td></tr>
                    <tr><th>Max articles per site per day</th><td><input type="number" name="random_max_per_site_per_day" value="<?php echo esc_attr((string) get_option(self::OPTION_RANDOM_MAX_PER_SITE_PER_DAY, '2')); ?>" min="1" max="20"></td></tr>
                    <tr><th>Only active sites meenemen</th><td><label><input type="checkbox" name="random_only_active_sites" value="1" <?php checked(get_option(self::OPTION_RANDOM_ONLY_ACTIVE_SITES, '1'), '1'); ?>> Alleen actieve blogs gebruiken</label></td></tr>
                    <tr><th>Allowed categories (optioneel)</th><td>
                        <?php $random_allowed_categories = $this->sanitize_blog_categories($this->decode_json_array(get_option(self::OPTION_RANDOM_ALLOWED_CATEGORIES, wp_json_encode([])))); ?>
                        <select name="random_allowed_categories[]" multiple size="8" style="min-width:260px;">
                            <?php foreach ($this->allowed_blog_categories() as $category) : ?>
                                <option value="<?php echo esc_attr($category); ?>" <?php selected(in_array($category, $random_allowed_categories, true)); ?>><?php echo esc_html($category); ?></option>
                            <?php endforeach; ?>
                        </select>
                        <p class="description">Leeg laten = geen categorie-filter.</p>
                    </td></tr>
                    <tr><th>Exclude recent topic duplicates window (dagen)</th><td><input type="number" name="random_duplicate_window_days" value="<?php echo esc_attr((string) get_option(self::OPTION_RANDOM_DUPLICATE_WINDOW_DAYS, '30')); ?>" min="1" max="365"></td></tr>
                </table>
                <p><button class="button button-primary">Instellingen opslaan</button></p>
                <p>Gebruik hier je eigen sleutels. Hardcode ze niet in de plugin.</p>
            </form>
        </div>
        <?php
    }

    private function now(): string {
        return current_time('mysql');
    }

    private function is_verbose_logging_enabled(): bool {
        return get_option(self::OPTION_ENABLE_VERBOSE_LOGS, '1') === '1';
    }

    private function vlog(string $context, string $message, $payload = null): void {
        if ($this->is_verbose_logging_enabled()) {
            $this->log('info', $context, $message, $payload);
        }
    }

    private function redirect_with_message(string $page, string $message, string $type = 'success', array $extra_args = []): void {
        $url = add_query_arg(array_merge([
            'page' => $page,
            'sch_message' => rawurlencode($message),
            'sch_message_type' => rawurlencode($type),
        ], $extra_args), admin_url('admin.php'));

        wp_safe_redirect($url);
        exit;
    }

    private function verify_admin_nonce(string $action): void {
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer($action);
    }

    private function decode_json_array($value): array {
        if (is_array($value)) {
            return $value;
        }
        if (!is_string($value) || trim($value) === '') {
            return [];
        }
        $decoded = json_decode($value, true);
        return is_array($decoded) ? $decoded : [];
    }

    private function implode_target_strings(array $targets): string {
        $out = [];
        foreach ($targets as $target) {
            $url = trim((string) ($target['url'] ?? ''));
            $anchor = trim((string) ($target['anchor'] ?? ''));
            if ($url !== '') {
                $out[] = $url . ($anchor !== '' ? ' [' . $anchor . ']' : '');
            }
        }
        return implode(' | ', $out);
    }

    private function extract_backlinks_from_content(string $content, array $allowed_targets): array {
        if ($content === '' || !$allowed_targets) {
            return [];
        }

        $allowed_by_url = [];
        foreach ($allowed_targets as $target) {
            $target_url = esc_url_raw((string) ($target['url'] ?? ''));
            if ($target_url !== '') {
                $allowed_by_url[$target_url] = true;
            }
        }

        if (!$allowed_by_url) {
            return [];
        }

        $matches = [];
        preg_match_all('/<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)<\/a>/is', $content, $matches, PREG_SET_ORDER);

        $backlinks = [];
        foreach ($matches as $match) {
            $url = esc_url_raw(html_entity_decode((string) ($match[1] ?? ''), ENT_QUOTES, 'UTF-8'));
            if ($url === '' || !isset($allowed_by_url[$url])) {
                continue;
            }

            $anchor = sanitize_text_field(wp_strip_all_tags((string) ($match[2] ?? '')));
            $backlinks[] = [
                'url' => $url,
                'anchor' => $anchor,
            ];
        }

        return $backlinks;
    }

    private function normalize_anchor_text(string $text): string {
        $text = function_exists('mb_strtolower') ? mb_strtolower($text) : strtolower($text);
        $text = preg_replace('/\s+/u', ' ', $text);
        return trim((string) $text);
    }

    private function classify_anchor_type(string $anchor, string $main_keyword, object $client): string {
        $anchor_normalized = $this->normalize_anchor_text(wp_strip_all_tags($anchor));
        $keyword_normalized = $this->normalize_anchor_text($main_keyword);

        if ($anchor_normalized === '' || in_array($anchor_normalized, ['klik hier', 'lees meer', 'meer info'], true)) {
            return 'generic';
        }

        if ($keyword_normalized !== '' && $anchor_normalized === $keyword_normalized) {
            return 'exact';
        }

        $client_name = $this->normalize_anchor_text((string) ($client->name ?? ''));
        $website_host = $this->normalize_anchor_text((string) wp_parse_url((string) ($client->website_url ?? ''), PHP_URL_HOST));
        $website_host = str_replace('www.', '', $website_host);

        if (($client_name !== '' && strpos($anchor_normalized, $client_name) !== false) || ($website_host !== '' && strpos($anchor_normalized, $website_host) !== false)) {
            return 'branded';
        }

        if ($keyword_normalized !== '' && strpos($anchor_normalized, $keyword_normalized) !== false) {
            return 'partial';
        }

        return 'generic';
    }

    private function store_anchor_history_rows(object $job, object $keyword, object $client, int $article_id, array $backlinks): void {
        if (!$backlinks) {
            return;
        }

        foreach ($backlinks as $backlink) {
            $target_url = esc_url_raw((string) ($backlink['url'] ?? ''));
            $anchor_text = sanitize_text_field((string) ($backlink['anchor'] ?? ''));
            if ($target_url === '') {
                continue;
            }

            $anchor_type = $this->classify_anchor_type($anchor_text, (string) $keyword->main_keyword, $client);
            $this->db->insert($this->table('anchor_history'), [
                'client_id' => (int) $client->id,
                'keyword_id' => (int) $keyword->id,
                'job_id' => (int) $job->id,
                'article_id' => $article_id,
                'target_url' => $target_url,
                'anchor_text' => $anchor_text,
                'anchor_type' => $anchor_type,
                'created_at' => $this->now(),
            ]);
        }
    }

    private function get_anchor_history_stats(int $client_id, string $target_url): array {
        $rows = $this->db->get_results($this->db->prepare(
            "SELECT anchor_type, COUNT(*) AS total
             FROM {$this->table('anchor_history')}
             WHERE client_id=%d AND target_url=%s
             GROUP BY anchor_type",
            $client_id,
            $target_url
        ));

        $stats = [
            'total' => 0,
            'exact' => 0,
            'partial' => 0,
            'branded' => 0,
            'generic' => 0,
        ];

        foreach ((array) $rows as $row) {
            $type = sanitize_key((string) ($row->anchor_type ?? ''));
            $count = (int) ($row->total ?? 0);
            if ($count < 1) {
                continue;
            }
            $stats['total'] += $count;
            if (isset($stats[$type])) {
                $stats[$type] += $count;
            }
        }

        $stats['exact_ratio_percent'] = $stats['total'] > 0 ? round(($stats['exact'] / $stats['total']) * 100, 1) : 0.0;
        return $stats;
    }

    private function sanitize_link_targets_from_post(): string {
        $urls    = array_map('wp_unslash', (array) ($_POST['link_target_url'] ?? []));
        $anchors = array_map('wp_unslash', (array) ($_POST['link_target_anchor'] ?? []));
        $items   = [];
        $count   = max(count($urls), count($anchors));

        for ($i = 0; $i < $count; $i++) {
            $url = esc_url_raw(trim((string) ($urls[$i] ?? '')));
            $anchor = sanitize_text_field((string) ($anchors[$i] ?? ''));
            if ($url === '') {
                continue;
            }
            $items[] = ['url' => $url, 'anchor' => $anchor];
        }

        return wp_json_encode($items);
    }

    private function sanitize_research_urls_from_post(): string {
        $urls = array_map('wp_unslash', (array) ($_POST['research_url'] ?? []));
        $items = [];

        foreach ($urls as $url) {
            $url = esc_url_raw(trim((string) $url));
            if ($url === '') {
                continue;
            }
            $items[] = ['url' => $url];
        }

        return wp_json_encode($items);
    }

    private function get_client_month_window(): array {
        $now = current_datetime();
        $month_start = (clone $now)->modify('first day of this month')->setTime(0, 0, 0);
        $month_end = (clone $month_start)->modify('first day of next month');
        $days_in_month = (int) $month_start->format('t');
        $day_in_month = (int) $now->format('j');

        return [
            'month_start' => $month_start->format('Y-m-d H:i:s'),
            'month_end' => $month_end->format('Y-m-d H:i:s'),
            'days_in_month' => max(1, $days_in_month),
            'day_in_month' => max(1, $day_in_month),
        ];
    }

    private function get_client_monthly_creation_budget(int $client_id, int $max_posts_per_month): array {
        if ($max_posts_per_month <= 0) {
            return [
                'allowed_now' => PHP_INT_MAX,
                'already_created' => 0,
                'remaining' => PHP_INT_MAX,
                'reason' => 'unlimited',
            ];
        }

        $window = $this->get_client_month_window();
        $allowed_by_today = (int) ceil(($window['day_in_month'] / $window['days_in_month']) * $max_posts_per_month);
        $allowed_by_today = min($max_posts_per_month, max(0, $allowed_by_today));

        $already_created = (int) $this->db->get_var($this->db->prepare(
            "SELECT COUNT(*) FROM {$this->table('jobs')} WHERE client_id=%d AND created_at >= %s AND created_at < %s",
            $client_id,
            $window['month_start'],
            $window['month_end']
        ));

        $remaining = max(0, $allowed_by_today - $already_created);

        return [
            'allowed_now' => $allowed_by_today,
            'already_created' => $already_created,
            'remaining' => $remaining,
            'reason' => $remaining > 0 ? 'quota_available' : 'quota_blocked_for_today',
            'window' => $window,
        ];
    }

    private function sanitize_secondary_keywords_from_post(): string {
        $items = [];
        foreach ((array) ($_POST['secondary_keywords'] ?? []) as $keyword) {
            $keyword = sanitize_text_field(wp_unslash((string) $keyword));
            if ($keyword !== '') {
                $items[] = ['keyword' => $keyword];
            }
        }
        return wp_json_encode($items);
    }

    private function get_secondary_keywords_list(object $keyword): array {
        $decoded = $this->decode_json_array($keyword->secondary_keywords);
        $list = [];
        foreach ($decoded as $item) {
            $value = is_array($item) ? trim((string) ($item['keyword'] ?? '')) : trim((string) $item);
            if ($value !== '') {
                $list[] = $value;
            }
        }
        return $list;
    }

    private function get_client_link_targets(object $client): array {
        $targets = $this->decode_json_array($client->link_targets);
        $clean = [];
        foreach ($targets as $target) {
            $url = esc_url_raw((string) ($target['url'] ?? ''));
            $anchor = sanitize_text_field((string) ($target['anchor'] ?? ''));
            if ($url !== '') {
                $clean[] = ['url' => $url, 'anchor' => $anchor];
            }
        }
        return $clean;
    }

    private function get_client_research_urls(object $client): array {
        $targets = $this->decode_json_array($client->research_urls);
        $clean = [];
        foreach ($targets as $target) {
            $url = esc_url_raw((string) ($target['url'] ?? ''));
            if ($url !== '') {
                $clean[] = ['url' => $url];
            }
        }
        return $clean;
    }

    private function generate_secret(int $length = 32): string {
        return wp_generate_password($length, false, false);
    }

    private function normalize_site_url(string $url): string {
        $url = trim($url);
        if ($url === '') {
            return '';
        }
        if (!preg_match('~^https?://~i', $url)) {
            $url = 'https://' . ltrim($url, '/');
        }
        return untrailingslashit(esc_url_raw($url));
    }

    private function normalize_host(string $url): string {
        $host = wp_parse_url($url, PHP_URL_HOST);
        return strtolower((string) $host);
    }

    private function json_response(array $data, int $status_code = 200): void {
        status_header($status_code);
        header('Content-Type: application/json; charset=' . get_bloginfo('charset'));
        echo wp_json_encode($data);
        exit;
    }

    private function get_trusted_source_domain(): string {
        $url = $this->normalize_site_url((string) get_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, 'https://shortcut.nl'));
        if ($url === '') {
            $url = 'https://shortcut.nl';
        }
        return $url;
    }

    private function looks_like_site_url(string $value): bool {
        $value = trim($value);
        if ($value === '') {
            return false;
        }

        return (bool) (
            filter_var($value, FILTER_VALIDATE_URL) ||
            preg_match('~^[a-z0-9][a-z0-9\.\-]+\.[a-z]{2,}(/.*)?$~i', $value)
        );
    }

    private function is_valid_default_status(string $value): bool {
        return in_array(strtolower(trim($value)), ['draft', 'publish'], true);
    }

    private function derive_site_name_from_url(string $url): string {
        $host = wp_parse_url($url, PHP_URL_HOST);
        if (!$host) {
            return 'Onbekende blog';
        }
        $host = preg_replace('~^www\.~i', '', (string) $host);
        return ucwords(str_replace(['-', '.'], [' ', ' '], $host));
    }

    private function split_bulk_site_line(string $line): array {
        $line = trim($line);
        if ($line === '') {
            return [];
        }

        if (strpos($line, '|') !== false) {
            $parts = array_map('trim', explode('|', $line));
            return array_values(array_filter($parts, static function ($value) {
                return $value !== '';
            }));
        }

        if (strpos($line, "\t") !== false) {
            $parts = array_map('trim', explode("\t", $line));
            return array_values(array_filter($parts, static function ($value) {
                return $value !== '';
            }));
        }

        $csv = str_getcsv($line);
        $csv = array_map('trim', $csv);
        $csv = array_values(array_filter($csv, static function ($value) {
            return $value !== '';
        }));
        return $csv;
    }

    private function parse_bulk_sites_input(
        string $input,
        string $fallback_status,
        string $fallback_category,
        int $fallback_max_posts_per_day,
        int $fallback_publish_priority
    ): array {
        $rows = preg_split('/\r\n|\r|\n/', $input);
        $items = [];
        $line_number = 0;

        foreach ((array) $rows as $line) {
            $line_number++;
            $line = trim($line);

            if ($line === '' || strpos($line, '#') === 0) {
                continue;
            }

            $parts = $this->split_bulk_site_line($line);
            if (!$parts) {
                continue;
            }

            $name = '';
            $base_url = '';
            $default_status = $fallback_status;
            $default_category = $fallback_category;
            $max_posts_per_day = $fallback_max_posts_per_day;
            $publish_priority = $fallback_publish_priority;

            if (count($parts) === 1) {
                $base_url = $parts[0];
            } else {
                if ($this->looks_like_site_url($parts[0])) {
                    $base_url = $parts[0];
                    $remaining = array_slice($parts, 1);
                } else {
                    $name = $parts[0];
                    $base_url = $parts[1] ?? '';
                    $remaining = array_slice($parts, 2);
                }

                if (!empty($remaining) && !$this->is_valid_default_status((string) $remaining[0])) {
                    array_shift($remaining);
                }

                if (isset($remaining[0]) && $this->is_valid_default_status((string) $remaining[0])) {
                    $default_status = strtolower((string) $remaining[0]);
                }

                if (isset($remaining[1])) {
                    $default_category = $this->sanitize_blog_category((string) $remaining[1]);
                }

                if (isset($remaining[2])) {
                    $max_posts_per_day = max(1, (int) $remaining[2]);
                }

                if (isset($remaining[3])) {
                    $publish_priority = (int) $remaining[3];
                }
            }

            $base_url = $this->normalize_site_url($base_url);
            if ($base_url === '') {
                $items[] = [
                    'error' => 'Regel ' . $line_number . ': geen geldige URL gevonden.',
                ];
                continue;
            }

            if ($name === '') {
                $name = $this->derive_site_name_from_url($base_url);
            }

            if (!$this->is_valid_default_status($default_status)) {
                $default_status = $fallback_status;
            }

            $items[] = [
                'name' => sanitize_text_field($name),
                'base_url' => $base_url,
                'receiver_secret' => '',
                'default_status' => $default_status,
                'default_category' => $this->sanitize_blog_category((string) $default_category),
                'max_posts_per_day' => $max_posts_per_day,
                'publish_priority' => $publish_priority,
            ];
        }

        return $items;
    }

    public function handle_save_client(): void {
        $this->verify_admin_nonce('sch_save_client');
        $id = (int) ($_POST['id'] ?? 0);

        $data = [
            'name'           => sanitize_text_field($_POST['name'] ?? ''),
            'website_url'    => esc_url_raw($_POST['website_url'] ?? ''),
            'default_anchor' => sanitize_text_field($_POST['default_anchor'] ?? ''),
            'link_targets'   => $this->sanitize_link_targets_from_post(),
            'research_urls'  => $this->sanitize_research_urls_from_post(),
            'max_posts_per_month' => max(0, (int) ($_POST['max_posts_per_month'] ?? 0)),
            'is_active'      => 1,
            'updated_at'     => $this->now(),
        ];

        if ($id > 0) {
            $updated = $this->db->update($this->table('clients'), $data, ['id' => $id]);
            if ($updated === false) {
                $this->log('error', 'client', 'Client update mislukt', [
                    'client_id' => $id,
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-clients', 'Klant bijwerken mislukt. Check logs.', 'error');
            }
        } else {
            $data['created_at'] = $this->now();
            $inserted = $this->db->insert($this->table('clients'), $data);
            if (!$inserted) {
                $this->log('error', 'client', 'Client insert mislukt', [
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-clients', 'Klant opslaan mislukt. Check logs.', 'error');
            }
            $id = (int) $this->db->insert_id;
        }

        $this->vlog('client', 'Klant opgeslagen', $data);
        $this->redirect_with_message('sch-clients', 'Klant opgeslagen.', 'success', [
            'edit' => $id,
        ]);
    }

    public function handle_register_receiver_blog(): void {
        $raw = file_get_contents('php://input');
        $payload = json_decode((string) $raw, true);
        if (!is_array($payload)) {
            $payload = array_map('wp_unslash', $_POST);
        }

        $blog_url = $this->normalize_site_url((string) ($payload['blog_url'] ?? ''));
        if ($blog_url === '') {
            $this->json_response([
                'success' => false,
                'message' => 'blog_url ontbreekt of is ongeldig.',
            ], 400);
        }

        $source_header = $this->normalize_site_url((string) ($_SERVER['HTTP_X_SCH_SOURCE_SITE'] ?? ''));
        if ($source_header !== '' && $this->normalize_host($source_header) !== $this->normalize_host($blog_url)) {
            $this->log('warning', 'receiver_registration', 'Registratie afgewezen door host mismatch', [
                'blog_url' => $blog_url,
                'source_header' => $source_header,
            ]);
            $this->json_response([
                'success' => false,
                'message' => 'Source header komt niet overeen met blog_url.',
            ], 403);
        }

        $default_status = sanitize_key((string) ($payload['default_status'] ?? 'draft'));
        if (!$this->is_valid_default_status($default_status)) {
            $default_status = 'draft';
        }

        $name = sanitize_text_field((string) ($payload['blog_name'] ?? ''));
        if ($name === '') {
            $name = $this->derive_site_name_from_url($blog_url);
        }

        $data = [
            'name' => $name,
            'base_url' => $blog_url,
            'receiver_secret' => '',
            'default_status' => $default_status,
            'default_category' => $this->sanitize_blog_category((string) ($payload['default_category'] ?? '')),
            'max_posts_per_day' => max(1, (int) ($payload['max_posts_per_day'] ?? 3)),
            'publish_priority' => (int) ($payload['publish_priority'] ?? 10),
            'is_active' => 1,
            'updated_at' => $this->now(),
        ];

        $existing_id = (int) $this->db->get_var($this->db->prepare(
            "SELECT id FROM {$this->table('sites')} WHERE base_url=%s LIMIT 1",
            $blog_url
        ));

        if ($data['default_category'] === '' && $existing_id > 0) {
            $existing_category = (string) $this->db->get_var($this->db->prepare(
                "SELECT default_category FROM {$this->table('sites')} WHERE id=%d LIMIT 1",
                $existing_id
            ));
            $existing_category = $this->sanitize_blog_category($existing_category);
            if ($existing_category !== '') {
                $data['default_category'] = $existing_category;
                $this->vlog('site_category', 'Bestaande handmatige categorie behouden bij receiver-registratie.', [
                    'site_id' => $existing_id,
                    'base_url' => $data['base_url'],
                    'category' => $existing_category,
                ]);
            }
        }

        if ($data['default_category'] === '') {
            $data['default_category'] = $this->determine_category_for_site($data['base_url'], $data['name']);
        }

        $operation = 'created';
        if ($existing_id > 0) {
            $updated = $this->db->update($this->table('sites'), $data, ['id' => $existing_id]);
            if ($updated === false) {
                $this->log('error', 'receiver_registration', 'Bestaande blog updaten mislukt', [
                    'site_id' => $existing_id,
                    'db_error' => $this->db->last_error,
                    'payload' => $payload,
                ]);
                $this->json_response([
                    'success' => false,
                    'message' => 'Bestaande blog kon niet worden bijgewerkt.',
                ], 500);
            }
            $site_id = $existing_id;
            $operation = 'updated';
        } else {
            $data['created_at'] = $this->now();
            $inserted = $this->db->insert($this->table('sites'), $data);
            if (!$inserted) {
                $this->log('error', 'receiver_registration', 'Nieuwe blog registreren mislukt', [
                    'db_error' => $this->db->last_error,
                    'payload' => $payload,
                ]);
                $this->json_response([
                    'success' => false,
                    'message' => 'Blog kon niet worden aangemaakt.',
                ], 500);
            }
            $site_id = (int) $this->db->insert_id;
        }

        $this->log('info', 'receiver_registration', 'Receiver-blog geregistreerd', [
            'site_id' => $site_id,
            'operation' => $operation,
            'blog_url' => $blog_url,
            'receiver_url' => esc_url_raw((string) ($payload['receiver_url'] ?? '')),
            'receiver_version' => sanitize_text_field((string) ($payload['receiver_version'] ?? '')),
        ]);

        $this->json_response([
            'success' => true,
            'site_id' => $site_id,
            'operation' => $operation,
            'blog_url' => $blog_url,
        ], 200);
    }

    public function handle_save_site(): void {
        $this->verify_admin_nonce('sch_save_site');
        $id = (int) ($_POST['id'] ?? 0);

        $data = [
            'name'              => sanitize_text_field($_POST['name'] ?? ''),
            'base_url'          => $this->normalize_site_url((string) ($_POST['base_url'] ?? '')),
            'receiver_secret'   => '',
            'default_status'    => sanitize_text_field($_POST['default_status'] ?? 'draft'),
            'default_category'  => $this->sanitize_blog_category((string) ($_POST['default_category'] ?? '')),
            'max_posts_per_day' => max(1, (int) ($_POST['max_posts_per_day'] ?? 3)),
            'publish_priority'  => (int) ($_POST['publish_priority'] ?? 10),
            'is_active'         => 1,
            'updated_at'        => $this->now(),
        ];

        if ($data['base_url'] === '') {
            $this->redirect_with_message('sch-sites', 'Geen geldige blog URL opgegeven.', 'error');
        }

        if (!$this->is_valid_default_status($data['default_status'])) {
            $data['default_status'] = 'draft';
        }

        if ($data['default_category'] === '') {
            $data['default_category'] = $this->determine_category_for_site($data['base_url'], $data['name']);
        }

        if ($id > 0) {
            $updated = $this->db->update($this->table('sites'), $data, ['id' => $id]);
            if ($updated === false) {
                $this->log('error', 'site', 'Site update mislukt', [
                    'site_id' => $id,
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-sites', 'Blog bijwerken mislukt. Check logs.', 'error');
            }
        } else {
            $data['created_at'] = $this->now();
            $inserted = $this->db->insert($this->table('sites'), $data);
            if (!$inserted) {
                $this->log('error', 'site', 'Site insert mislukt', [
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-sites', 'Blog opslaan mislukt. Check logs.', 'error');
            }
        }

        $this->vlog('site', 'Blog opgeslagen', $data);
        $this->redirect_with_message('sch-sites', 'Blog opgeslagen.');
    }

    public function handle_bulk_save_sites(): void {
        $this->verify_admin_nonce('sch_bulk_save_sites');

        $bulk_sites = (string) wp_unslash($_POST['bulk_sites'] ?? '');
        $fallback_status = sanitize_text_field($_POST['bulk_default_status'] ?? 'draft');
        $fallback_category = $this->sanitize_blog_category((string) ($_POST['bulk_default_category'] ?? ''));
        $fallback_max_posts_per_day = max(1, (int) ($_POST['bulk_max_posts_per_day'] ?? 3));
        $fallback_publish_priority = (int) ($_POST['bulk_publish_priority'] ?? 10);
        $update_existing = isset($_POST['bulk_update_existing']);

        if (!$this->is_valid_default_status($fallback_status)) {
            $fallback_status = 'draft';
        }

        $parsed = $this->parse_bulk_sites_input(
            $bulk_sites,
            $fallback_status,
            $fallback_category,
            $fallback_max_posts_per_day,
            $fallback_publish_priority
        );

        $result = [
            'created' => 0,
            'updated' => 0,
            'skipped' => 0,
            'messages' => [],
        ];

        foreach ($parsed as $item) {
            if (!empty($item['error'])) {
                $result['skipped']++;
                $result['messages'][] = $item['error'];
                $this->log('warning', 'site_bulk', 'Bulkregel overgeslagen', $item);
                continue;
            }

            $existing = $this->db->get_row($this->db->prepare(
                "SELECT * FROM {$this->table('sites')} WHERE base_url=%s LIMIT 1",
                $item['base_url']
            ));

            $data = [
                'name'              => $item['name'],
                'base_url'          => $item['base_url'],
                'receiver_secret'   => '',
                'default_status'    => $item['default_status'],
                'default_category'  => $item['default_category'],
                'max_posts_per_day' => $item['max_posts_per_day'],
                'publish_priority'  => $item['publish_priority'],
                'is_active'         => 1,
                'updated_at'        => $this->now(),
            ];

            if ($data['default_category'] === '') {
                if ($existing && $this->sanitize_blog_category((string) $existing->default_category) !== '') {
                    $data['default_category'] = $this->sanitize_blog_category((string) $existing->default_category);
                    $this->vlog('site_category', 'Bestaande handmatige categorie behouden bij bulk update.', [
                        'site_id' => (int) $existing->id,
                        'base_url' => $data['base_url'],
                        'category' => $data['default_category'],
                    ]);
                } else {
                    $data['default_category'] = $this->determine_category_for_site($data['base_url'], $data['name']);
                }
            }

            if ($existing) {
                if ($update_existing) {
                    $updated = $this->db->update($this->table('sites'), $data, ['id' => (int) $existing->id]);
                    if ($updated === false) {
                        $result['skipped']++;
                        $result['messages'][] = 'DB fout bij bijwerken: ' . $item['base_url'];
                        $this->log('error', 'site_bulk', 'Bulk site update mislukt', [
                            'existing_id' => (int) $existing->id,
                            'db_error' => $this->db->last_error,
                            'data' => $data,
                        ]);
                    } else {
                        $result['updated']++;
                    }
                } else {
                    $result['skipped']++;
                    $result['messages'][] = 'Overgeslagen, bestaat al: ' . $item['base_url'];
                }
            } else {
                $data['created_at'] = $this->now();
                $inserted = $this->db->insert($this->table('sites'), $data);
                if (!$inserted) {
                    $result['skipped']++;
                    $result['messages'][] = 'DB fout bij insert: ' . $item['base_url'];
                    $this->log('error', 'site_bulk', 'Bulk site insert mislukt', [
                        'db_error' => $this->db->last_error,
                        'data' => $data,
                    ]);
                } else {
                    $result['created']++;
                }
            }
        }

        $this->vlog('site_bulk', 'Bulk blogs verwerkt', $result);

        set_transient('sch_bulk_sites_result_' . get_current_user_id(), $result, MINUTE_IN_SECONDS * 5);
        wp_safe_redirect(admin_url('admin.php?page=sch-sites'));
        exit;
    }

    public function handle_bulk_update_sites_status(): void {
        $this->verify_admin_nonce('sch_bulk_update_sites_status');

        $status = sanitize_key((string) ($_POST['default_status'] ?? 'draft'));
        if (!$this->is_valid_default_status($status)) {
            $status = 'draft';
        }

        $updated = $this->db->query($this->db->prepare(
            "UPDATE {$this->table('sites')} SET default_status=%s, updated_at=%s",
            $status,
            $this->now()
        ));

        if ($updated === false) {
            $this->log('error', 'site_bulk', 'Bulk status update mislukt', [
                'status' => $status,
                'db_error' => $this->db->last_error,
            ]);
            $this->redirect_with_message('sch-sites', 'Bulk status update mislukt. Check logs.', 'error');
        }

        $this->redirect_with_message(
            'sch-sites',
            sprintf('Default status bijgewerkt naar "%s" voor %d blogs.', $status, (int) $updated)
        );
    }

    public function handle_save_keyword(): void {
        $this->verify_admin_nonce('sch_save_keyword');

        $id = (int) ($_POST['id'] ?? 0);
        $site_ids = array_values(array_unique(array_filter(array_map('intval', (array) ($_POST['target_site_ids'] ?? [])))));
        $selected_categories = $this->sanitize_blog_categories((array) ($_POST['target_site_categories'] ?? []));

        if (empty($site_ids)) {
            $this->log('error', 'keyword', 'Keyword niet opgeslagen: geen doelblogs geselecteerd', [
                'post' => $_POST,
            ]);
            $this->redirect_with_message('sch-keywords', 'Selecteer minimaal 1 doelblog voor dit keyword.', 'error');
        }

        $client_id = (int) ($_POST['client_id'] ?? 0);
        $main_keyword = sanitize_text_field($_POST['main_keyword'] ?? '');

        if ($client_id <= 0 || $main_keyword === '') {
            $this->log('error', 'keyword', 'Keyword niet opgeslagen: client of hoofdkeyword ontbreekt', [
                'client_id' => $client_id,
                'main_keyword' => $main_keyword,
            ]);
            $this->redirect_with_message('sch-keywords', 'Klant en hoofdkeyword zijn verplicht.', 'error');
        }

        $data = [
            'client_id'          => $client_id,
            'main_keyword'       => $main_keyword,
            'secondary_keywords' => $this->sanitize_secondary_keywords_from_post(),
            'target_site_ids'    => wp_json_encode($site_ids),
            'target_site_categories' => wp_json_encode($selected_categories),
            'content_type'       => sanitize_text_field($_POST['content_type'] ?? 'pillar'),
            'tone_of_voice'      => sanitize_text_field($_POST['tone_of_voice'] ?? 'deskundig maar menselijk'),
            'target_word_count'  => max(300, (int) ($_POST['target_word_count'] ?? 1200)),
            'priority'           => (int) ($_POST['priority'] ?? 10),
            'status'             => 'queued',
            'source'             => 'manual',
            'updated_at'         => $this->now(),
        ];

        if ($id > 0) {
            $updated = $this->db->update($this->table('keywords'), $data, ['id' => $id]);

            if ($updated === false) {
                $this->log('error', 'keyword', 'Keyword update mislukt', [
                    'keyword_id' => $id,
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-keywords', 'Keyword bijwerken mislukt. Check logs.', 'error');
            }

            $this->delete_jobs_for_keyword($id);
            $created_jobs = $this->create_jobs_for_keyword($id);

            if ($created_jobs < 1) {
                $this->redirect_with_message('sch-keywords', 'Keyword opgeslagen, maar er zijn geen jobs aangemaakt. Check logs.', 'error');
            }

            $this->redirect_with_message('sch-keywords', 'Keyword bijgewerkt. Jobs aangemaakt: ' . $created_jobs);
        } else {
            $data['created_at'] = $this->now();
            $inserted = $this->db->insert($this->table('keywords'), $data);

            if (!$inserted) {
                $this->log('error', 'keyword', 'Keyword insert mislukt', [
                    'db_error' => $this->db->last_error,
                    'data' => $data,
                ]);
                $this->redirect_with_message('sch-keywords', 'Keyword opslaan mislukt. Check logs.', 'error');
            }

            $id = (int) $this->db->insert_id;
            $created_jobs = $this->create_jobs_for_keyword($id);

            if ($created_jobs < 1) {
                $this->redirect_with_message('sch-keywords', 'Keyword opgeslagen, maar er zijn geen jobs aangemaakt. Check logs.', 'error');
            }

            $this->redirect_with_message('sch-keywords', 'Keyword opgeslagen. Jobs aangemaakt: ' . $created_jobs);
        }
    }

    public function handle_discover_keywords(): void {
        $this->verify_admin_nonce('sch_discover_keywords');

        $client_id = (int) ($_POST['client_id'] ?? 0);
        if ($client_id <= 0) {
            $this->redirect_with_message('sch-clients', 'Geen geldige klant geselecteerd.', 'error');
        }

        $client = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", $client_id));
        if (!$client) {
            $this->redirect_with_message('sch-clients', 'Klant niet gevonden.', 'error');
        }

        try {
            $created = $this->discover_keywords_for_client($client);
            $this->redirect_with_message('sch-clients', 'Keyword discovery afgerond. Nieuwe keywords: ' . $created);
        } catch (Throwable $e) {
            $this->log('error', 'keyword_discovery', 'Keyword discovery mislukt', [
                'client_id' => $client_id,
                'error' => $e->getMessage(),
            ]);
            $this->redirect_with_message('sch-clients', 'Keyword discovery mislukt. Check logs.', 'error');
        }
    }

    public function handle_gsc_connect(): void {
        $client_id = (int) ($_GET['client_id'] ?? 0);
        if ($client_id <= 0) {
            $this->redirect_with_message('sch-clients', 'Geen geldige klant geselecteerd.', 'error');
        }
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer('sch_gsc_connect_' . $client_id);

        if (!$this->is_gsc_integration_enabled()) {
            $this->redirect_with_message('sch-clients', 'Google Search Console integratie staat uit.', 'error');
        }

        $oauth_client_id = (string) get_option(self::OPTION_GSC_CLIENT_ID, '');
        $oauth_client_secret = (string) get_option(self::OPTION_GSC_CLIENT_SECRET, '');
        if ($oauth_client_id === '' || $oauth_client_secret === '') {
            $this->redirect_with_message('sch-settings', 'Vul eerst Google OAuth client ID en secret in.', 'error');
        }

        $state_token = wp_generate_password(32, false, false);
        set_transient('sch_gsc_state_' . $state_token, [
            'client_id' => $client_id,
            'user_id' => get_current_user_id(),
        ], MINUTE_IN_SECONDS * 15);

        $this->log('info', 'gsc_oauth', 'OAuth connect gestart', [
            'client_id' => $client_id,
            'user_id' => get_current_user_id(),
        ]);

        $auth_url = add_query_arg([
            'client_id' => $oauth_client_id,
            'redirect_uri' => $this->gsc_oauth_redirect_uri(),
            'response_type' => 'code',
            'scope' => 'https://www.googleapis.com/auth/webmasters.readonly https://www.googleapis.com/auth/userinfo.email',
            'access_type' => 'offline',
            'include_granted_scopes' => 'true',
            'prompt' => 'consent',
            'state' => $state_token,
        ], 'https://accounts.google.com/o/oauth2/v2/auth');

        $this->log('info', 'gsc_oauth', 'OAuth redirect naar Google', [
            'client_id' => $client_id,
            'user_id' => get_current_user_id(),
            'redirect_uri' => $this->gsc_oauth_redirect_uri(),
            'auth_url' => $auth_url,
        ]);

        wp_redirect($auth_url);
        exit;
    }

    public function handle_gsc_oauth_callback(): void {
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }

        $state = sanitize_text_field((string) ($_GET['state'] ?? ''));
        $code = sanitize_text_field((string) ($_GET['code'] ?? ''));
        $error = sanitize_text_field((string) ($_GET['error'] ?? ''));

        $this->log('info', 'gsc_oauth', 'OAuth callback ontvangen', [
            'state_present' => $state !== '',
            'code_present' => $code !== '',
            'error' => $error,
            'user_id' => get_current_user_id(),
        ]);

        $state_payload = get_transient('sch_gsc_state_' . $state);
        delete_transient('sch_gsc_state_' . $state);

        if (!is_array($state_payload) || empty($state_payload['client_id']) || (int) ($state_payload['user_id'] ?? 0) !== get_current_user_id()) {
            $this->redirect_with_message('sch-clients', 'OAuth state validatie mislukt.', 'error');
        }

        $client_id = (int) $state_payload['client_id'];
        if ($error !== '') {
            $this->log('error', 'gsc_oauth', 'OAuth connect mislukt', [
                'client_id' => $client_id,
                'error' => $error,
            ]);
            $this->redirect_with_message('sch-clients', 'Google autorisatie geannuleerd of mislukt.', 'error');
        }

        if ($code === '') {
            $this->redirect_with_message('sch-clients', 'Geen OAuth code ontvangen.', 'error');
        }

        try {
            $token = $this->gsc_exchange_code_for_token($code);
            $email = $this->gsc_fetch_account_email((string) ($token['access_token'] ?? ''));

            $token_payload = [
                'access_token' => (string) ($token['access_token'] ?? ''),
                'refresh_token' => (string) ($token['refresh_token'] ?? ''),
                'token_type' => (string) ($token['token_type'] ?? 'Bearer'),
                'scope' => (string) ($token['scope'] ?? ''),
            ];

            $expires_at = $this->gsc_expiry_time_from_token_response($token);

            $this->db->update($this->table('clients'), [
                'gsc_token_data' => $this->encrypt_sensitive_value(wp_json_encode($token_payload)),
                'gsc_token_expires_at' => $expires_at,
                'gsc_connected_email' => $email,
                'updated_at' => $this->now(),
            ], ['id' => $client_id]);

            $this->log('info', 'gsc_oauth', 'OAuth connect geslaagd', [
                'client_id' => $client_id,
                'email' => $email,
            ]);
            $this->redirect_with_message('sch-clients', 'Google Search Console gekoppeld.');
        } catch (Throwable $e) {
            $this->log('error', 'gsc_oauth', 'OAuth connect mislukt', [
                'client_id' => $client_id,
                'error' => $e->getMessage(),
            ]);
            $this->redirect_with_message('sch-clients', 'Google koppeling mislukt. Check logs.', 'error');
        }
    }

    public function handle_gsc_disconnect(): void {
        $client_id = (int) ($_GET['client_id'] ?? 0);
        if ($client_id <= 0) {
            $this->redirect_with_message('sch-clients', 'Geen geldige klant geselecteerd.', 'error');
        }
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer('sch_gsc_disconnect_' . $client_id);

        $this->db->update($this->table('clients'), [
            'gsc_token_data' => null,
            'gsc_token_expires_at' => null,
            'gsc_connected_email' => '',
            'gsc_property' => '',
            'updated_at' => $this->now(),
        ], ['id' => $client_id]);

        $this->log('info', 'gsc_oauth', 'Google Search Console koppeling verbroken', ['client_id' => $client_id]);
        $this->redirect_with_message('sch-clients', 'Google Search Console koppeling verbroken.');
    }

    public function handle_gsc_fetch_properties(): void {
        $client_id = (int) ($_GET['client_id'] ?? 0);
        if ($client_id <= 0) {
            $this->redirect_with_message('sch-clients', 'Geen geldige klant geselecteerd.', 'error');
        }
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer('sch_gsc_fetch_properties_' . $client_id);

        $client = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", $client_id));
        if (!$client) {
            $this->redirect_with_message('sch-clients', 'Klant niet gevonden.', 'error');
        }

        try {
            $properties = $this->gsc_list_properties_for_client($client);
            set_transient($this->gsc_properties_cache_key($client_id), $properties, HOUR_IN_SECONDS);
            $this->log('info', 'gsc_property', 'Property lijst opgehaald', [
                'client_id' => $client_id,
                'count' => count($properties),
            ]);
            $this->redirect_with_message('sch-clients', 'Properties opgehaald: ' . count($properties));
        } catch (Throwable $e) {
            $this->log('error', 'gsc_property', 'Property lijst ophalen mislukt', [
                'client_id' => $client_id,
                'error' => $e->getMessage(),
            ]);
            $this->redirect_with_message('sch-clients', 'Properties ophalen mislukt. Check logs.', 'error');
        }
    }

    public function handle_gsc_save_property(): void {
        $this->verify_admin_nonce('sch_gsc_save_property');
        $client_id = (int) ($_POST['client_id'] ?? 0);
        $property = sanitize_text_field((string) ($_POST['gsc_property'] ?? ''));
        if ($client_id <= 0 || $property === '') {
            $this->redirect_with_message('sch-clients', 'Klant en property zijn verplicht.', 'error');
        }

        $this->db->update($this->table('clients'), [
            'gsc_property' => $property,
            'updated_at' => $this->now(),
        ], ['id' => $client_id]);

        $this->log('info', 'gsc_property', 'Property gekoppeld aan klant', [
            'client_id' => $client_id,
            'property' => $property,
        ]);
        $this->redirect_with_message('sch-clients', 'Property gekoppeld aan klant.');
    }

    public function handle_gsc_sync_keywords(): void {
        $this->verify_admin_nonce('sch_gsc_sync_keywords');
        $client_id = (int) ($_POST['client_id'] ?? 0);
        $range_days = $this->sanitize_gsc_range_days((int) ($_POST['range_days'] ?? (int) get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28')));
        $row_limit = max(1, min(25000, (int) ($_POST['row_limit'] ?? (int) get_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, '250'))));
        $top_n_clicks = $this->sanitize_gsc_top_n_clicks((int) ($_POST['top_n_clicks'] ?? (int) get_option(self::OPTION_GSC_DEFAULT_TOP_N_CLICKS, '0')));
        $min_impressions = $this->sanitize_gsc_min_impressions((int) ($_POST['min_impressions'] ?? (int) get_option(self::OPTION_GSC_DEFAULT_MIN_IMPRESSIONS, '0')));

        if ($client_id <= 0) {
            $this->redirect_with_message('sch-clients', 'Geen geldige klant geselecteerd.', 'error');
        }

        $client = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", $client_id));
        if (!$client) {
            $this->redirect_with_message('sch-clients', 'Klant niet gevonden.', 'error');
        }

        try {
            $result = $this->sync_gsc_keywords_for_client($client, $range_days, $row_limit, $top_n_clicks, $min_impressions);
            $this->redirect_with_message('sch-clients', sprintf('GSC sync klaar. Rows: %d, inserts: %d, updates: %d', $result['rows'], $result['inserted'], $result['updated']));
        } catch (Throwable $e) {
            $this->log('error', 'gsc_sync', 'Keyword sync mislukt', [
                'client_id' => $client_id,
                'error' => $e->getMessage(),
            ]);
            $this->redirect_with_message('sch-clients', 'GSC keyword sync mislukt. Check logs.', 'error');
        }
    }

    public function handle_save_settings(): void {
        $this->verify_admin_nonce('sch_save_settings');

        update_option(self::OPTION_OPENAI_API_KEY, sanitize_text_field((string) ($_POST['openai_api_key'] ?? '')));
        update_option(self::OPTION_OPENAI_MODEL, sanitize_text_field((string) ($_POST['openai_model'] ?? 'gpt-5.4-mini')));
        update_option(self::OPTION_UNSPLASH_ACCESS_KEY, sanitize_text_field((string) ($_POST['unsplash_access_key'] ?? '')));

        $temperature = max(0, min(2, (float) ($_POST['openai_temperature'] ?? 0.6)));
        update_option(self::OPTION_OPENAI_TEMPERATURE, (string) $temperature);

        $trusted_source_domain = $this->normalize_site_url((string) ($_POST['trusted_source_domain'] ?? 'https://shortcut.nl'));
        if ($trusted_source_domain === '') {
            $trusted_source_domain = 'https://shortcut.nl';
        }
        update_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, $trusted_source_domain);

        update_option(self::OPTION_ENABLE_FEATURED_IMAGES, isset($_POST['enable_featured_images']) ? '1' : '0');
        update_option(self::OPTION_ENABLE_SUPPORTING, isset($_POST['enable_supporting']) ? '1' : '0');
        update_option(self::OPTION_ENABLE_AUTO_DISCOVERY, isset($_POST['enable_auto_discovery']) ? '1' : '0');
        update_option(self::OPTION_ENABLE_VERBOSE_LOGS, isset($_POST['enable_verbose_logs']) ? '1' : '0');

        $max_research_pages = max(1, min(20, (int) ($_POST['max_research_pages'] ?? 5)));
        $max_discovery_keywords = max(1, min(50, (int) ($_POST['max_discovery_keywords'] ?? 10)));
        update_option(self::OPTION_MAX_RESEARCH_PAGES, (string) $max_research_pages);
        update_option(self::OPTION_MAX_DISCOVERY_KEYWORDS, (string) $max_discovery_keywords);
        update_option(self::OPTION_GSC_ENABLED, isset($_POST['gsc_enabled']) ? '1' : '0');
        update_option(self::OPTION_GSC_CLIENT_ID, sanitize_text_field((string) ($_POST['gsc_client_id'] ?? '')));
        update_option(self::OPTION_GSC_CLIENT_SECRET, sanitize_text_field((string) ($_POST['gsc_client_secret'] ?? '')));
        update_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, (string) $this->sanitize_gsc_range_days((int) ($_POST['gsc_default_sync_range'] ?? 28)));
        update_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, (string) max(1, min(25000, (int) ($_POST['gsc_default_row_limit'] ?? 250))));
        update_option(self::OPTION_GSC_DEFAULT_TOP_N_CLICKS, (string) $this->sanitize_gsc_top_n_clicks((int) ($_POST['gsc_default_top_n_clicks'] ?? 0)));
        update_option(self::OPTION_GSC_DEFAULT_MIN_IMPRESSIONS, (string) $this->sanitize_gsc_min_impressions((int) ($_POST['gsc_default_min_impressions'] ?? 0)));
        update_option(self::OPTION_GSC_AUTO_SYNC, isset($_POST['gsc_auto_sync']) ? '1' : '0');

        update_option(self::OPTION_RANDOM_MACHINE_ENABLED, isset($_POST['random_machine_enabled']) ? '1' : '0');
        update_option(self::OPTION_RANDOM_DAILY_MAX, (string) max(1, min(100, (int) ($_POST['random_daily_max'] ?? 10))));

        $random_status = sanitize_key((string) ($_POST['random_status'] ?? 'draft'));
        if (!in_array($random_status, ['draft', 'publish'], true)) {
            $random_status = 'draft';
        }
        update_option(self::OPTION_RANDOM_STATUS, $random_status);

        $random_min_words = max(400, min(5000, (int) ($_POST['random_min_words'] ?? 900)));
        $random_max_words = max(500, min(6000, (int) ($_POST['random_max_words'] ?? 1400)));
        if ($random_max_words < $random_min_words) {
            $random_max_words = $random_min_words;
        }

        update_option(self::OPTION_RANDOM_MIN_WORDS, (string) $random_min_words);
        update_option(self::OPTION_RANDOM_MAX_WORDS, (string) $random_max_words);
        update_option(self::OPTION_RANDOM_MAX_PER_SITE_PER_DAY, (string) max(1, min(20, (int) ($_POST['random_max_per_site_per_day'] ?? 2))));
        update_option(self::OPTION_RANDOM_ONLY_ACTIVE_SITES, isset($_POST['random_only_active_sites']) ? '1' : '0');

        $random_allowed_categories = $this->sanitize_blog_categories((array) ($_POST['random_allowed_categories'] ?? []));
        update_option(self::OPTION_RANDOM_ALLOWED_CATEGORIES, wp_json_encode($random_allowed_categories));
        update_option(self::OPTION_RANDOM_DUPLICATE_WINDOW_DAYS, (string) max(1, min(365, (int) ($_POST['random_duplicate_window_days'] ?? 30))));

        $this->redirect_with_message('sch-settings', 'Instellingen opgeslagen.');
    }

    public function handle_run_now(): void {
        $this->verify_admin_nonce('sch_run_now');
        $this->run_worker();
        $this->redirect_with_message('sch-content-hub', 'Worker uitgevoerd. Check jobs en logs.');
    }

    public function handle_retry_job(): void {
        $this->verify_admin_nonce('sch_retry_job');
        $id = (int) ($_GET['id'] ?? 0);
        if ($id > 0) {
            $updated = $this->db->update($this->table('jobs'), [
                'status'      => 'queued',
                'locked_at'   => null,
                'started_at'  => null,
                'finished_at' => null,
                'updated_at'  => $this->now(),
            ], ['id' => $id]);

            $this->vlog('worker', 'Job opnieuw in queue gezet', [
                'job_id' => $id,
                'updated' => $updated,
                'db_error' => $this->db->last_error,
            ]);
        }
        $this->redirect_with_message('sch-jobs', 'Job opnieuw in queue gezet.');
    }

    public function handle_approve_publish(): void {
        $this->verify_admin_nonce('sch_approve_publish');
        $article_id = (int) ($_REQUEST['article_id'] ?? 0);
        if ($article_id <= 0) {
            $this->redirect_with_message('sch-editorial', 'Artikel niet gevonden.', 'error');
        }
        $site_id = $this->resolve_publish_site_id($article_id, $_REQUEST['site_id'] ?? null);

        try {
            $this->approve_and_publish_article($article_id, $site_id);
            $this->redirect_with_message('sch-editorial', 'Artikel gepubliceerd.');
        } catch (Throwable $e) {
            $this->log('error', 'editorial', 'Publiceren na redactionele approval mislukt', [
                'article_id' => $article_id,
                'site_id' => $site_id,
                'error' => $e->getMessage(),
            ]);
            $this->redirect_with_message('sch-editorial', 'Publiceren mislukt. Check logs.', 'error');
        }
    }

    public function handle_bulk_approve_publish(): void {
        $this->verify_admin_nonce('sch_bulk_approve_publish');
        $article_ids = array_map('intval', (array) ($_POST['article_ids'] ?? []));
        $article_ids = array_values(array_filter($article_ids, static function (int $id): bool {
            return $id > 0;
        }));

        if (!$article_ids) {
            $this->redirect_with_message('sch-editorial', 'Geen artikelen geselecteerd.', 'error');
        }

        $single_publish_article_id = (int) ($_POST['publish_now_article_id'] ?? 0);
        if ($single_publish_article_id > 0) {
            $site_id = $this->resolve_publish_site_id($single_publish_article_id, (array) ($_POST['article_sites'] ?? []));
            try {
                $this->approve_and_publish_article($single_publish_article_id, $site_id);
                $this->redirect_with_message('sch-editorial', 'Artikel gepubliceerd.');
            } catch (Throwable $e) {
                $this->log('error', 'editorial', 'Publiceren na redactionele approval mislukt', [
                    'article_id' => $single_publish_article_id,
                    'site_id' => $site_id,
                    'error' => $e->getMessage(),
                ]);
                $this->redirect_with_message('sch-editorial', 'Publiceren mislukt. Check logs.', 'error');
            }
        }

        $published = 0;
        $failed = 0;
        foreach ($article_ids as $article_id) {
            $site_id = $this->resolve_publish_site_id($article_id, (array) ($_POST['article_sites'] ?? []));
            try {
                $this->approve_and_publish_article($article_id, $site_id);
                $published++;
            } catch (Throwable $e) {
                $failed++;
                $this->log('error', 'editorial', 'Bulk publiceren mislukt voor artikel', [
                    'article_id' => $article_id,
                    'site_id' => $site_id,
                    'error' => $e->getMessage(),
                ]);
            }
        }

        if ($failed > 0) {
            $this->redirect_with_message('sch-editorial', "Klaar. Gepubliceerd: {$published}, mislukt: {$failed}.", 'error');
        }

        $this->redirect_with_message('sch-editorial', "Klaar. Gepubliceerd: {$published}.");
    }

    public function handle_delete_client(): void {
        $this->verify_admin_nonce('sch_delete_client');
        $id = (int) ($_GET['id'] ?? 0);
        if ($id > 0) {
            $this->db->delete($this->table('clients'), ['id' => $id]);
            $this->vlog('client', 'Klant verwijderd', ['client_id' => $id]);
        }
        $this->redirect_with_message('sch-clients', 'Klant verwijderd.');
    }

    public function handle_delete_site(): void {
        $this->verify_admin_nonce('sch_delete_site');
        $id = (int) ($_GET['id'] ?? 0);
        if ($id > 0) {
            $this->db->delete($this->table('sites'), ['id' => $id]);
            $this->vlog('site', 'Blog verwijderd', ['site_id' => $id]);
        }
        $this->redirect_with_message('sch-sites', 'Blog verwijderd.');
    }

    public function handle_delete_keyword(): void {
        $this->verify_admin_nonce('sch_delete_keyword');
        $id = (int) ($_GET['id'] ?? 0);
        if ($id > 0) {
            $this->delete_jobs_for_keyword($id);
            $this->db->delete($this->table('keywords'), ['id' => $id]);
            $this->vlog('keyword', 'Keyword verwijderd', ['keyword_id' => $id]);
        }
        $this->redirect_with_message('sch-keywords', 'Keyword verwijderd.');
    }

    private function delete_jobs_for_keyword(int $keyword_id): void {
        $deleted = $this->db->query($this->db->prepare("DELETE FROM {$this->table('jobs')} WHERE keyword_id=%d", $keyword_id));
        $this->vlog('jobs', 'Jobs verwijderd voor keyword', [
            'keyword_id' => $keyword_id,
            'deleted' => $deleted,
            'db_error' => $this->db->last_error,
        ]);
    }

    private function create_jobs_for_keyword(int $keyword_id): int {
        $keyword = $this->db->get_row($this->db->prepare(
            "SELECT * FROM {$this->table('keywords')} WHERE id=%d",
            $keyword_id
        ));

        if (!$keyword) {
            $this->log('error', 'jobs', 'Geen jobs aangemaakt: keyword niet gevonden', [
                'keyword_id' => $keyword_id,
            ]);
            return 0;
        }

        $site_ids = $this->decode_json_array($keyword->target_site_ids);
        $site_ids = array_values(array_unique(array_filter(array_map('intval', (array) $site_ids))));
        $target_site_categories = $this->sanitize_blog_categories($this->decode_json_array($keyword->target_site_categories));

        if (empty($site_ids)) {
            $this->log('error', 'jobs', 'Geen jobs aangemaakt: target_site_ids is leeg', [
                'keyword_id' => $keyword_id,
                'raw_target_site_ids' => $keyword->target_site_ids,
            ]);
            return 0;
        }

        $created = 0;
        $client_max_posts_per_month = (int) $this->db->get_var($this->db->prepare(
            "SELECT max_posts_per_month FROM {$this->table('clients')} WHERE id=%d",
            (int) $keyword->client_id
        ));
        $monthly_budget = $this->get_client_monthly_creation_budget((int) $keyword->client_id, $client_max_posts_per_month);

        if ($monthly_budget['remaining'] <= 0) {
            $this->log('info', 'jobs', 'Geen jobs aangemaakt: maandlimiet bereikt voor huidige dagverdeling', [
                'keyword_id' => $keyword_id,
                'client_id' => (int) $keyword->client_id,
                'max_posts_per_month' => $client_max_posts_per_month,
                'budget' => $monthly_budget,
            ]);
            return 0;
        }

        $conflicts = $this->find_cannibalization_conflicts($keyword);
        if (!empty($conflicts)) {
            $suggestion = 'Consolideer dit onderwerp met bestaand artikel en voeg waar nodig een interne link toe in plaats van een nieuw artikel.';
            $blocked_count = 0;

            foreach ($site_ids as $site_id) {
                $site_exists = (int) $this->db->get_var($this->db->prepare(
                    "SELECT COUNT(*) FROM {$this->table('sites')} WHERE id=%d AND is_active=1",
                    $site_id
                ));

                if ($site_exists < 1) {
                    continue;
                }

                if (!empty($target_site_categories)) {
                    $site_category = (string) $this->db->get_var($this->db->prepare(
                        "SELECT default_category FROM {$this->table('sites')} WHERE id=%d LIMIT 1",
                        $site_id
                    ));
                    $site_category = $this->sanitize_blog_category($site_category);

                    if (!in_array($site_category, $target_site_categories, true)) {
                        continue;
                    }
                }

                $inserted = $this->db->insert($this->table('jobs'), [
                    'keyword_id' => (int) $keyword->id,
                    'client_id'  => (int) $keyword->client_id,
                    'site_id'    => (int) $site_id,
                    'job_type'   => 'write_publish',
                    'status'     => 'blocked_cannibalization',
                    'attempts'   => 0,
                    'payload'    => wp_json_encode([
                        'reason' => 'cannibalization_detected',
                        'conflicts' => $conflicts,
                        'suggestion' => $suggestion,
                    ]),
                    'result'     => wp_json_encode([
                        'blocked' => true,
                        'reason' => 'cannibalization_detected',
                    ]),
                    'finished_at' => $this->now(),
                    'created_at' => $this->now(),
                    'updated_at' => $this->now(),
                ]);

                if ($inserted) {
                    $blocked_count++;
                }
            }

            $this->log('warning', 'jobs', 'Job-aanmaak geblokkeerd wegens keyword cannibalisatie', [
                'keyword_id' => $keyword_id,
                'client_id' => (int) $keyword->client_id,
                'blocked_jobs' => $blocked_count,
                'conflicts' => $conflicts,
                'suggestion' => $suggestion,
            ]);

            return $blocked_count;
        }

        foreach ($site_ids as $site_id) {
            if ($created >= (int) $monthly_budget['remaining']) {
                break;
            }

            $site_exists = (int) $this->db->get_var($this->db->prepare(
                "SELECT COUNT(*) FROM {$this->table('sites')} WHERE id=%d AND is_active=1",
                $site_id
            ));

            if ($site_exists < 1) {
                $this->log('warning', 'jobs', 'Site overgeslagen bij job-aanmaak: site bestaat niet of is inactief', [
                    'keyword_id' => $keyword_id,
                    'site_id' => $site_id,
                ]);
                continue;
            }

            if (!empty($target_site_categories)) {
                $site_category = (string) $this->db->get_var($this->db->prepare(
                    "SELECT default_category FROM {$this->table('sites')} WHERE id=%d LIMIT 1",
                    $site_id
                ));
                $site_category = $this->sanitize_blog_category($site_category);

                if (!in_array($site_category, $target_site_categories, true)) {
                    $this->vlog('jobs', 'Site overgeslagen bij job-aanmaak: valt buiten categorie-filter', [
                        'keyword_id' => $keyword_id,
                        'site_id' => $site_id,
                        'site_category' => $site_category,
                        'allowed_categories' => $target_site_categories,
                    ]);
                    continue;
                }
            }

            $inserted = $this->db->insert($this->table('jobs'), [
                'keyword_id' => (int) $keyword->id,
                'client_id'  => (int) $keyword->client_id,
                'site_id'    => (int) $site_id,
                'job_type'   => 'write_publish',
                'status'     => 'queued',
                'attempts'   => 0,
                'created_at' => $this->now(),
                'updated_at' => $this->now(),
            ]);

            if ($inserted) {
                $created++;
                $this->vlog('jobs', 'Job aangemaakt', [
                    'keyword_id' => $keyword_id,
                    'site_id' => $site_id,
                    'job_id' => (int) $this->db->insert_id,
                ]);
            } else {
                $this->log('error', 'jobs', 'Job insert mislukt', [
                    'keyword_id' => $keyword_id,
                    'site_id' => $site_id,
                    'db_error' => $this->db->last_error,
                ]);
            }
        }

        $this->log('info', 'jobs', 'Job-aanmaak afgerond', [
            'keyword_id' => $keyword_id,
            'requested_site_ids' => $site_ids,
            'target_site_categories' => $target_site_categories,
            'created_jobs' => $created,
            'client_max_posts_per_month' => $client_max_posts_per_month,
            'monthly_budget' => $monthly_budget,
        ]);

        return $created;
    }

    private function find_cannibalization_conflicts(object $keyword): array {
        $candidate_terms = $this->keyword_terms_from_values(
            (string) $keyword->main_keyword,
            $this->decode_json_array($keyword->secondary_keywords)
        );
        if (empty($candidate_terms)) {
            return [];
        }

        $rows = $this->db->get_results($this->db->prepare(
            "SELECT
                a.id AS article_id,
                a.title AS article_title,
                a.remote_url,
                a.created_at,
                k.main_keyword AS existing_main_keyword,
                k.secondary_keywords AS existing_secondary_keywords
            FROM {$this->table('articles')} a
            INNER JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
            WHERE a.client_id = %d
              AND a.keyword_id <> %d
            ORDER BY a.created_at DESC",
            (int) $keyword->client_id,
            (int) $keyword->id
        ));

        if (!$rows) {
            return [];
        }

        $conflicts = [];
        foreach ($rows as $row) {
            $existing_terms = $this->keyword_terms_from_values(
                (string) $row->existing_main_keyword,
                $this->decode_json_array($row->existing_secondary_keywords)
            );
            if (empty($existing_terms)) {
                continue;
            }

            $matched = array_values(array_intersect($candidate_terms, $existing_terms));
            if (empty($matched)) {
                continue;
            }

            $conflicts[] = [
                'article_id' => (int) $row->article_id,
                'article_title' => sanitize_text_field((string) $row->article_title),
                'article_url' => esc_url_raw((string) $row->remote_url),
                'article_created_at' => (string) $row->created_at,
                'existing_keyword' => sanitize_text_field((string) $row->existing_main_keyword),
                'matched_terms' => array_values(array_map('sanitize_text_field', $matched)),
            ];
        }

        return $conflicts;
    }

    private function keyword_terms_from_values(string $main_keyword, array $secondary_keywords): array {
        $terms = [];
        $main = $this->normalize_keyword_term($main_keyword);
        if ($main !== '') {
            $terms[] = $main;
        }

        foreach ($secondary_keywords as $term) {
            $normalized = $this->normalize_keyword_term((string) $term);
            if ($normalized !== '') {
                $terms[] = $normalized;
            }
        }

        return array_values(array_unique($terms));
    }

    private function normalize_keyword_term(string $term): string {
        $term = strtolower(sanitize_text_field($term));
        $term = preg_replace('/\s+/', ' ', trim($term));
        return is_string($term) ? $term : '';
    }

    private function log(string $level, string $context, string $message, $payload = null): void {
        $this->db->insert($this->table('logs'), [
            'level'      => $level,
            'context'    => $context,
            'message'    => $message,
            'payload'    => $payload !== null ? wp_json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : null,
            'created_at' => $this->now(),
        ]);
    }

    public function run_worker(): void {
        $this->log('info', 'worker', 'run_worker gestart');
        $this->maybe_prepare_random_content_jobs();

        $job = $this->db->get_row("SELECT * FROM {$this->table('jobs')} WHERE status='queued' ORDER BY id ASC LIMIT 1");

        if (!$job) {
            $this->log('info', 'worker', 'Geen queued job gevonden');

            if (get_option(self::OPTION_ENABLE_AUTO_DISCOVERY, '0') === '1') {
                $this->vlog('worker', 'Auto discovery staat aan, client scan start');
                $this->maybe_run_auto_discovery();
            }
            return;
        }

        $this->log('info', 'worker', 'Queued job gevonden', [
            'job_id' => (int) $job->id,
            'keyword_id' => (int) $job->keyword_id,
            'site_id' => (int) $job->site_id,
            'attempts' => (int) $job->attempts,
        ]);

        $updated = $this->db->update($this->table('jobs'), [
            'status'     => 'running',
            'attempts'   => (int) $job->attempts + 1,
            'locked_at'  => $this->now(),
            'started_at' => $this->now(),
            'updated_at' => $this->now(),
        ], ['id' => (int) $job->id]);

        if ($updated === false) {
            $this->log('error', 'worker', 'Job kon niet op running gezet worden', [
                'job_id' => (int) $job->id,
                'db_error' => $this->db->last_error,
            ]);
            return;
        }

        try {
            $this->process_job((int) $job->id);
            $this->log('info', 'worker', 'Job succesvol verwerkt', [
                'job_id' => (int) $job->id,
            ]);
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), 'ANCHOR_BLOCK:') === 0) {
                $this->db->update($this->table('jobs'), [
                    'status'      => 'blocked',
                    'result'      => wp_json_encode(['error' => $e->getMessage()]),
                    'finished_at' => $this->now(),
                    'updated_at'  => $this->now(),
                ], ['id' => (int) $job->id]);

                $this->log('warning', 'worker', 'Job geblokkeerd door anchor policy', [
                    'job_id' => (int) $job->id,
                    'error'  => $e->getMessage(),
                ]);
                return;
            }

            $this->db->update($this->table('jobs'), [
                'status'      => 'failed',
                'result'      => wp_json_encode(['error' => $e->getMessage()]),
                'finished_at' => $this->now(),
                'updated_at'  => $this->now(),
            ], ['id' => (int) $job->id]);

            $this->log('error', 'worker', 'Job failed', [
                'job_id' => (int) $job->id,
                'error'  => $e->getMessage(),
            ]);
        }
    }


    private function maybe_prepare_random_content_jobs(): void {
        if (get_option(self::OPTION_RANDOM_MACHINE_ENABLED, '0') !== '1') {
            return;
        }

        $daily_max = max(1, (int) get_option(self::OPTION_RANDOM_DAILY_MAX, '10'));
        $already_today = (int) $this->db->get_var($this->db->prepare(
            "SELECT COUNT(*) FROM {$this->table('jobs')} WHERE job_type=%s AND DATE(created_at)=CURDATE()",
            'random_fresh_content'
        ));

        $remaining = max(0, $daily_max - $already_today);

        $this->log('info', 'random_machine', 'Random content machine gestart', [
            'daily_max' => $daily_max,
            'already_today' => $already_today,
            'remaining' => $remaining,
        ]);

        if ($remaining <= 0) {
            $this->log('info', 'random_machine', 'Daily cap bereikt', [
                'daily_max' => $daily_max,
                'already_today' => $already_today,
            ]);
            return;
        }

        $sites = $this->get_random_machine_candidate_sites();
        if (!$sites) {
            $this->log('warning', 'random_machine', 'Geen geschikte sites gevonden voor random machine');
            return;
        }

        $duplicate_window_days = max(1, (int) get_option(self::OPTION_RANDOM_DUPLICATE_WINDOW_DAYS, '30'));
        $created = 0;
        $attempts = 0;
        $max_attempts = max(6, $remaining * 6);

        while ($created < $remaining && $attempts < $max_attempts) {
            $attempts++;
            $site = $this->pick_random_machine_site($sites);
            if (!$site) {
                break;
            }

            $site_today_count = $this->count_random_jobs_for_site_today((int) $site->id);
            $site_daily_max = max(1, (int) get_option(self::OPTION_RANDOM_MAX_PER_SITE_PER_DAY, '2'));
            if ($site_today_count >= $site_daily_max) {
                $this->vlog('random_machine', 'Site overgeslagen door per-site daglimiet', [
                    'site_id' => (int) $site->id,
                    'site_today_count' => $site_today_count,
                    'site_daily_max' => $site_daily_max,
                ]);
                continue;
            }

            try {
                $research = $this->research_random_topic_for_site($site, $duplicate_window_days);
            } catch (Throwable $e) {
                $this->log('warning', 'random_machine', 'Research stap mislukt voor site', [
                    'site_id' => (int) $site->id,
                    'error' => $e->getMessage(),
                ]);
                continue;
            }

            if (!$research) {
                continue;
            }

            $job_id = $this->create_random_machine_job($site, $research);
            if ($job_id > 0) {
                $created++;
            }
        }

        $this->log('info', 'random_machine', 'Random content machine afgerond', [
            'created_jobs' => $created,
            'attempts' => $attempts,
            'remaining_slots_at_start' => $remaining,
        ]);
    }

    private function get_random_machine_candidate_sites(): array {
        $only_active = get_option(self::OPTION_RANDOM_ONLY_ACTIVE_SITES, '1') === '1';
        $allowed_categories = $this->sanitize_blog_categories($this->decode_json_array((string) get_option(self::OPTION_RANDOM_ALLOWED_CATEGORIES, wp_json_encode([]))));

        $where = [];
        if ($only_active) {
            $where[] = 'is_active=1';
        }
        if (!empty($allowed_categories)) {
            $in = implode(', ', array_fill(0, count($allowed_categories), '%s'));
            $where[] = "default_category IN ({$in})";
        }

        $sql = "SELECT * FROM {$this->table('sites')}";
        if ($where) {
            $sql .= ' WHERE ' . implode(' AND ', $where);
        }
        $sql .= ' ORDER BY publish_priority ASC, id ASC';

        if (!empty($allowed_categories)) {
            return (array) $this->db->get_results($this->db->prepare($sql, ...$allowed_categories));
        }

        return (array) $this->db->get_results($sql);
    }

    private function pick_random_machine_site(array $sites): ?object {
        if (!$sites) {
            return null;
        }

        usort($sites, function ($a, $b) {
            $a_count = $this->count_random_jobs_for_site_today((int) $a->id);
            $b_count = $this->count_random_jobs_for_site_today((int) $b->id);
            if ($a_count === $b_count) {
                return ((int) $a->publish_priority <=> (int) $b->publish_priority);
            }
            return $a_count <=> $b_count;
        });

        return $sites[0] ?? null;
    }

    private function count_random_jobs_for_site_today(int $site_id): int {
        return (int) $this->db->get_var($this->db->prepare(
            "SELECT COUNT(*) FROM {$this->table('jobs')} WHERE job_type=%s AND site_id=%d AND DATE(created_at)=CURDATE()",
            'random_fresh_content',
            $site_id
        ));
    }

    private function research_random_topic_for_site(object $site, int $duplicate_window_days): array {
        $this->log('info', 'random_machine', 'OpenAI research gestart', [
            'site_id' => (int) $site->id,
            'site_name' => (string) $site->name,
        ]);

        $site_page = [];
        try {
            $site_page = $this->fetch_and_extract_page((string) $site->base_url);
        } catch (Throwable $e) {
            $this->vlog('random_machine', 'Site homepage analyse overgeslagen', [
                'site_id' => (int) $site->id,
                'error' => $e->getMessage(),
            ]);
        }

        $recent_rows = $this->db->get_results($this->db->prepare(
            "SELECT a.title, a.meta_description, a.created_at, k.main_keyword
             FROM {$this->table('articles')} a
             LEFT JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
             WHERE a.site_id=%d
             ORDER BY a.id DESC
             LIMIT 14",
            (int) $site->id
        ));

        $recent_topics = [];
        foreach ((array) $recent_rows as $row) {
            $recent_topics[] = [
                'title' => sanitize_text_field((string) ($row->title ?? '')),
                'main_keyword' => sanitize_text_field((string) ($row->main_keyword ?? '')),
                'created_at' => sanitize_text_field((string) ($row->created_at ?? '')),
            ];
        }

        $result = $this->openai_json_call(
            'random_topic_research',
            [
                'role' => 'Je bent een Nederlandse content researcher voor blogs.',
                'goal' => 'Bepaal één vers, niche-passend onderwerp met keywordset en intent voor een linkloos artikel. Geef alleen JSON terug.',
            ],
            [
                'site' => [
                    'id' => (int) $site->id,
                    'name' => (string) $site->name,
                    'base_url' => (string) $site->base_url,
                    'default_category' => (string) $site->default_category,
                    'homepage_title' => (string) ($site_page['title'] ?? ''),
                    'homepage_text_excerpt' => (string) mb_substr((string) ($site_page['text'] ?? ''), 0, 4000),
                ],
                'recent_topics' => $recent_topics,
                'requirements' => [
                    'language' => 'nl',
                    'must_fit_existing_blog_niche' => true,
                    'no_links_required' => true,
                    'fresh_angle' => true,
                    'secondary_keywords_min' => 3,
                    'secondary_keywords_max' => 8,
                    'avoid_overlap_with_recent_topics' => true,
                ],
            ],
            [
                'type' => 'object',
                'additionalProperties' => false,
                'properties' => [
                    'primary_keyword' => ['type' => 'string'],
                    'secondary_keywords' => ['type' => 'array', 'items' => ['type' => 'string']],
                    'topic_angle' => ['type' => 'string'],
                    'target_category' => ['type' => 'string'],
                    'search_intent' => ['type' => 'string'],
                    'proposed_title' => ['type' => 'string'],
                ],
                'required' => ['primary_keyword', 'secondary_keywords', 'topic_angle', 'target_category', 'search_intent', 'proposed_title'],
            ]
        );

        $primary_keyword = sanitize_text_field((string) ($result['primary_keyword'] ?? ''));
        if ($primary_keyword === '') {
            throw new RuntimeException('Research gaf geen primary keyword terug.');
        }

        $secondary_keywords = array_values(array_filter(array_map('sanitize_text_field', (array) ($result['secondary_keywords'] ?? []))));
        $secondary_keywords = array_slice(array_values(array_unique($secondary_keywords)), 0, 8);
        if (count($secondary_keywords) < 3) {
            throw new RuntimeException('Research gaf te weinig secondary keywords terug.');
        }

        $research = [
            'primary_keyword' => $primary_keyword,
            'secondary_keywords' => $secondary_keywords,
            'topic_angle' => sanitize_text_field((string) ($result['topic_angle'] ?? '')),
            'target_category' => $this->sanitize_blog_category((string) ($result['target_category'] ?? '')),
            'search_intent' => sanitize_text_field((string) ($result['search_intent'] ?? 'informerend')),
            'proposed_title' => sanitize_text_field((string) ($result['proposed_title'] ?? '')),
            'duplicate_fingerprint' => md5(strtolower($primary_keyword . '|' . (string) ($result['topic_angle'] ?? ''))),
        ];

        if ($this->is_random_topic_duplicate($site, $research, $duplicate_window_days)) {
            $this->log('info', 'random_machine', 'Onderwerp afgekeurd wegens overlap', [
                'site_id' => (int) $site->id,
                'primary_keyword' => $research['primary_keyword'],
                'proposed_title' => $research['proposed_title'],
                'window_days' => $duplicate_window_days,
            ]);
            return [];
        }

        $this->log('info', 'random_machine', 'OpenAI research geslaagd', [
            'site_id' => (int) $site->id,
            'primary_keyword' => $research['primary_keyword'],
            'topic_angle' => $research['topic_angle'],
        ]);

        return $research;
    }

    private function is_random_topic_duplicate(object $site, array $research, int $window_days): bool {
        $since = gmdate('Y-m-d H:i:s', time() - DAY_IN_SECONDS * max(1, $window_days));

        $rows = $this->db->get_results($this->db->prepare(
            "SELECT k.main_keyword, k.source_context, a.title
             FROM {$this->table('keywords')} k
             LEFT JOIN {$this->table('articles')} a ON a.keyword_id = k.id
             WHERE k.source='random_machine'
               AND k.created_at >= %s
               AND (a.site_id = %d OR a.site_id IS NULL)",
            $since,
            (int) $site->id
        ));

        $current_fingerprint = (string) ($research['duplicate_fingerprint'] ?? '');
        $current_keyword = strtolower((string) ($research['primary_keyword'] ?? ''));
        $current_title = strtolower((string) ($research['proposed_title'] ?? ''));

        foreach ((array) $rows as $row) {
            $ctx = $this->decode_json_array((string) ($row->source_context ?? ''));
            $existing_fingerprint = strtolower((string) ($ctx['duplicate_fingerprint'] ?? ''));
            $existing_keyword = strtolower((string) ($row->main_keyword ?? ''));
            $existing_title = strtolower((string) ($row->title ?? ''));

            if ($current_fingerprint !== '' && $existing_fingerprint !== '' && $current_fingerprint === $existing_fingerprint) {
                return true;
            }

            if ($current_keyword !== '' && $existing_keyword !== '' && $current_keyword === $existing_keyword) {
                return true;
            }

            if ($current_title !== '' && $existing_title !== '' && similar_text($current_title, $existing_title, $percent) && $percent >= 72) {
                return true;
            }
        }

        return false;
    }

    private function ensure_random_machine_client_id(): int {
        $client = $this->db->get_row($this->db->prepare(
            "SELECT * FROM {$this->table('clients')} WHERE website_url=%s LIMIT 1",
            'https://system.local/random-machine'
        ));

        if ($client) {
            return (int) $client->id;
        }

        $inserted = $this->db->insert($this->table('clients'), [
            'name' => 'Random Content Machine',
            'website_url' => 'https://system.local/random-machine',
            'default_anchor' => 'Random Content Machine',
            'link_targets' => wp_json_encode([]),
            'research_urls' => wp_json_encode([]),
            'max_posts_per_month' => 0,
            'is_active' => 1,
            'created_at' => $this->now(),
            'updated_at' => $this->now(),
        ]);

        if (!$inserted) {
            throw new RuntimeException('System client kon niet worden aangemaakt: ' . $this->db->last_error);
        }

        return (int) $this->db->insert_id;
    }

    private function create_random_machine_job(object $site, array $research): int {
        $client_id = $this->ensure_random_machine_client_id();
        $min_words = max(400, (int) get_option(self::OPTION_RANDOM_MIN_WORDS, '900'));
        $max_words = max($min_words, (int) get_option(self::OPTION_RANDOM_MAX_WORDS, '1400'));
        $target_word_count = wp_rand($min_words, $max_words);

        $secondary_payload = [];
        foreach ((array) ($research['secondary_keywords'] ?? []) as $term) {
            $term = sanitize_text_field((string) $term);
            if ($term !== '') {
                $secondary_payload[] = ['keyword' => $term];
            }
        }

        $keyword_insert = $this->db->insert($this->table('keywords'), [
            'client_id' => $client_id,
            'main_keyword' => sanitize_text_field((string) ($research['primary_keyword'] ?? '')),
            'secondary_keywords' => wp_json_encode($secondary_payload),
            'target_site_ids' => wp_json_encode([(int) $site->id]),
            'target_site_categories' => wp_json_encode([]),
            'content_type' => 'random_fresh_content',
            'tone_of_voice' => 'natuurlijk en informatief',
            'target_word_count' => $target_word_count,
            'priority' => 50,
            'status' => 'queued',
            'source' => 'random_machine',
            'source_context' => wp_json_encode($research, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            'created_at' => $this->now(),
            'updated_at' => $this->now(),
        ]);

        if (!$keyword_insert) {
            $this->log('error', 'random_machine', 'Keyword insert mislukt voor random machine', [
                'site_id' => (int) $site->id,
                'db_error' => $this->db->last_error,
            ]);
            return 0;
        }

        $keyword_id = (int) $this->db->insert_id;
        $payload = [
            'random_machine' => true,
            'linkless' => true,
            'publish_status' => (string) get_option(self::OPTION_RANDOM_STATUS, 'draft'),
            'target_category' => (string) ($research['target_category'] ?? ''),
            'research' => $research,
        ];

        $job_insert = $this->db->insert($this->table('jobs'), [
            'keyword_id' => $keyword_id,
            'client_id' => $client_id,
            'site_id' => (int) $site->id,
            'job_type' => 'random_fresh_content',
            'status' => 'queued',
            'attempts' => 0,
            'payload' => wp_json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            'created_at' => $this->now(),
            'updated_at' => $this->now(),
        ]);

        if (!$job_insert) {
            $this->log('error', 'random_machine', 'Job insert mislukt voor random machine', [
                'keyword_id' => $keyword_id,
                'site_id' => (int) $site->id,
                'db_error' => $this->db->last_error,
            ]);
            return 0;
        }

        $job_id = (int) $this->db->insert_id;
        $this->log('info', 'random_machine', 'Random content job aangemaakt', [
            'job_id' => $job_id,
            'keyword_id' => $keyword_id,
            'site_id' => (int) $site->id,
            'primary_keyword' => (string) ($research['primary_keyword'] ?? ''),
        ]);

        return $job_id;
    }

    private function process_random_fresh_content_job(object $job, object $keyword, object $client, object $site): void {
        $payload = $this->decode_json_array((string) ($job->payload ?? ''));
        $research = is_array($payload['research'] ?? null) ? $payload['research'] : [];

        $this->log('info', 'random_machine', 'Random content job verwerking gestart', [
            'job_id' => (int) $job->id,
            'site_id' => (int) $site->id,
            'keyword_id' => (int) $keyword->id,
        ]);

        $article = $this->generate_random_article($site, $keyword, $research);
        $article_id = $this->store_article($job, $keyword, $client, $site, $article, null, false);

        $publish_status = sanitize_key((string) ($payload['publish_status'] ?? get_option(self::OPTION_RANDOM_STATUS, 'draft')));
        if (!in_array($publish_status, ['draft', 'publish'], true)) {
            $publish_status = 'draft';
        }
        $target_category = $this->sanitize_blog_category((string) ($payload['target_category'] ?? ''));

        $publishable_article = [
            'title' => (string) $article['title'],
            'slug' => (string) $article['slug'],
            'content' => (string) $article['content'],
            'meta_title' => (string) $article['meta_title'],
            'meta_description' => (string) $article['meta_description'],
            'canonical_url' => '',
            'article_type' => 'random_fresh_content',
            'backlinks' => [],
        ];

        $publish_result = $this->publish_to_remote_site($site, $publishable_article, null, $job, $keyword, $client, $article_id, $target_category, $publish_status);

        $this->db->update($this->table('articles'), [
            'remote_post_id' => sanitize_text_field((string) ($publish_result['remote_post_id'] ?? '')),
            'remote_url' => esc_url_raw((string) ($publish_result['remote_url'] ?? '')),
            'publish_status' => sanitize_text_field((string) ($publish_result['status'] ?? $publish_status)),
            'updated_at' => $this->now(),
        ], ['id' => $article_id]);

        $this->db->update($this->table('jobs'), [
            'status' => 'published',
            'result' => wp_json_encode($publish_result),
            'finished_at' => $this->now(),
            'updated_at' => $this->now(),
        ], ['id' => (int) $job->id]);

        $this->db->update($this->table('keywords'), [
            'status' => 'processed',
            'last_processed_at' => $this->now(),
            'updated_at' => $this->now(),
        ], ['id' => (int) $keyword->id]);

        $this->log('info', 'random_machine', 'Random artikel gegenereerd en gedistribueerd', [
            'job_id' => (int) $job->id,
            'article_id' => $article_id,
            'site_id' => (int) $site->id,
            'status' => (string) ($publish_result['status'] ?? $publish_status),
        ]);
    }

    private function generate_random_article(object $site, object $keyword, array $research): array {
        $result = $this->openai_json_call(
            'random_writer',
            [
                'role' => 'Je bent een Nederlandse SEO copywriter voor blogs.',
                'goal' => 'Schrijf een sterk, direct publiceerbaar blogartikel zonder links. Geef alleen JSON terug.',
            ],
            [
                'site' => [
                    'name' => (string) $site->name,
                    'base_url' => (string) $site->base_url,
                    'category' => (string) $site->default_category,
                ],
                'topic_research' => $research,
                'main_keyword' => (string) $keyword->main_keyword,
                'secondary_keywords' => $this->get_secondary_keywords_list($keyword),
                'target_word_count' => (int) $keyword->target_word_count,
                'requirements' => [
                    'language' => 'nl',
                    'html_content' => true,
                    'use_h2_h3' => true,
                    'intro_body_conclusion' => true,
                    'no_external_links' => true,
                    'no_internal_links' => true,
                    'no_urls_or_sources' => true,
                    'no_read_more_click_here' => true,
                    'no_markdown_links' => true,
                    'no_html_anchors' => true,
                    'no_h1_in_content' => true,
                    'seo_but_natural' => true,
                    'non_generic_site_specific_angle' => true,
                ],
            ],
            [
                'type' => 'object',
                'additionalProperties' => false,
                'properties' => [
                    'title' => ['type' => 'string'],
                    'slug' => ['type' => 'string'],
                    'content' => ['type' => 'string'],
                    'meta_title' => ['type' => 'string'],
                    'meta_description' => ['type' => 'string'],
                ],
                'required' => ['title', 'slug', 'content', 'meta_title', 'meta_description'],
            ]
        );

        $clean_content = $this->sanitize_linkless_content((string) ($result['content'] ?? ''));

        return [
            'title' => sanitize_text_field((string) ($result['title'] ?? '')),
            'slug' => sanitize_title((string) ($result['slug'] ?? (string) $keyword->main_keyword)),
            'content' => $clean_content,
            'meta_title' => sanitize_text_field((string) ($result['meta_title'] ?? '')),
            'meta_description' => sanitize_textarea_field((string) ($result['meta_description'] ?? '')),
            'canonical_url' => '',
            'article_type' => 'random_fresh_content',
        ];
    }

    private function sanitize_linkless_content(string $content): string {
        $content = preg_replace('/<a\b[^>]*>(.*?)<\/a>/is', '$1', $content);
        $content = preg_replace('~https?://\S+~i', '', (string) $content);
        $content = preg_replace('~www\.\S+~i', '', (string) $content);
        $content = preg_replace('/\blees ook\b|\bklik hier\b|\bbron:\b/iu', '', (string) $content);
        return wp_kses_post((string) $content);
    }

    private function maybe_run_auto_discovery(): void {
        $client = $this->db->get_row("SELECT * FROM {$this->table('clients')} WHERE is_active=1 ORDER BY updated_at ASC LIMIT 1");
        if (!$client) {
            $this->vlog('keyword_discovery', 'Geen actieve client gevonden voor auto discovery');
            return;
        }

        try {
            $created = $this->discover_keywords_for_client($client);
            $this->log('info', 'keyword_discovery', 'Auto discovery afgerond', [
                'client_id' => (int) $client->id,
                'created_keywords' => $created,
            ]);
        } catch (Throwable $e) {
            $this->log('error', 'keyword_discovery', 'Auto discovery mislukt', [
                'client_id' => (int) $client->id,
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function process_job(int $job_id): void {
        $job = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('jobs')} WHERE id=%d", $job_id));
        if (!$job) {
            throw new RuntimeException('Job niet gevonden.');
        }

        $keyword = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('keywords')} WHERE id=%d", (int) $job->keyword_id));
        $client  = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", (int) $job->client_id));
        $site    = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('sites')} WHERE id=%d", (int) $job->site_id));
        if (!$keyword || !$client || !$site) {
            throw new RuntimeException('Keyword, klant of blog ontbreekt.');
        }

        $this->vlog('worker', 'Job context geladen', [
            'job_id' => $job_id,
            'keyword_id' => (int) $keyword->id,
            'client_id' => (int) $client->id,
            'site_id' => (int) $site->id,
            'keyword' => (string) $keyword->main_keyword,
        ]);

        if ((string) $job->job_type === 'random_fresh_content') {
            $this->process_random_fresh_content_job($job, $keyword, $client, $site);
            return;
        }

        $article = $this->generate_article($keyword, $client, $site);

        $featured_image = null;
        if (get_option(self::OPTION_ENABLE_FEATURED_IMAGES, '1') === '1') {
            try {
                $featured_image = $this->generate_featured_image_payload($keyword, $client, $article);
            } catch (Throwable $e) {
                $this->log('warning', 'featured_image', 'Featured image stap overgeslagen', [
                    'job_id' => $job_id,
                    'error'  => $e->getMessage(),
                ]);
            }
        }

        $article_id = $this->store_article($job, $keyword, $client, $site, $article, $featured_image);

        $this->db->update($this->table('jobs'), [
            'status'      => 'awaiting_approval',
            'result'      => wp_json_encode([
                'message' => 'Artikel klaar voor redactionele approval.',
                'article_id' => $article_id,
            ]),
            'finished_at' => $this->now(),
            'updated_at'  => $this->now(),
        ], ['id' => $job_id]);

        $this->db->update($this->table('keywords'), [
            'status'            => 'ready_for_approval',
            'last_processed_at' => $this->now(),
            'updated_at'        => $this->now(),
        ], ['id' => (int) $keyword->id]);

        $this->log('info', 'editorial', 'Artikel wacht op redactionele approval', [
            'job_id' => $job_id,
            'article_id' => $article_id,
            'site_id' => (int) $site->id,
        ]);
    }

    private function resolve_publish_site_id(int $article_id, $site_input): int {
        if (is_array($site_input)) {
            $site_id = (int) ($site_input[$article_id] ?? 0);
        } else {
            $site_id = (int) $site_input;
        }

        if ($site_id <= 0) {
            return 0;
        }

        $exists = (int) $this->db->get_var($this->db->prepare(
            "SELECT COUNT(*) FROM {$this->table('sites')} WHERE id=%d",
            $site_id
        ));
        return $exists > 0 ? $site_id : 0;
    }

    private function approve_and_publish_article(int $article_id, int $target_site_id = 0): void {
        $article = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('articles')} WHERE id=%d", $article_id));
        if (!$article) {
            throw new RuntimeException('Artikel niet gevonden.');
        }

        $job = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('jobs')} WHERE id=%d", (int) $article->job_id));
        $keyword = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('keywords')} WHERE id=%d", (int) $article->keyword_id));
        $client = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('clients')} WHERE id=%d", (int) $article->client_id));
        if ($target_site_id > 0) {
            $article->site_id = $target_site_id;
        }

        $site = $this->db->get_row($this->db->prepare("SELECT * FROM {$this->table('sites')} WHERE id=%d", (int) $article->site_id));

        if (!$job || !$keyword || !$client || !$site) {
            throw new RuntimeException('Publicatiecontext is incompleet.');
        }
        if ($job->status !== 'awaiting_approval') {
            throw new RuntimeException('Job staat niet op awaiting_approval.');
        }

        $publishable_article = [
            'title' => (string) $article->title,
            'slug' => (string) $article->slug,
            'content' => (string) $article->content,
            'meta_title' => (string) $article->meta_title,
            'meta_description' => (string) $article->meta_description,
            'canonical_url' => (string) $article->canonical_url,
            'article_type' => (string) $article->article_type,
            'backlinks' => $this->decode_json_array($article->backlinks_data),
        ];

        $featured_image = null;
        if (!empty($article->featured_image_data)) {
            $decoded_image = json_decode((string) $article->featured_image_data, true);
            if (is_array($decoded_image)) {
                $featured_image = $decoded_image;
            }
        }

        $publish_result = $this->publish_to_remote_site($site, $publishable_article, $featured_image, $job, $keyword, $client, (int) $article->id);

        $this->db->update($this->table('articles'), [
            'site_id' => (int) $site->id,
            'remote_post_id' => sanitize_text_field((string) ($publish_result['remote_post_id'] ?? '')),
            'remote_url'     => esc_url_raw((string) ($publish_result['remote_url'] ?? '')),
            'publish_status' => sanitize_text_field((string) ($publish_result['status'] ?? 'draft')),
            'updated_at'     => $this->now(),
        ], ['id' => (int) $article->id]);

        $this->db->update($this->table('jobs'), [
            'site_id'      => (int) $site->id,
            'status'      => 'published',
            'result'      => wp_json_encode($publish_result),
            'finished_at' => $this->now(),
            'updated_at'  => $this->now(),
        ], ['id' => (int) $job->id]);

        $this->db->update($this->table('keywords'), [
            'status'            => 'processed',
            'last_processed_at' => $this->now(),
            'updated_at'        => $this->now(),
        ], ['id' => (int) $keyword->id]);

        if ($keyword->content_type === 'pillar' && get_option(self::OPTION_ENABLE_SUPPORTING, '1') === '1') {
            $this->maybe_create_supporting_jobs($keyword, $client);
        }
    }

    private function maybe_create_supporting_jobs(object $pillar_keyword, object $client): void {
        $secondary = $this->get_secondary_keywords_list($pillar_keyword);
        if (!$secondary) {
            $this->vlog('supporting', 'Geen secondary keywords gevonden voor supporting jobs', [
                'keyword_id' => (int) $pillar_keyword->id,
            ]);
            return;
        }

        $site_ids = $this->decode_json_array($pillar_keyword->target_site_ids);
        foreach (array_slice($secondary, 0, 3) as $supporting_keyword_text) {
            $existing = $this->db->get_var($this->db->prepare(
                "SELECT id FROM {$this->table('keywords')} WHERE client_id=%d AND main_keyword=%s LIMIT 1",
                (int) $client->id,
                $supporting_keyword_text
            ));
            if ($existing) {
                $this->vlog('supporting', 'Supporting keyword bestaat al', [
                    'client_id' => (int) $client->id,
                    'keyword' => $supporting_keyword_text,
                ]);
                continue;
            }

            $inserted = $this->db->insert($this->table('keywords'), [
                'client_id'          => (int) $client->id,
                'main_keyword'       => $supporting_keyword_text,
                'secondary_keywords' => wp_json_encode([]),
                'target_site_ids'    => wp_json_encode($site_ids),
                'content_type'       => 'supporting',
                'tone_of_voice'      => $pillar_keyword->tone_of_voice,
                'target_word_count'  => max(700, (int) floor((int) $pillar_keyword->target_word_count * 0.6)),
                'priority'           => (int) $pillar_keyword->priority + 5,
                'status'             => 'queued',
                'source'             => 'supporting',
                'source_context'     => wp_json_encode([
                    'parent_keyword_id' => (int) $pillar_keyword->id,
                    'parent_keyword' => (string) $pillar_keyword->main_keyword,
                ]),
                'created_at'         => $this->now(),
                'updated_at'         => $this->now(),
            ]);

            if ($inserted) {
                $new_keyword_id = (int) $this->db->insert_id;
                $this->create_jobs_for_keyword($new_keyword_id);
                $this->log('info', 'supporting', 'Supporting keyword aangemaakt', [
                    'keyword_id' => $new_keyword_id,
                    'keyword' => $supporting_keyword_text,
                ]);
            } else {
                $this->log('error', 'supporting', 'Supporting keyword insert mislukt', [
                    'db_error' => $this->db->last_error,
                    'keyword' => $supporting_keyword_text,
                ]);
            }
        }
    }

    private function is_openai_quota_error(Throwable $e): bool {
        $message = strtolower($e->getMessage());

        return (
            strpos($message, 'insufficient_quota') !== false ||
            strpos($message, 'exceeded your current quota') !== false ||
            strpos($message, 'billing') !== false ||
            strpos($message, 'http 429') !== false
        );
    }

    private function generate_article(object $keyword, object $client, object $site): array {
        $api_key = trim((string) get_option(self::OPTION_OPENAI_API_KEY, ''));
        if ($api_key === '') {
            throw new RuntimeException('OpenAI API key ontbreekt. Artikel wordt niet gepubliceerd.');
        }

        $secondary_keywords = $this->get_secondary_keywords_list($keyword);
        $link_targets = $this->get_client_link_targets($client);
        if (!$link_targets) {
            $link_targets[] = ['url' => $client->website_url, 'anchor' => $client->default_anchor ?: $keyword->main_keyword];
        }
        $anchor_plan = $this->build_anchor_plan($keyword, $client, $link_targets);
        $primary_target = $this->pick_primary_link_target($link_targets, (string) $keyword->main_keyword, $client, (bool) ($anchor_plan['force_non_exact'] ?? false));

        $research_bundle = [];
        try {
            $research_bundle = $this->build_client_research_bundle($client);
        } catch (Throwable $e) {
            $this->log('warning', 'research', 'Research bundle kon niet volledig worden opgebouwd', [
                'client_id' => (int) $client->id,
                'error' => $e->getMessage(),
            ]);
        }

        if ((bool) ($anchor_plan['threshold_exceeded'] ?? false) && (bool) ($anchor_plan['force_non_exact'] ?? false) && $this->classify_anchor_type((string) ($primary_target['anchor'] ?? ''), (string) $keyword->main_keyword, $client) === 'exact') {
            throw new RuntimeException('ANCHOR_BLOCK: Exact-match drempel overschreden en geen alternatief anchor type beschikbaar.');
        }

        try {
            $plan = $this->agent_plan($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $research_bundle, $anchor_plan);
            $draft = $this->agent_write($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $plan, $research_bundle, $anchor_plan);
            $edited = $this->agent_edit($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $plan, $draft, $research_bundle, $anchor_plan);
            $reviewed = $this->agent_review($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $plan, $edited, $research_bundle, $anchor_plan);
            $reviewed = $this->ensure_title_variation($reviewed, $keyword, $client, $site);
            return $reviewed;
        } catch (Throwable $e) {
            if ($this->is_openai_quota_error($e)) {
                $this->log('error', 'openai_quota', 'OpenAI quota/billing probleem. Publicatie geblokkeerd.', [
                    'keyword' => (string) $keyword->main_keyword,
                    'client_id' => (int) $client->id,
                    'site_id' => (int) $site->id,
                    'error' => $e->getMessage(),
                ]);

                throw new RuntimeException('OpenAI quota of billing probleem. Post is niet gepubliceerd.');
            }

            $this->log('warning', 'openai', 'AI pipeline fallback gebruikt', [
                'keyword' => $keyword->main_keyword,
                'error'   => $e->getMessage(),
            ]);

            return $this->generate_fallback_article($keyword, $client, $site, $e->getMessage());
        }
    }

    private function build_anchor_plan(object $keyword, object $client, array $link_targets): array {
        $default_mix = ['branded' => 45, 'partial' => 35, 'generic' => 20];
        $first_target = $link_targets[0] ?? ['url' => '', 'anchor' => ''];
        $target_url = esc_url_raw((string) ($first_target['url'] ?? ''));
        $history = $this->get_anchor_history_stats((int) $client->id, $target_url);
        $threshold = self::EXACT_MATCH_THRESHOLD_PERCENT;
        $threshold_exceeded = ((float) ($history['exact_ratio_percent'] ?? 0)) >= $threshold;

        $has_non_exact_candidate = false;
        foreach ($link_targets as $candidate) {
            $candidate_type = $this->classify_anchor_type((string) ($candidate['anchor'] ?? ''), (string) $keyword->main_keyword, $client);
            if ($candidate_type !== 'exact') {
                $has_non_exact_candidate = true;
                break;
            }
        }

        if ($threshold_exceeded && $has_non_exact_candidate) {
            $this->log('warning', 'anchor_plan', 'Job wordt herpland naar non-exact anchor door overschreden exact-match ratio.', [
                'client_id' => (int) $client->id,
                'keyword_id' => (int) $keyword->id,
                'target_url' => $target_url,
                'exact_ratio_percent' => (float) ($history['exact_ratio_percent'] ?? 0),
                'exact_match_threshold_percent' => $threshold,
            ]);
        }

        return [
            'percentages' => $default_mix,
            'exact_match_threshold_percent' => $threshold,
            'history' => $history,
            'threshold_exceeded' => $threshold_exceeded,
            'force_non_exact' => $threshold_exceeded,
            'target_url' => $target_url,
        ];
    }

    private function pick_primary_link_target(array $targets, string $main_keyword, object $client, bool $force_non_exact = false): array {
        if ($force_non_exact) {
            foreach ($targets as $target) {
                $type = $this->classify_anchor_type((string) ($target['anchor'] ?? ''), $main_keyword, $client);
                if ($type !== 'exact') {
                    return $target;
                }
            }
        }

        foreach ($targets as $target) {
            if (stripos((string) ($target['anchor'] ?? ''), $main_keyword) !== false) {
                return $target;
            }
        }
        return $targets[0];
    }

    private function build_client_research_bundle(object $client): array {
        $urls = $this->get_client_research_urls($client);
        if (!$urls && !empty($client->website_url)) {
            $urls[] = ['url' => (string) $client->website_url];
        }

        $max_pages = max(1, (int) get_option(self::OPTION_MAX_RESEARCH_PAGES, '5'));
        $urls = array_slice($urls, 0, $max_pages);

        $pages = [];
        foreach ($urls as $item) {
            $url = (string) ($item['url'] ?? '');
            if ($url === '') {
                continue;
            }

            $page = $this->fetch_and_extract_page($url);
            if (!empty($page['text'])) {
                $pages[] = $page;
            }
        }

        $bundle = [
            'client_name' => (string) $client->name,
            'website_url' => (string) $client->website_url,
            'pages' => $pages,
        ];

        $this->vlog('research', 'Research bundle opgebouwd', [
            'client_id' => (int) $client->id,
            'page_count' => count($pages),
            'urls' => array_map(static function ($p) { return (string) ($p['url'] ?? ''); }, $pages),
        ]);

        return $bundle;
    }

    private function fetch_and_extract_page(string $url): array {
        $start = microtime(true);
        $request_args = [
            'timeout' => 25,
            'redirection' => 5,
            'headers' => [
                'User-Agent' => 'ShortcutContentHubBot/0.5.2 (+WordPress)',
                'Accept' => 'text/html,application/xhtml+xml',
            ],
        ];
        $response = wp_remote_get($url, $request_args);

        if (is_wp_error($response) && function_exists('curl_init')) {
            $this->log('warning', 'research_http', 'wp_remote_get mislukt, cURL fallback gestart.', [
                'url' => $url,
                'error' => $response->get_error_message(),
            ]);
            $response = $this->curl_fallback_get($url, $request_args);
        }

        if (is_wp_error($response)) {
            throw new RuntimeException('Research GET mislukt voor ' . $url . ': ' . $response->get_error_message());
        }

        $code = (int) wp_remote_retrieve_response_code($response);
        $body = (string) wp_remote_retrieve_body($response);

        if ($code < 200 || $code >= 300) {
            throw new RuntimeException('Research GET gaf HTTP ' . $code . ' voor ' . $url);
        }

        $title = '';
        if (preg_match('~<title[^>]*>(.*?)</title>~is', $body, $m)) {
            $title = trim(wp_strip_all_tags(html_entity_decode($m[1], ENT_QUOTES | ENT_HTML5)));
        }

        $text = preg_replace('~<script\b[^>]*>.*?</script>~is', ' ', $body);
        $text = preg_replace('~<style\b[^>]*>.*?</style>~is', ' ', $text);
        $text = preg_replace('~<noscript\b[^>]*>.*?</noscript>~is', ' ', $text);
        $text = wp_strip_all_tags($text);
        $text = html_entity_decode($text, ENT_QUOTES | ENT_HTML5);
        $text = preg_replace('/\s+/u', ' ', $text);
        $text = trim((string) $text);
        $text = mb_substr($text, 0, 12000);

        $duration = round((microtime(true) - $start) * 1000);

        $this->vlog('research_http', 'Pagina opgehaald', [
            'url' => $url,
            'http_code' => $code,
            'title' => $title,
            'text_length' => mb_strlen($text),
            'duration_ms' => $duration,
        ]);

        return [
            'url' => $url,
            'title' => $title,
            'text' => $text,
        ];
    }

    private function curl_fallback_get(string $url, array $request_args) {
        if (!function_exists('curl_init')) {
            return new WP_Error('curl_missing', 'cURL extension ontbreekt.');
        }

        $ch = curl_init($url);
        if ($ch === false) {
            return new WP_Error('curl_init_failed', 'cURL initialisatie mislukt.');
        }

        $timeout = max(1, (int) ($request_args['timeout'] ?? 25));
        $redirection = max(0, (int) ($request_args['redirection'] ?? 5));
        $headers = [];
        foreach ((array) ($request_args['headers'] ?? []) as $key => $value) {
            $headers[] = (string) $key . ': ' . (string) $value;
        }

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_MAXREDIRS, $redirection);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, min(10, $timeout));
        curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 2);

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $body = curl_exec($ch);
        if ($body === false) {
            $error = curl_error($ch);
            curl_close($ch);
            return new WP_Error('curl_request_failed', 'cURL request mislukt: ' . $error);
        }

        $http_code = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        return [
            'response' => [
                'code' => $http_code,
                'message' => '',
            ],
            'body' => (string) $body,
            'headers' => [],
            'cookies' => [],
            'filename' => null,
        ];
    }

    private function discover_keywords_for_client(object $client): int {
        $this->log('info', 'keyword_discovery', 'Keyword discovery gestart', [
            'client_id' => (int) $client->id,
            'client_name' => (string) $client->name,
        ]);

        $research_bundle = $this->build_client_research_bundle($client);
        if (empty($research_bundle['pages'])) {
            throw new RuntimeException('Geen bruikbare research pagina’s gevonden voor deze klant.');
        }

        $sites = $this->db->get_results("SELECT id FROM {$this->table('sites')} WHERE is_active=1 ORDER BY publish_priority ASC");
        $site_ids = array_map(static function ($site) {
            return (int) $site->id;
        }, (array) $sites);

        if (!$site_ids) {
            throw new RuntimeException('Er zijn geen actieve blogs beschikbaar voor discovery-keywords.');
        }

        $ideas = $this->agent_discover_keywords($client, $research_bundle);

        $created = 0;
        foreach ($ideas as $idea) {
            $main_keyword = sanitize_text_field((string) ($idea['main_keyword'] ?? ''));
            if ($main_keyword === '') {
                continue;
            }

            $exists = $this->db->get_var($this->db->prepare(
                "SELECT id FROM {$this->table('keywords')} WHERE client_id=%d AND main_keyword=%s LIMIT 1",
                (int) $client->id,
                $main_keyword
            ));
            if ($exists) {
                $this->vlog('keyword_discovery', 'Keyword bestaat al, discovery slaat over', [
                    'client_id' => (int) $client->id,
                    'keyword' => $main_keyword,
                ]);
                continue;
            }

            $secondary = [];
            foreach ((array) ($idea['secondary_keywords'] ?? []) as $secondary_keyword) {
                $secondary_keyword = sanitize_text_field((string) $secondary_keyword);
                if ($secondary_keyword !== '') {
                    $secondary[] = ['keyword' => $secondary_keyword];
                }
            }

            $inserted = $this->db->insert($this->table('keywords'), [
                'client_id'          => (int) $client->id,
                'main_keyword'       => $main_keyword,
                'secondary_keywords' => wp_json_encode($secondary),
                'target_site_ids'    => wp_json_encode($site_ids),
                'target_site_categories' => wp_json_encode([]),
                'content_type'       => sanitize_text_field((string) ($idea['content_type'] ?? 'pillar')),
                'tone_of_voice'      => 'deskundig maar menselijk',
                'target_word_count'  => max(600, (int) ($idea['target_word_count'] ?? 1200)),
                'priority'           => (int) ($idea['priority'] ?? 10),
                'status'             => 'queued',
                'source'             => 'discovery',
                'source_context'     => wp_json_encode($idea, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                'created_at'         => $this->now(),
                'updated_at'         => $this->now(),
            ]);

            if ($inserted) {
                $keyword_id = (int) $this->db->insert_id;
                $created_jobs = $this->create_jobs_for_keyword($keyword_id);
                $created++;

                $this->log('info', 'keyword_discovery', 'Discovery keyword aangemaakt', [
                    'client_id' => (int) $client->id,
                    'keyword_id' => $keyword_id,
                    'keyword' => $main_keyword,
                    'created_jobs' => $created_jobs,
                ]);
            } else {
                $this->log('error', 'keyword_discovery', 'Discovery keyword insert mislukt', [
                    'client_id' => (int) $client->id,
                    'keyword' => $main_keyword,
                    'db_error' => $this->db->last_error,
                ]);
            }
        }

        $this->log('info', 'keyword_discovery', 'Keyword discovery afgerond', [
            'client_id' => (int) $client->id,
            'created_keywords' => $created,
        ]);

        return $created;
    }

    private function agent_discover_keywords(object $client, array $research_bundle): array {
        $max_keywords = max(1, (int) get_option(self::OPTION_MAX_DISCOVERY_KEYWORDS, '10'));

        $result = $this->openai_json_call(
            'keyword_discovery',
            [
                'role' => 'Je bent een Nederlandse SEO strateeg die zelfstandig keywordkansen ontdekt op basis van echte websitecontent.',
                'goal' => 'Vind concrete blogkeywordkansen, cluster-gerelateerde long-tail termen en geef bruikbare contenttypes terug. Geef alleen JSON terug.',
            ],
            [
                'client_name' => (string) $client->name,
                'website_url' => (string) $client->website_url,
                'max_keywords' => $max_keywords,
                'research_bundle' => $research_bundle,
                'requirements' => [
                    'focus_on_commercially_relevant_informational_keywords' => true,
                    'include_long_tail' => true,
                    'avoid_brand_only_terms' => true,
                    'avoid_obvious_duplicate_variants' => true,
                ],
            ],
            [
                'type' => 'object',
                'additionalProperties' => false,
                'properties' => [
                    'keywords' => [
                        'type' => 'array',
                        'items' => [
                            'type' => 'object',
                            'additionalProperties' => false,
                            'properties' => [
                                'main_keyword' => ['type' => 'string'],
                                'secondary_keywords' => [
                                    'type' => 'array',
                                    'items' => ['type' => 'string'],
                                ],
                                'content_type' => ['type' => 'string'],
                                'target_word_count' => ['type' => 'integer'],
                                'priority' => ['type' => 'integer'],
                                'why' => ['type' => 'string'],
                            ],
                            'required' => ['main_keyword', 'secondary_keywords', 'content_type', 'target_word_count', 'priority', 'why'],
                        ],
                    ],
                ],
                'required' => ['keywords'],
            ]
        );

        $keywords = (array) ($result['keywords'] ?? []);
        $this->vlog('keyword_discovery', 'OpenAI discovery output ontvangen', [
            'client_id' => (int) $client->id,
            'keyword_count' => count($keywords),
        ]);

        return $keywords;
    }

    private function generate_article_system_payload(
        object $keyword,
        object $client,
        object $site,
        array $primary_target,
        array $secondary_keywords,
        array $link_targets,
        array $research_bundle,
        array $anchor_plan
    ): array {
        $previous_variants = $this->get_recent_article_variants((int) $client->id, (string) $keyword->main_keyword);

        return [
            'main_keyword'       => (string) $keyword->main_keyword,
            'secondary_keywords' => $secondary_keywords,
            'content_type'       => (string) $keyword->content_type,
            'tone_of_voice'      => (string) $keyword->tone_of_voice,
            'target_word_count'  => (int) $keyword->target_word_count,
            'client_name'        => (string) $client->name,
            'client_website'     => (string) $client->website_url,
            'site_name'          => (string) $site->name,
            'primary_target'     => $primary_target,
            'available_targets'  => $link_targets,
            'research_bundle'    => $research_bundle,
            'anchor_plan'        => $anchor_plan,
            'site_profile'       => [
                'audience_level' => 'Nederlandstalige zakelijke lezers; niveau: geïnformeerde beginner tot medior.',
                'desired_angle' => 'Praktisch en toepasbaar met voldoende technische diepgang waar relevant.',
                'forbidden_repetitions' => [
                    'Gebruik geen titel die semantisch gelijk is aan eerdere varianten.',
                    'Start de titel met een andere formulering dan eerdere varianten (vermijd dezelfde eerste 4 woorden).',
                    'Gebruik geen intro met vrijwel dezelfde openingszin als eerdere varianten.',
                    'Kopieer geen H2-structuur van eerdere varianten.',
                    'Hergebruik geen identieke CTA-formulering uit eerdere varianten.',
                ],
            ],
            'previous_variants'  => $previous_variants,
        ];
    }

    private function get_recent_article_variants(int $client_id, string $main_keyword, int $limit = 8): array {
        $rows = $this->db->get_results($this->db->prepare(
            "SELECT a.title, a.content, s.name AS site_name
             FROM {$this->table('articles')} a
             INNER JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
             INNER JOIN {$this->table('sites')} s ON s.id = a.site_id
             WHERE a.client_id = %d
               AND k.main_keyword = %s
             ORDER BY a.id DESC
             LIMIT %d",
            $client_id,
            $main_keyword,
            max(1, $limit)
        ));

        if (!is_array($rows) || !$rows) {
            return [];
        }

        $variants = [];
        foreach ($rows as $row) {
            $content = wp_strip_all_tags((string) ($row->content ?? ''));
            $intro = trim((string) mb_substr(preg_replace('/\s+/', ' ', $content) ?: '', 0, 240));
            $variants[] = [
                'title' => sanitize_text_field((string) ($row->title ?? '')),
                'intro_excerpt' => $intro,
                'site_name' => sanitize_text_field((string) ($row->site_name ?? '')),
            ];
        }

        return $variants;
    }

    private function agent_plan(
        object $keyword,
        object $client,
        object $site,
        array $primary_target,
        array $secondary_keywords,
        array $link_targets,
        array $research_bundle,
        array $anchor_plan
    ): array {
        return $this->openai_json_call(
            'planner',
            [
                'role' => 'Senior content strategist voor Nederlandse SEO-content.',
                'goal' => 'Maak een diep contentplan voor een artikel op basis van keyword, doelgroep, zoekintentie en opgehaalde websitecontent. Geef alleen JSON terug.',
            ],
            $this->generate_article_system_payload($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $research_bundle, $anchor_plan),
            [
                'type' => 'object',
                'additionalProperties' => false,
                'properties' => [
                    'angle' => ['type' => 'string'],
                    'search_intent' => ['type' => 'string'],
                    'anchor_plan' => [
                        'type' => 'object',
                        'additionalProperties' => false,
                        'properties' => [
                            'branded' => ['type' => 'integer'],
                            'partial' => ['type' => 'integer'],
                            'generic' => ['type' => 'integer'],
                        ],
                        'required' => ['branded', 'partial', 'generic'],
                    ],
                    'headline_options' => ['type' => 'array', 'items' => ['type' => 'string']],
                    'outline' => ['type' => 'array', 'items' => ['type' => 'string']],
                    'differentiation_strategy' => ['type' => 'array', 'items' => ['type' => 'string']],
                    'internal_notes' => ['type' => 'array', 'items' => ['type' => 'string']],
                    'content_gaps_found_on_site' => ['type' => 'array', 'items' => ['type' => 'string']],
                ],
                'required' => ['angle', 'search_intent', 'anchor_plan', 'headline_options', 'outline', 'differentiation_strategy', 'internal_notes', 'content_gaps_found_on_site'],
            ]
        );
    }

    private function agent_write(
        object $keyword,
        object $client,
        object $site,
        array $primary_target,
        array $secondary_keywords,
        array $link_targets,
        array $plan,
        array $research_bundle,
        array $anchor_plan
    ): array {
        return $this->openai_json_call(
            'writer',
            [
                'role' => 'Nederlandse SEO-copywriter.',
                'goal' => 'Schrijf een sterk artikel in HTML, gevoed door research uit de website van de klant. Geef alleen JSON terug.',
            ],
            array_merge(
                $this->generate_article_system_payload($keyword, $client, $site, $primary_target, $secondary_keywords, $link_targets, $research_bundle, $anchor_plan),
                [
                    'plan' => $plan,
                    'requirements' => [
                        'content_html' => true,
                        'one_primary_link' => true,
                        'natural_dutch' => true,
                        'avoid_generic_ai_phrases' => true,
                        'build_on_site_research' => true,
                        'respect_anchor_plan_percentages' => true,
                        'do_not_output_h1_in_content' => true,
                        'do_not_repeat_post_title_inside_content' => true,
                        'use_a_distinct_opening_hook_vs_previous_variants' => true,
                        'use_a_distinct_h2_structure_vs_previous_variants' => true,
                        'use_a_distinct_cta_wording_vs_previous_variants' => true,
                    ],
                ]
            ),
            $this->article_schema()
        );
    }

    private function agent_edit(
        object $keyword,
        object $client,
        object $site,
        array $primary_target,
        array $secondary_keywords,
        array $link_targets,
        array $plan,
        array $draft,
        array $research_bundle,
        array $anchor_plan
    ): array {
        return $this->openai_json_call(
            'editor',
            [
                'role' => 'Kritische eindredacteur.',
                'goal' => 'Verbeter stijl, logica, ritme en leesbaarheid, zonder de commerciële intentie kwijt te raken. Geef alleen JSON terug.',
            ],
            [
                'main_keyword' => (string) $keyword->main_keyword,
                'secondary_keywords' => $secondary_keywords,
                'primary_target' => $primary_target,
                'plan' => $plan,
                'draft' => $draft,
                'research_bundle' => $research_bundle,
                'anchor_plan' => $anchor_plan,
            ],
            $this->article_schema()
        );
    }

    private function agent_review(
        object $keyword,
        object $client,
        object $site,
        array $primary_target,
        array $secondary_keywords,
        array $link_targets,
        array $plan,
        array $edited,
        array $research_bundle,
        array $anchor_plan
    ): array {
        $reviewed = $this->openai_json_call(
            'reviewer',
            [
                'role' => 'SEO reviewer en eindredacteur.',
                'goal' => 'Controleer SEO, structuur, meta data, interne consistentie met site research en linkplaatsing. Lever alleen de definitieve JSON terug.',
            ],
            [
                'main_keyword'       => (string) $keyword->main_keyword,
                'secondary_keywords' => $secondary_keywords,
                'target_word_count'  => (int) $keyword->target_word_count,
                'site_name'          => (string) $site->name,
                'primary_target'     => $primary_target,
                'available_targets'  => $link_targets,
                'plan'               => $plan,
                'draft'              => $edited,
                'research_bundle'    => $research_bundle,
                'anchor_plan'        => $anchor_plan,
                'checks'             => [
                    'keyword_in_title' => true,
                    'keyword_in_intro' => true,
                    'single_primary_link' => true,
                    'meta_title_present' => true,
                    'meta_description_present' => true,
                    'uses_site_context' => true,
                    'no_h1_in_content' => true,
                    'do_not_repeat_post_title_inside_content' => true,
                    'title_not_semantically_too_close_to_previous_variants' => true,
                    'intro_not_semantically_too_close_to_previous_variants' => true,
                ],
            ],
            $this->article_schema()
        );

        $reviewed['slug'] = sanitize_title((string) ($reviewed['slug'] ?? $keyword->main_keyword . '-' . $site->name));
        $reviewed['article_type'] = sanitize_text_field((string) ($reviewed['article_type'] ?? $keyword->content_type));

        return [
            'title'            => sanitize_text_field((string) ($reviewed['title'] ?? '')),
            'slug'             => sanitize_title((string) ($reviewed['slug'] ?? '')),
            'content'          => wp_kses_post((string) ($reviewed['content'] ?? '')),
            'meta_title'       => sanitize_text_field((string) ($reviewed['meta_title'] ?? '')),
            'meta_description' => sanitize_textarea_field((string) ($reviewed['meta_description'] ?? '')),
            'canonical_url'    => esc_url_raw((string) ($reviewed['canonical_url'] ?? '')),
            'article_type'     => sanitize_text_field((string) ($reviewed['article_type'] ?? 'pillar')),
        ];
    }

    private function article_schema(): array {
        return [
            'type' => 'object',
            'additionalProperties' => false,
            'properties' => [
                'title' => ['type' => 'string'],
                'slug' => ['type' => 'string'],
                'content' => ['type' => 'string'],
                'meta_title' => ['type' => 'string'],
                'meta_description' => ['type' => 'string'],
                'canonical_url' => ['type' => 'string'],
                'article_type' => ['type' => 'string'],
            ],
            'required' => ['title', 'slug', 'content', 'meta_title', 'meta_description', 'canonical_url', 'article_type'],
        ];
    }

    private function ensure_title_variation(array $article, object $keyword, object $client, object $site): array {
        $recent_titles = $this->get_recent_titles_for_keyword((int) $client->id, (string) $keyword->main_keyword);
        $current_title = sanitize_text_field((string) ($article['title'] ?? ''));

        if ($current_title === '' || !$this->is_title_too_close_to_existing($current_title, $recent_titles)) {
            return $article;
        }

        $this->log('info', 'title_variation', 'Titel te vergelijkbaar met eerdere varianten; herschrijven gestart', [
            'client_id' => (int) $client->id,
            'site_id' => (int) $site->id,
            'keyword' => (string) $keyword->main_keyword,
            'current_title' => $current_title,
        ]);

        try {
            $variation = $this->openai_json_call(
                'title_variation',
                [
                    'role' => 'Nederlandse SEO copy chief met focus op titeldiversiteit.',
                    'goal' => 'Herschrijf title, slug en meta_title zodat de nieuwe titel duidelijk verschilt van eerdere varianten voor hetzelfde keyword. Geef alleen JSON terug.',
                ],
                [
                    'main_keyword' => (string) $keyword->main_keyword,
                    'site_name' => (string) $site->name,
                    'current_title' => $current_title,
                    'current_slug' => (string) ($article['slug'] ?? ''),
                    'current_meta_title' => (string) ($article['meta_title'] ?? ''),
                    'recent_titles_same_keyword' => $recent_titles,
                    'rules' => [
                        'keyword_moet_in_titel_blijven' => true,
                        'nieuwe_opening_en_andere_woordvolgorde' => true,
                        'vermijd_woordelijke_of_semantische_dubbels' => true,
                    ],
                ],
                [
                    'type' => 'object',
                    'additionalProperties' => false,
                    'properties' => [
                        'title' => ['type' => 'string'],
                        'slug' => ['type' => 'string'],
                        'meta_title' => ['type' => 'string'],
                    ],
                    'required' => ['title', 'slug', 'meta_title'],
                ]
            );

            $candidate_title = sanitize_text_field((string) ($variation['title'] ?? ''));
            if ($candidate_title !== '' && !$this->is_title_too_close_to_existing($candidate_title, $recent_titles, 60.0)) {
                $article['title'] = $candidate_title;
                $article['slug'] = sanitize_title((string) ($variation['slug'] ?? ($article['slug'] ?? '')));
                $article['meta_title'] = sanitize_text_field((string) ($variation['meta_title'] ?? ($article['meta_title'] ?? $candidate_title)));
            }
        } catch (Throwable $e) {
            $this->log('warning', 'title_variation', 'Titelvariatie herschrijving mislukt', [
                'client_id' => (int) $client->id,
                'site_id' => (int) $site->id,
                'keyword' => (string) $keyword->main_keyword,
                'error' => $e->getMessage(),
            ]);
        }

        return $article;
    }

    private function get_recent_titles_for_keyword(int $client_id, string $main_keyword, int $limit = 20): array {
        $rows = $this->db->get_results($this->db->prepare(
            "SELECT a.title
             FROM {$this->table('articles')} a
             INNER JOIN {$this->table('keywords')} k ON k.id = a.keyword_id
             WHERE a.client_id = %d
               AND k.main_keyword = %s
             ORDER BY a.id DESC
             LIMIT %d",
            $client_id,
            $main_keyword,
            max(1, $limit)
        ));

        $titles = [];
        foreach ((array) $rows as $row) {
            $title = sanitize_text_field((string) ($row->title ?? ''));
            if ($title !== '') {
                $titles[] = $title;
            }
        }

        return $titles;
    }

    private function is_title_too_close_to_existing(string $candidate, array $existing_titles, float $threshold = 64.0): bool {
        $normalized_candidate = $this->normalize_title_for_similarity($candidate);
        if ($normalized_candidate === '') {
            return false;
        }

        foreach ($existing_titles as $existing_title) {
            $normalized_existing = $this->normalize_title_for_similarity((string) $existing_title);
            if ($normalized_existing === '') {
                continue;
            }

            similar_text($normalized_candidate, $normalized_existing, $percent);
            if ($percent >= $threshold) {
                return true;
            }

            $candidate_words = preg_split('/\s+/u', $normalized_candidate) ?: [];
            $existing_words = preg_split('/\s+/u', $normalized_existing) ?: [];
            $candidate_prefix = implode(' ', array_slice($candidate_words, 0, 4));
            $existing_prefix = implode(' ', array_slice($existing_words, 0, 4));
            if ($candidate_prefix !== '' && $candidate_prefix === $existing_prefix) {
                return true;
            }
        }

        return false;
    }

    private function normalize_title_for_similarity(string $title): string {
        $normalized = mb_strtolower(wp_strip_all_tags($title));
        $normalized = preg_replace('/[^\p{L}\p{N}\s]+/u', ' ', (string) $normalized);
        $normalized = preg_replace('/\s+/u', ' ', (string) $normalized);

        return trim((string) $normalized);
    }

    private function openai_json_call(string $name, array $system_context, array $user_payload, array $schema): array {
        $api_key = trim((string) get_option(self::OPTION_OPENAI_API_KEY, ''));
        $model = trim((string) get_option(self::OPTION_OPENAI_MODEL, 'gpt-5.4-mini'));
        $temperature = (float) get_option(self::OPTION_OPENAI_TEMPERATURE, '0.6');

        if ($api_key === '') {
            throw new RuntimeException('OpenAI API key ontbreekt.');
        }

        $instructions = implode("\n", [
            $system_context['role'] ?? '',
            $system_context['goal'] ?? '',
            'Schrijf in natuurlijk Nederlands.',
            'Geef uitsluitend geldige JSON terug die exact aan het schema voldoet.',
            'Geen markdown code fences.',
            'Geen extra uitleg buiten de JSON.',
        ]);

        $body = [
            'model' => $model,
            'instructions' => $instructions,
            'input' => wp_json_encode($user_payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            'temperature' => $temperature,
            'text' => [
                'format' => [
                    'type' => 'json_schema',
                    'name' => $name,
                    'strict' => true,
                    'schema' => $schema,
                ],
            ],
        ];

        $this->vlog('openai', 'OpenAI request start', [
            'name' => $name,
            'model' => $model,
            'temperature' => $temperature,
        ]);

        $start = microtime(true);
        $response = wp_remote_post('https://api.openai.com/v1/responses', [
            'timeout' => 120,
            'headers' => [
                'Authorization' => 'Bearer ' . $api_key,
                'Content-Type'  => 'application/json',
            ],
            'body' => wp_json_encode($body),
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('OpenAI request mislukt: ' . $response->get_error_message());
        }

        $code = (int) wp_remote_retrieve_response_code($response);
        $raw  = (string) wp_remote_retrieve_body($response);
        $json = json_decode($raw, true);
        $duration = round((microtime(true) - $start) * 1000);

        $this->vlog('openai', 'OpenAI response ontvangen', [
            'name' => $name,
            'http_code' => $code,
            'duration_ms' => $duration,
            'body_excerpt' => mb_substr($raw, 0, 800),
        ]);

        if ($code < 200 || $code >= 300 || !is_array($json)) {
            throw new RuntimeException('OpenAI response ongeldig: HTTP ' . $code . ' / ' . mb_substr($raw, 0, 800));
        }

        $text = $this->extract_openai_text($json);
        if ($text === '') {
            throw new RuntimeException('Geen OpenAI output gevonden.');
        }

        $decoded = json_decode($text, true);
        if (!is_array($decoded)) {
            throw new RuntimeException('OpenAI gaf geen geldige JSON terug: ' . mb_substr($text, 0, 800));
        }

        return $decoded;
    }

    private function extract_openai_text(array $response): string {
        if (!empty($response['output_text']) && is_string($response['output_text'])) {
            return trim($response['output_text']);
        }
        if (!empty($response['output']) && is_array($response['output'])) {
            foreach ($response['output'] as $output_item) {
                if (empty($output_item['content']) || !is_array($output_item['content'])) {
                    continue;
                }
                foreach ($output_item['content'] as $content_item) {
                    if (!empty($content_item['text']) && is_string($content_item['text'])) {
                        return trim($content_item['text']);
                    }
                }
            }
        }
        return '';
    }

    private function generate_fallback_article(object $keyword, object $client, object $site, string $reason): array {
        $secondary = $this->get_secondary_keywords_list($keyword);
        $targets = $this->get_client_link_targets($client);

        $primary_target = $targets ? $targets[0] : [
            'url' => $client->website_url,
            'anchor' => $client->default_anchor ?: $keyword->main_keyword,
        ];

        $title = ucfirst((string) $keyword->main_keyword) . ' uitgelegd';

        $content  = '<p>Fallbackcontent gebruikt. Reden: ' . esc_html($reason) . '</p>';
        $content .= '<p><a href="' . esc_url($primary_target['url'] ?? $client->website_url) . '">' . esc_html($primary_target['anchor'] ?? $keyword->main_keyword) . '</a></p>';

        if ($secondary) {
            $content .= '<h2>Gerelateerde onderwerpen</h2>';
            $content .= '<p>' . esc_html(implode(', ', $secondary)) . '</p>';
        }

        $content .= '<h2>Praktische informatie</h2>';
        $content .= '<p>Deze content houdt de pipeline draaiend wanneer de AI-laag tijdelijk faalt.</p>';

        return [
            'title' => $title,
            'slug' => sanitize_title((string) $keyword->main_keyword . '-' . $site->name),
            'content' => $content,
            'meta_title' => $title . ' | ' . $site->name,
            'meta_description' => 'Lees meer over ' . $keyword->main_keyword . '.',
            'canonical_url' => '',
            'article_type' => (string) $keyword->content_type,
        ];
    }

    private function generate_featured_image_payload(object $keyword, object $client, array $article): ?array {
        $access_key = trim((string) get_option(self::OPTION_UNSPLASH_ACCESS_KEY, ''));
        if ($access_key === '') {
            return null;
        }

        $search_term = $this->derive_unsplash_search_term_via_openai(
            (string) $keyword->main_keyword,
            wp_strip_all_tags((string) ($article['meta_description'] ?? ''))
        );

        $photo = $this->fetch_unsplash_random($search_term, $access_key);
        if (!empty($photo['download_loc'])) {
            wp_remote_get($photo['download_loc'], [
                'timeout' => 10,
                'headers' => ['Authorization' => 'Client-ID ' . $access_key],
            ]);
        }

        return [
            'search_term' => $search_term,
            'image_url' => (string) $photo['image_url'],
            'alt' => (string) ($photo['alt'] ?: $keyword->main_keyword),
            'credit_name' => (string) ($photo['photographer'] ?? ''),
            'credit_url' => (string) ($photo['profile_html'] ?? ''),
            'source_url' => (string) ($photo['photo_page'] ?? ''),
            'caption' => (string) ('Photo via Unsplash - ' . (!empty($photo['photographer']) ? $photo['photographer'] : '')),
            'unsplash_id' => (string) ($photo['id'] ?? ''),
        ];
    }

    private function derive_unsplash_search_term_via_openai(string $term_name, string $description): string {
        $api_key = trim((string) get_option(self::OPTION_OPENAI_API_KEY, ''));
        if ($api_key === '') {
            return $term_name;
        }

        try {
            $result = $this->openai_json_call(
                'featured_image_search_term',
                [
                    'role' => 'Je kiest een korte Unsplash zoekterm.',
                    'goal' => 'Geef een ultrakorte stock-photo zoekterm van 1 tot 3 woorden.',
                ],
                [
                    'term_name' => $term_name,
                    'description' => $description,
                ],
                [
                    'type' => 'object',
                    'additionalProperties' => false,
                    'properties' => [
                        'search_term' => ['type' => 'string'],
                    ],
                    'required' => ['search_term'],
                ]
            );
            $term = trim((string) ($result['search_term'] ?? ''));
            return $term !== '' ? $term : $term_name;
        } catch (Throwable $e) {
            return $term_name;
        }
    }

    private function fetch_unsplash_random(string $keyword, string $access_key): array {
        $url = add_query_arg([
            'query' => $keyword,
            'content_filter' => 'high',
            'orientation' => 'landscape',
        ], 'https://api.unsplash.com/photos/random');

        $start = microtime(true);
        $response = wp_remote_get($url, [
            'timeout' => 20,
            'headers' => [
                'Authorization' => 'Client-ID ' . $access_key,
                'Accept-Version' => 'v1',
            ],
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('Unsplash error: ' . $response->get_error_message());
        }

        $code = (int) wp_remote_retrieve_response_code($response);
        $body = (string) wp_remote_retrieve_body($response);
        $duration = round((microtime(true) - $start) * 1000);

        $this->vlog('featured_image', 'Unsplash response ontvangen', [
            'keyword' => $keyword,
            'http_code' => $code,
            'duration_ms' => $duration,
            'body_excerpt' => mb_substr($body, 0, 500),
        ]);

        if ($code !== 200) {
            throw new RuntimeException('Unsplash HTTP ' . $code . ' - ' . mb_substr($body, 0, 500));
        }

        $data = json_decode($body, true);
        if (!$data) {
            throw new RuntimeException('Unsplash invalid JSON');
        }
        if (isset($data[0]) && is_array($data[0])) {
            $data = $data[0];
        }

        $urls = $data['urls'] ?? [];
        $chosen = $urls['regular'] ?? ($urls['full'] ?? ($urls['small'] ?? ''));
        if ($chosen === '') {
            throw new RuntimeException('Geen bruikbare image URL in Unsplash response.');
        }

        return [
            'image_url' => $chosen,
            'photo_page' => $data['links']['html'] ?? '',
            'download_loc' => $data['links']['download_location'] ?? '',
            'photographer' => $data['user']['name'] ?? '',
            'profile_html' => $data['user']['links']['html'] ?? '',
            'alt' => $data['alt_description'] ?? '',
            'id' => $data['id'] ?? '',
        ];
    }

    private function store_article(object $job, object $keyword, object $client, object $site, array $article, ?array $featured_image, bool $collect_backlinks = true): int {
        $sanitized_content = wp_kses_post($article['content'] ?? '');
        $backlinks = $collect_backlinks ? $this->extract_backlinks_from_content($sanitized_content, $this->get_client_link_targets($client)) : [];

        $inserted = $this->db->insert($this->table('articles'), [
            'job_id' => (int) $job->id,
            'keyword_id' => (int) $keyword->id,
            'client_id' => (int) $client->id,
            'site_id' => (int) $site->id,
            'article_type' => sanitize_text_field($article['article_type'] ?? 'pillar'),
            'title' => wp_strip_all_tags($article['title'] ?? ''),
            'slug' => sanitize_title($article['slug'] ?? ''),
            'content' => $sanitized_content,
            'meta_title' => sanitize_text_field($article['meta_title'] ?? ''),
            'meta_description' => sanitize_textarea_field($article['meta_description'] ?? ''),
            'canonical_url' => esc_url_raw($article['canonical_url'] ?? ''),
            'featured_image_data' => $featured_image ? wp_json_encode($featured_image) : null,
            'publish_status' => 'queued',
            'backlinks_data' => $backlinks ? wp_json_encode($backlinks) : null,
            'created_at' => $this->now(),
            'updated_at' => $this->now(),
        ]);

        if (!$inserted) {
            throw new RuntimeException('Article insert mislukt: ' . $this->db->last_error);
        }

        $article_id = (int) $this->db->insert_id;
        $this->store_anchor_history_rows($job, $keyword, $client, $article_id, $backlinks);
        $this->vlog('article', 'Artikel opgeslagen in centrale database', [
            'article_id' => $article_id,
            'job_id' => (int) $job->id,
            'keyword_id' => (int) $keyword->id,
        ]);

        return $article_id;
    }

    private function build_receiver_url(object $site): string {
        return untrailingslashit((string) $site->base_url) . '/wp-admin/admin-post.php?action=shortcut_receive_content';
    }

    private function publish_to_remote_site(object $site, array $article, ?array $featured_image, object $job, object $keyword, object $client, int $article_id, ?string $forced_category = null, ?string $forced_status = null): array {
        $trusted_source_domain = $this->get_trusted_source_domain();
        $category = $this->sanitize_blog_category((string) $forced_category);
        if ($category === '') {
            $category = trim((string) $site->default_category);
        }

        if ($category === '') {
            $category = $this->determine_category_for_site((string) $site->base_url, (string) $site->name);

            $updated = $this->db->update(
                $this->table('sites'),
                [
                    'default_category' => $category,
                    'updated_at' => $this->now(),
                ],
                ['id' => (int) $site->id]
            );

            if ($updated === false) {
                $this->log('warning', 'site_category', 'Site categorie kon niet worden opgeslagen na auto-classificatie.', [
                    'site_id' => (int) $site->id,
                    'base_url' => (string) $site->base_url,
                    'category' => $category,
                    'db_error' => $this->db->last_error,
                ]);
            } else {
                $site->default_category = $category;
            }
        }

        $payload = [
            'timestamp' => time(),
            'source' => 'shortcut-content-hub',
            'source_site' => $trusted_source_domain,
            'title' => (string) $article['title'],
            'slug' => (string) $article['slug'],
            'content' => (string) $article['content'],
            'meta_title' => (string) $article['meta_title'],
            'meta_description' => (string) $article['meta_description'],
            'canonical_url' => (string) ($article['canonical_url'] ?? ''),
            'status' => in_array((string) $forced_status, ['draft', 'publish'], true) ? (string) $forced_status : (string) $site->default_status,
            'category' => $category,
            'external_job_id' => (int) $job->id,
            'external_article_id' => $article_id,
            'client_name' => (string) $client->name,
            'keyword' => (string) $keyword->main_keyword,
            'content_type' => (string) ($article['article_type'] ?? 'pillar'),
            'featured_image' => $featured_image,
        ];

        $body_json = wp_json_encode($payload);
        $url = $this->build_receiver_url($site);

        $this->vlog('publish', 'Remote publish start', [
            'site_id' => (int) $site->id,
            'url' => $url,
            'source_site' => $trusted_source_domain,
            'payload_excerpt' => mb_substr($body_json, 0, 1000),
        ]);

        $start = microtime(true);
        $response = wp_remote_post($url, [
            'timeout' => 60,
            'headers' => [
                'Content-Type' => 'application/json',
                'X-SCH-Source-Site' => $trusted_source_domain,
            ],
            'body' => $body_json,
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('Publish mislukt: ' . $response->get_error_message());
        }

        $code = (int) wp_remote_retrieve_response_code($response);
        $body = (string) wp_remote_retrieve_body($response);
        $json = json_decode($body, true);
        $duration = round((microtime(true) - $start) * 1000);

        $this->vlog('publish', 'Remote publish response ontvangen', [
            'site_id' => (int) $site->id,
            'http_code' => $code,
            'duration_ms' => $duration,
            'body' => mb_substr($body, 0, 1000),
        ]);

        if ($code < 200 || $code >= 300 || !is_array($json) || empty($json['success'])) {
            throw new RuntimeException('Remote response ongeldig: HTTP ' . $code . ' / ' . mb_substr($body, 0, 1000));
        }

        $this->log('info', 'publish', 'Remote publish geslaagd', [
            'site_id' => (int) $site->id,
            'response' => $json,
        ]);

        return $json;
    }

    private function determine_category_for_article(array $article): string {
        $allowed_categories = $this->allowed_blog_categories();

        try {
            $result = $this->openai_json_call(
                'blog_category_picker',
                [
                    'role' => 'Je bent een strikte content-classifier voor WordPress categorieën.',
                    'goal' => 'Kies exact één categorie uit de toegestane lijst op basis van titel en content.',
                ],
                [
                    'title' => (string) ($article['title'] ?? ''),
                    'content' => wp_strip_all_tags((string) ($article['content'] ?? '')),
                    'allowed_categories' => $allowed_categories,
                ],
                [
                    'type' => 'object',
                    'additionalProperties' => false,
                    'properties' => [
                        'category' => [
                            'type' => 'string',
                            'enum' => $allowed_categories,
                        ],
                    ],
                    'required' => ['category'],
                ]
            );

            $category = sanitize_text_field((string) ($result['category'] ?? ''));
            if (in_array($category, $allowed_categories, true)) {
                return $category;
            }
        } catch (Throwable $e) {
            $this->log('warning', 'openai_category', 'OpenAI categorisatie mislukt, fallback categorie toegepast.', [
                'error' => $e->getMessage(),
            ]);
        }

        return 'Inspiratie';
    }

    private function determine_category_for_site(string $base_url, string $site_name): string {
        $allowed_categories = $this->allowed_blog_categories();
        $fallback_category = 'Inspiratie';

        try {
            $this->vlog('site_category', 'Automatische site-categorisatie gestart.', [
                'base_url' => $base_url,
                'site_name' => $site_name,
            ]);

            $page = $this->fetch_and_extract_page($base_url);

            $result = $this->openai_json_call(
                'site_category_picker',
                [
                    'role' => 'Je bent een strikte website-classifier voor WordPress blog categorieën.',
                    'goal' => 'Kies exact één categorie uit de toegestane lijst op basis van website-inhoud.',
                ],
                [
                    'site_name' => $site_name,
                    'base_url' => $base_url,
                    'page_title' => (string) ($page['title'] ?? ''),
                    'page_text' => (string) ($page['text'] ?? ''),
                    'allowed_categories' => $allowed_categories,
                ],
                [
                    'type' => 'object',
                    'additionalProperties' => false,
                    'properties' => [
                        'category' => [
                            'type' => 'string',
                            'enum' => $allowed_categories,
                        ],
                    ],
                    'required' => ['category'],
                ]
            );

            $category = sanitize_text_field((string) ($result['category'] ?? ''));
            if (in_array($category, $allowed_categories, true)) {
                $this->log('info', 'site_category', 'Site-categorie automatisch bepaald.', [
                    'base_url' => $base_url,
                    'site_name' => $site_name,
                    'category' => $category,
                    'page_title' => (string) ($page['title'] ?? ''),
                    'page_text_length' => mb_strlen((string) ($page['text'] ?? '')),
                ]);
                return $category;
            }

            $this->log('warning', 'openai_site_category', 'OpenAI gaf ongeldige categorie, fallback categorie toegepast.', [
                'base_url' => $base_url,
                'site_name' => $site_name,
                'returned_category' => $category,
            ]);
        } catch (Throwable $e) {
            $this->log('warning', 'openai_site_category', 'Site categorisatie mislukt, fallback categorie toegepast.', [
                'base_url' => $base_url,
                'error' => $e->getMessage(),
            ]);
        }

        return $fallback_category;
    }

    public function run_gsc_auto_sync(): void {
        if (get_option(self::OPTION_GSC_ENABLED, '0') !== '1' || get_option(self::OPTION_GSC_AUTO_SYNC, '0') !== '1') {
            return;
        }

        $range_days = $this->sanitize_gsc_range_days((int) get_option(self::OPTION_GSC_DEFAULT_SYNC_RANGE, '28'));
        $row_limit = max(1, min(25000, (int) get_option(self::OPTION_GSC_DEFAULT_ROW_LIMIT, '250')));
        $top_n_clicks = $this->sanitize_gsc_top_n_clicks((int) get_option(self::OPTION_GSC_DEFAULT_TOP_N_CLICKS, '0'));
        $min_impressions = $this->sanitize_gsc_min_impressions((int) get_option(self::OPTION_GSC_DEFAULT_MIN_IMPRESSIONS, '0'));
        $clients = $this->db->get_results("SELECT * FROM {$this->table('clients')} WHERE gsc_property <> '' AND gsc_token_data IS NOT NULL");
        foreach ((array) $clients as $client) {
            try {
                $this->sync_gsc_keywords_for_client($client, $range_days, $row_limit, $top_n_clicks, $min_impressions);
            } catch (Throwable $e) {
                $this->log('error', 'gsc_sync', 'Auto sync mislukt voor klant', [
                    'client_id' => (int) $client->id,
                    'error' => $e->getMessage(),
                ]);
            }
        }
    }

    private function sync_gsc_keywords_for_client(object $client, int $range_days, int $row_limit, int $top_n_clicks = 0, int $min_impressions = 0): array {
        $client_id = (int) ($client->id ?? 0);
        $property = sanitize_text_field((string) ($client->gsc_property ?? ''));
        if ($client_id <= 0 || $property === '') {
            throw new RuntimeException('Geen geldige klant/property voor GSC sync.');
        }

        $this->log('info', 'gsc_sync', 'Keyword sync gestart', [
            'client_id' => $client_id,
            'property' => $property,
            'range_days' => $range_days,
            'row_limit' => $row_limit,
            'top_n_clicks' => $top_n_clicks,
            'min_impressions' => $min_impressions,
        ]);

        $access_token = $this->gsc_get_valid_access_token_for_client($client);
        $end = new DateTimeImmutable('now', wp_timezone());
        $start = $end->sub(new DateInterval('P' . max(1, $range_days) . 'D'));
        $rows = $this->gsc_query_keywords($property, $access_token, $start->format('Y-m-d'), $end->format('Y-m-d'), $row_limit);

        if ($min_impressions > 0) {
            $rows = array_values(array_filter($rows, static function (array $row) use ($min_impressions): bool {
                return (float) ($row['impressions'] ?? 0) >= $min_impressions;
            }));
        }

        usort($rows, static function (array $a, array $b): int {
            return ((float) ($b['clicks'] ?? 0)) <=> ((float) ($a['clicks'] ?? 0));
        });

        if ($top_n_clicks > 0 && count($rows) > $top_n_clicks) {
            $rows = array_slice($rows, 0, $top_n_clicks);
        }

        $site_ids = $this->get_default_target_site_ids();
        $inserted = 0;
        $updated = 0;

        foreach ($rows as $row) {
            $query = $this->normalize_keyword_term((string) ($row['query'] ?? ''));
            if ($query === '') {
                continue;
            }

            $source_context = wp_json_encode([
                'source' => 'google_search_console',
                'property' => $property,
                'range_days' => $range_days,
                'clicks' => (float) ($row['clicks'] ?? 0),
                'impressions' => (float) ($row['impressions'] ?? 0),
                'ctr' => (float) ($row['ctr'] ?? 0),
                'position' => (float) ($row['position'] ?? 0),
                'last_synced_at' => $this->now(),
            ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

            $existing_id = (int) $this->db->get_var($this->db->prepare(
                "SELECT id FROM {$this->table('keywords')} WHERE client_id=%d AND main_keyword=%s AND source='google_search_console' LIMIT 1",
                $client_id,
                $query
            ));

            $data = [
                'client_id' => $client_id,
                'main_keyword' => $query,
                'secondary_keywords' => wp_json_encode([]),
                'target_site_ids' => wp_json_encode($site_ids),
                'target_site_categories' => wp_json_encode([]),
                'content_type' => 'pillar',
                'tone_of_voice' => 'deskundig maar menselijk',
                'target_word_count' => 1200,
                'priority' => 10,
                'status' => 'queued',
                'source' => 'google_search_console',
                'source_context' => $source_context,
                'updated_at' => $this->now(),
            ];

            if ($existing_id > 0) {
                $ok = $this->db->update($this->table('keywords'), $data, ['id' => $existing_id]);
                if ($ok !== false) {
                    $updated++;
                }
            } else {
                $data['created_at'] = $this->now();
                $ok = $this->db->insert($this->table('keywords'), $data);
                if ($ok) {
                    $inserted++;
                }
            }
        }

        $this->db->update($this->table('clients'), [
            'gsc_last_synced_at' => $this->now(),
            'updated_at' => $this->now(),
        ], ['id' => $client_id]);

        $this->log('info', 'gsc_sync', 'Keyword sync geslaagd', [
            'client_id' => $client_id,
            'property' => $property,
            'rows' => count($rows),
            'top_n_clicks' => $top_n_clicks,
            'min_impressions' => $min_impressions,
            'inserted' => $inserted,
            'updated' => $updated,
        ]);

        return [
            'rows' => count($rows),
            'inserted' => $inserted,
            'updated' => $updated,
        ];
    }

    private function gsc_query_keywords(string $property, string $access_token, string $start_date, string $end_date, int $row_limit): array {
        $encoded_property = rawurlencode($property);
        $response = wp_remote_post('https://www.googleapis.com/webmasters/v3/sites/' . $encoded_property . '/searchAnalytics/query', [
            'timeout' => 30,
            'headers' => [
                'Authorization' => 'Bearer ' . $access_token,
                'Content-Type' => 'application/json',
            ],
            'body' => wp_json_encode([
                'startDate' => $start_date,
                'endDate' => $end_date,
                'dimensions' => ['query'],
                'rowLimit' => max(1, min(25000, $row_limit)),
            ]),
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('Search Analytics request mislukt: ' . $response->get_error_message());
        }

        $status = (int) wp_remote_retrieve_response_code($response);
        $body = json_decode((string) wp_remote_retrieve_body($response), true);
        if ($status < 200 || $status >= 300) {
            throw new RuntimeException('Search Analytics fout: HTTP ' . $status);
        }

        $rows = [];
        foreach ((array) ($body['rows'] ?? []) as $item) {
            $rows[] = [
                'query' => (string) (($item['keys'][0] ?? '')),
                'clicks' => (float) ($item['clicks'] ?? 0),
                'impressions' => (float) ($item['impressions'] ?? 0),
                'ctr' => (float) ($item['ctr'] ?? 0),
                'position' => (float) ($item['position'] ?? 0),
            ];
        }

        $this->log('info', 'gsc_sync', 'Rows opgehaald uit Search Console', [
            'property' => $property,
            'start_date' => $start_date,
            'end_date' => $end_date,
            'row_count' => count($rows),
        ]);

        return $rows;
    }

    private function gsc_list_properties_for_client(object $client): array {
        $access_token = $this->gsc_get_valid_access_token_for_client($client);
        $response = wp_remote_get('https://www.googleapis.com/webmasters/v3/sites', [
            'timeout' => 30,
            'headers' => ['Authorization' => 'Bearer ' . $access_token],
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('Properties ophalen mislukt: ' . $response->get_error_message());
        }

        $status = (int) wp_remote_retrieve_response_code($response);
        $body = json_decode((string) wp_remote_retrieve_body($response), true);
        if ($status < 200 || $status >= 300) {
            throw new RuntimeException('Properties ophalen mislukt: HTTP ' . $status);
        }

        $properties = [];
        foreach ((array) ($body['siteEntry'] ?? []) as $entry) {
            $site_url = sanitize_text_field((string) ($entry['siteUrl'] ?? ''));
            if ($site_url !== '') {
                $properties[] = $site_url;
            }
        }

        return array_values(array_unique($properties));
    }

    private function gsc_get_valid_access_token_for_client(object $client): string {
        $token_data = $this->gsc_get_token_payload_for_client($client);
        $access_token = (string) ($token_data['access_token'] ?? '');
        $refresh_token = (string) ($token_data['refresh_token'] ?? '');
        $expires_at = (string) ($client->gsc_token_expires_at ?? '');

        if ($access_token !== '' && $expires_at !== '' && strtotime($expires_at) > time() + 60) {
            return $access_token;
        }

        if ($refresh_token === '') {
            throw new RuntimeException('Geen refresh token aanwezig voor klant.');
        }

        $oauth_client_id = (string) get_option(self::OPTION_GSC_CLIENT_ID, '');
        $oauth_client_secret = (string) get_option(self::OPTION_GSC_CLIENT_SECRET, '');
        if ($oauth_client_id === '' || $oauth_client_secret === '') {
            throw new RuntimeException('Google OAuth instellingen ontbreken.');
        }

        $response = wp_remote_post('https://oauth2.googleapis.com/token', [
            'timeout' => 30,
            'body' => [
                'client_id' => $oauth_client_id,
                'client_secret' => $oauth_client_secret,
                'grant_type' => 'refresh_token',
                'refresh_token' => $refresh_token,
            ],
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('Token refresh mislukt: ' . $response->get_error_message());
        }

        $status = (int) wp_remote_retrieve_response_code($response);
        $body = json_decode((string) wp_remote_retrieve_body($response), true);
        if ($status < 200 || $status >= 300 || !is_array($body)) {
            throw new RuntimeException('Token refresh gaf HTTP ' . $status);
        }

        $token_data['access_token'] = (string) ($body['access_token'] ?? '');
        if (!empty($body['refresh_token'])) {
            $token_data['refresh_token'] = (string) $body['refresh_token'];
        }
        $expires_at_new = $this->gsc_expiry_time_from_token_response($body);

        $this->db->update($this->table('clients'), [
            'gsc_token_data' => $this->encrypt_sensitive_value(wp_json_encode($token_data)),
            'gsc_token_expires_at' => $expires_at_new,
            'updated_at' => $this->now(),
        ], ['id' => (int) $client->id]);

        $this->log('info', 'gsc_oauth', 'Refresh token hergebruikt', [
            'client_id' => (int) $client->id,
            'expires_at' => $expires_at_new,
        ]);

        return (string) $token_data['access_token'];
    }

    private function gsc_get_token_payload_for_client(object $client): array {
        $encrypted = (string) ($client->gsc_token_data ?? '');
        if ($encrypted === '') {
            throw new RuntimeException('Geen token data gevonden voor klant.');
        }
        $json = $this->decrypt_sensitive_value($encrypted);
        $decoded = json_decode($json, true);
        if (!is_array($decoded)) {
            throw new RuntimeException('Token data kon niet worden gelezen.');
        }
        return $decoded;
    }

    private function gsc_exchange_code_for_token(string $code): array {
        $response = wp_remote_post('https://oauth2.googleapis.com/token', [
            'timeout' => 30,
            'body' => [
                'code' => $code,
                'client_id' => (string) get_option(self::OPTION_GSC_CLIENT_ID, ''),
                'client_secret' => (string) get_option(self::OPTION_GSC_CLIENT_SECRET, ''),
                'redirect_uri' => $this->gsc_oauth_redirect_uri(),
                'grant_type' => 'authorization_code',
            ],
        ]);

        if (is_wp_error($response)) {
            throw new RuntimeException('OAuth token exchange mislukt: ' . $response->get_error_message());
        }

        $status = (int) wp_remote_retrieve_response_code($response);
        $body = json_decode((string) wp_remote_retrieve_body($response), true);
        if ($status < 200 || $status >= 300 || !is_array($body)) {
            throw new RuntimeException('OAuth token exchange gaf HTTP ' . $status);
        }
        if (empty($body['access_token'])) {
            throw new RuntimeException('OAuth token response bevat geen access token.');
        }

        return $body;
    }

    private function gsc_fetch_account_email(string $access_token): string {
        if ($access_token === '') {
            return '';
        }
        $response = wp_remote_get('https://www.googleapis.com/oauth2/v2/userinfo', [
            'timeout' => 20,
            'headers' => ['Authorization' => 'Bearer ' . $access_token],
        ]);
        if (is_wp_error($response)) {
            return '';
        }
        $body = json_decode((string) wp_remote_retrieve_body($response), true);
        return sanitize_email((string) ($body['email'] ?? ''));
    }

    private function gsc_expiry_time_from_token_response(array $token_response): string {
        $expires_in = max(300, (int) ($token_response['expires_in'] ?? HOUR_IN_SECONDS));
        return gmdate('Y-m-d H:i:s', time() + $expires_in);
    }

    private function gsc_oauth_redirect_uri(): string {
        return admin_url('admin-post.php?action=sch_gsc_oauth_callback');
    }

    private function is_gsc_integration_enabled(): bool {
        return get_option(self::OPTION_GSC_ENABLED, '0') === '1';
    }

    private function client_has_gsc_connection(object $client): bool {
        return !empty($client->gsc_token_data);
    }

    private function gsc_properties_cache_key(int $client_id): string {
        return 'sch_gsc_props_' . get_current_user_id() . '_' . $client_id;
    }

    private function get_cached_gsc_properties_for_user(int $client_id): array {
        $properties = get_transient($this->gsc_properties_cache_key($client_id));
        if (!is_array($properties)) {
            return [];
        }
        return array_values(array_filter(array_map('sanitize_text_field', $properties)));
    }

    private function sanitize_gsc_range_days(int $range_days): int {
        if (in_array($range_days, [7, 28, 90], true)) {
            return $range_days;
        }
        return 28;
    }

    private function sanitize_gsc_top_n_clicks(int $top_n_clicks): int {
        return max(0, min(25000, $top_n_clicks));
    }

    private function sanitize_gsc_min_impressions(int $min_impressions): int {
        return max(0, $min_impressions);
    }

    private function get_default_target_site_ids(): array {
        $site_ids = $this->db->get_col("SELECT id FROM {$this->table('sites')} WHERE is_active=1 ORDER BY id ASC");
        $site_ids = array_map('intval', (array) $site_ids);
        return array_values(array_filter($site_ids));
    }

    private function encrypt_sensitive_value(string $value): string {
        if ($value === '') {
            return '';
        }

        $key = hash('sha256', wp_salt('auth'), true);
        $iv = random_bytes(16);
        $cipher = openssl_encrypt($value, 'AES-256-CBC', $key, OPENSSL_RAW_DATA, $iv);
        if ($cipher === false) {
            return base64_encode($value);
        }

        return base64_encode($iv . $cipher);
    }

    private function decrypt_sensitive_value(string $encoded): string {
        $binary = base64_decode($encoded, true);
        if ($binary === false) {
            return '';
        }

        if (strlen($binary) < 17) {
            return $binary;
        }

        $key = hash('sha256', wp_salt('auth'), true);
        $iv = substr($binary, 0, 16);
        $cipher = substr($binary, 16);
        $plain = openssl_decrypt($cipher, 'AES-256-CBC', $key, OPENSSL_RAW_DATA, $iv);
        return is_string($plain) && $plain !== '' ? $plain : $binary;
    }

    private function sanitize_blog_categories(array $categories): array {
        $clean = [];
        foreach ($categories as $category) {
            $normalized = $this->sanitize_blog_category((string) $category);
            if ($normalized !== '') {
                $clean[] = $normalized;
            }
        }
        return array_values(array_unique($clean));
    }

    private function sanitize_blog_category(string $category): string {
        $category = sanitize_text_field(trim($category));
        if ($category === '') {
            return '';
        }

        $allowed_categories = $this->allowed_blog_categories();
        if (in_array($category, $allowed_categories, true)) {
            return $category;
        }

        $legacy_map = [
            'Technologie' => 'Tech',
            'Werk & Carrière' => 'Werk',
            'Persoonlijke Groei' => 'Groei',
            'Geld & Financieel' => 'Geld',
            'Bewegen & Sport' => 'Sport',
            'Gezin & Opvoeding' => 'Gezin',
            'Beauty & Verzorging' => 'Beauty',
            'Inspiratie & Tips' => 'Inspiratie',
        ];

        return $legacy_map[$category] ?? '';
    }

    private function allowed_blog_categories(): array {
        return [
            'Gezondheid',
            'Lifestyle',
            'Wonen',
            'Reizen',
            'Tech',
            'Werk',
            'Ondernemen',
            'Relaties',
            'Groei',
            'Geld',
            'Voeding',
            'Sport',
            'Mindset',
            'Gezin',
            'Beauty',
            'Cultuur',
            'Entertainment',
            'Duurzaamheid',
            'Vrije Tijd',
            'Inspiratie',
        ];
    }
}

SCH_Orchestrator::instance();
