<?php
/*
Plugin Name: Shortcut Content Hub Receiver
Description: Ontvangt content van de Shortcut Content Hub Orchestrator via admin-post en maakt of update WordPress posts, SEO-meta en featured images.
Version: 0.5.0
Author: OpenAI
*/



/// THIS IS THE RECEIVER PLUGIN


if (!defined('ABSPATH')) {
    exit;
}

final class SCH_Receiver {
    const OPTION_TRUSTED_SOURCE_DOMAIN = 'sch_receiver_trusted_source_domain';
    const REGISTRATION_ACTION = 'sch_register_receiver_blog';
    const REGISTRATION_TIMEOUT = 20;
    const DEFAULT_TRUSTED_SOURCE_DOMAIN = 'https://shortcut.nl';

    private static $instance = null;

    public static function instance() {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function __construct() {
        register_activation_hook(__FILE__, [self::class, 'handle_activation']);
        add_action('admin_menu', [$this, 'admin_menu']);
        add_action('admin_post_shortcut_receive_content', [$this, 'handle_receive']);
        add_action('admin_post_nopriv_shortcut_receive_content', [$this, 'handle_receive']);
        add_action('admin_post_sch_receiver_save_settings', [$this, 'handle_save_settings']);
    }

    public static function handle_activation() {
        $instance = self::instance();
        $instance->notify_orchestrator_about_receiver();
    }

    public function admin_menu() {
        add_options_page('Content Hub Receiver', 'Content Hub Receiver', 'manage_options', 'sch-receiver', [$this, 'render_settings']);
    }

    public function render_settings() {
        ?>
        <div class="wrap">
            <h1>Content Hub Receiver</h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <?php wp_nonce_field('sch_receiver_save_settings'); ?>
                <input type="hidden" name="action" value="sch_receiver_save_settings">
                <table class="form-table">
                    <tr>
                        <th>Trusted source domein</th>
                        <td>
                            <input type="url" name="trusted_source_domain" class="regular-text" value="<?php echo esc_attr($this->get_trusted_source_domain()); ?>" placeholder="https://shortcut.nl">
                            <p class="description">Alle requests met source domein <code>shortcut.nl</code> worden geaccepteerd. Geen tokens of shared secrets nodig.</p>
                        </td>
                    </tr>
                </table>
                <p><button class="button button-primary">Opslaan</button></p>
            </form>
        </div>
        <?php
    }

    public function handle_save_settings() {
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer('sch_receiver_save_settings');
        $trusted_source_domain = $this->normalize_site_url((string) ($_POST['trusted_source_domain'] ?? self::DEFAULT_TRUSTED_SOURCE_DOMAIN));
        if ($trusted_source_domain === '') {
            $trusted_source_domain = self::DEFAULT_TRUSTED_SOURCE_DOMAIN;
        }
        update_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, $trusted_source_domain);
        $this->notify_orchestrator_about_receiver();
        wp_safe_redirect(admin_url('options-general.php?page=sch-receiver'));
        exit;
    }

    public function handle_receive() {
        if (!$this->is_trusted_request()) {
            $this->json_response(['success' => false, 'message' => 'Request is niet afkomstig van trusted source domein.'], 403);
        }

        $raw = file_get_contents('php://input');
        if (!is_string($raw) || trim($raw) === '') {
            $this->json_response(['success' => false, 'message' => 'Lege payload.'], 400);
        }

        $payload = json_decode($raw, true);
        if (!is_array($payload)) {
            $this->json_response(['success' => false, 'message' => 'Ongeldige JSON payload.'], 400);
        }

        try {
            $post_id = $this->upsert_post($payload);
            $this->json_response([
                'success' => true,
                'remote_post_id' => (string) $post_id,
                'remote_url' => get_permalink($post_id),
                'status' => get_post_status($post_id),
            ], 200);
        } catch (Throwable $e) {
            $this->json_response(['success' => false, 'message' => $e->getMessage()], 500);
        }
    }

    private function upsert_post($payload) {
        $external_article_id = (int) ($payload['external_article_id'] ?? 0);
        if ($external_article_id <= 0) {
            throw new RuntimeException('external_article_id ontbreekt.');
        }

        $existing = get_posts([
            'post_type' => 'post',
            'post_status' => 'any',
            'meta_key' => '_sch_external_article_id',
            'meta_value' => (string) $external_article_id,
            'posts_per_page' => 1,
            'fields' => 'ids',
            'no_found_rows' => true,
        ]);
        $post_id = !empty($existing[0]) ? (int) $existing[0] : 0;

        $status = sanitize_key((string) ($payload['status'] ?? 'draft'));
        if (!in_array($status, ['draft', 'publish', 'pending', 'private'], true)) {
            $status = 'draft';
        }

        $postarr = [
            'post_title' => wp_strip_all_tags((string) ($payload['title'] ?? '')),
            'post_name' => sanitize_title((string) ($payload['slug'] ?? '')),
            'post_content' => wp_kses_post((string) ($payload['content'] ?? '')),
            'post_status' => $status,
            'post_type' => 'post',
        ];

        if ($post_id > 0) {
            $postarr['ID'] = $post_id;
            $result = wp_update_post($postarr, true);
        } else {
            $result = wp_insert_post($postarr, true);
        }

        if (is_wp_error($result)) {
            throw new RuntimeException('Post opslaan mislukt: ' . $result->get_error_message());
        }
        $post_id = (int) $result;

        update_post_meta($post_id, '_sch_external_article_id', (string) $external_article_id);
        update_post_meta($post_id, '_sch_external_job_id', sanitize_text_field((string) ($payload['external_job_id'] ?? '')));
        update_post_meta($post_id, '_sch_client_name', sanitize_text_field((string) ($payload['client_name'] ?? '')));
        update_post_meta($post_id, '_sch_keyword', sanitize_text_field((string) ($payload['keyword'] ?? '')));
        update_post_meta($post_id, '_sch_content_type', sanitize_text_field((string) ($payload['content_type'] ?? '')));
        $this->update_seo_meta($post_id, $payload);

        $canonical = esc_url_raw((string) ($payload['canonical_url'] ?? ''));
        if ($canonical !== '') {
            update_post_meta($post_id, '_yoast_wpseo_canonical', $canonical);
            update_post_meta($post_id, 'rank_math_canonical_url', $canonical);
        }

        $category_name = sanitize_text_field((string) ($payload['category'] ?? ''));
        if ($category_name !== '') {
            $term = term_exists($category_name, 'category');
            if (!$term) {
                $term = wp_insert_term($category_name, 'category');
            }

            $term_id = $this->extract_term_id($term);

            if ($term_id > 0) {
                wp_set_post_terms($post_id, [$term_id], 'category', false);
            }
        }

        if (!empty($payload['featured_image']) && is_array($payload['featured_image'])) {
            $attachment_id = $this->ensure_featured_image($post_id, $payload['featured_image']);
            if ($attachment_id > 0) {
                set_post_thumbnail($post_id, $attachment_id);
            }
        }

        $this->apply_internal_links($post_id, $payload);

        return $post_id;
    }

    private function apply_internal_links($post_id, $payload) {
        $terms = $this->extract_link_terms($payload);
        $related_posts = $this->find_related_posts($post_id, $terms, 4);

        if (count($related_posts) < 2) {
            $fallback = get_posts([
                'post_type' => 'post',
                'post_status' => 'publish',
                'post__not_in' => array_merge([$post_id], array_map(function ($item) {
                    return (int) $item['ID'];
                }, $related_posts)),
                'posts_per_page' => 4,
                'orderby' => 'date',
                'order' => 'DESC',
                'no_found_rows' => true,
            ]);

            foreach ($fallback as $post) {
                if (count($related_posts) >= 4) {
                    break;
                }
                $related_posts[] = [
                    'ID' => (int) $post->ID,
                    'post_title' => (string) $post->post_title,
                    'permalink' => (string) get_permalink($post->ID),
                ];
            }
        }

        if (count($related_posts) < 2) {
            delete_post_meta($post_id, '_sch_internal_links_applied');
            return;
        }

        $link_count = max(2, min(4, count($related_posts)));
        $related_posts = array_slice($related_posts, 0, $link_count);

        $source_content = (string) ($payload['content'] ?? '');
        $content_without_old_links = preg_replace('/<!-- sch-internal-links:start -->.*?<!-- sch-internal-links:end -->/is', '', $source_content);
        if (!is_string($content_without_old_links)) {
            $content_without_old_links = $source_content;
        }

        $internal_links = [];
        $items_markup = '';
        foreach ($related_posts as $related) {
            $anchor = $this->build_safe_anchor((string) $related['post_title']);
            $url = esc_url((string) $related['permalink']);
            if ($url === '' || $anchor === '') {
                continue;
            }

            $items_markup .= '<li><a href="' . $url . '">' . esc_html($anchor) . '</a></li>';
            $internal_links[] = [
                'post_id' => (int) $related['ID'],
                'url' => esc_url_raw((string) $related['permalink']),
                'anchor' => $anchor,
            ];
        }

        if (count($internal_links) < 2) {
            delete_post_meta($post_id, '_sch_internal_links_applied');
            return;
        }

        $links_block = "\n<!-- sch-internal-links:start -->\n";
        $links_block .= '<h2>' . esc_html__('Gerelateerde artikelen', 'sch-receiver') . "</h2>\n";
        $links_block .= "<p>" . esc_html__('Lees ook deze aanvullende artikelen over vergelijkbare onderwerpen:', 'sch-receiver') . "</p>\n";
        $links_block .= "<ul>\n" . $items_markup . "</ul>\n";
        $links_block .= "<!-- sch-internal-links:end -->";

        $updated_content = trim($content_without_old_links) . "\n\n" . $links_block;
        wp_update_post([
            'ID' => $post_id,
            'post_content' => wp_kses_post($updated_content),
        ]);

        update_post_meta($post_id, '_sch_internal_links_applied', $internal_links);
        update_post_meta($post_id, '_sch_internal_link_terms', $terms);
    }

    private function extract_link_terms($payload) {
        $candidate_text = implode(' ', [
            (string) ($payload['keyword'] ?? ''),
            (string) ($payload['title'] ?? ''),
            (string) ($payload['category'] ?? ''),
        ]);
        $candidate_text = sanitize_text_field(wp_strip_all_tags($candidate_text));
        if ($candidate_text === '') {
            return [];
        }

        $parts = preg_split('/\s+/u', mb_strtolower($candidate_text));
        if (!is_array($parts)) {
            return [];
        }

        $stopwords = ['de', 'het', 'een', 'en', 'van', 'voor', 'met', 'over', 'naar', 'in', 'op', 'te', 'bij', 'aan', 'of', 'is', 'je', 'jouw', 'uw'];
        $terms = [];
        foreach ($parts as $part) {
            $clean = preg_replace('/[^a-z0-9\-]+/u', '', (string) $part);
            if (!is_string($clean) || mb_strlen($clean) < 4 || in_array($clean, $stopwords, true)) {
                continue;
            }
            $terms[$clean] = true;
            if (count($terms) >= 10) {
                break;
            }
        }

        return array_keys($terms);
    }

    private function find_related_posts($post_id, $terms, $limit = 4) {
        if (empty($terms)) {
            return [];
        }

        $candidates = get_posts([
            'post_type' => 'post',
            'post_status' => 'publish',
            'post__not_in' => [$post_id],
            'posts_per_page' => 30,
            'orderby' => 'date',
            'order' => 'DESC',
            's' => implode(' ', $terms),
            'no_found_rows' => true,
        ]);

        if (empty($candidates)) {
            return [];
        }

        $scored = [];
        foreach ($candidates as $candidate) {
            $text = mb_strtolower(wp_strip_all_tags((string) $candidate->post_title . ' ' . (string) $candidate->post_excerpt));
            $score = 0;
            foreach ($terms as $term) {
                if (strpos($text, $term) !== false) {
                    $score++;
                }
            }

            if ($score < 1) {
                continue;
            }

            $scored[] = [
                'score' => $score,
                'ID' => (int) $candidate->ID,
                'post_title' => (string) $candidate->post_title,
                'permalink' => (string) get_permalink($candidate->ID),
            ];
        }

        usort($scored, function ($a, $b) {
            if ($a['score'] === $b['score']) {
                return 0;
            }

            return ($a['score'] < $b['score']) ? 1 : -1;
        });

        $unique = [];
        foreach ($scored as $item) {
            if (count($unique) >= $limit) {
                break;
            }
            if (empty($item['permalink'])) {
                continue;
            }
            $unique[$item['ID']] = [
                'ID' => $item['ID'],
                'post_title' => $item['post_title'],
                'permalink' => $item['permalink'],
            ];
        }

        return array_values($unique);
    }

    private function build_safe_anchor($title) {
        $normalized_title = trim(sanitize_text_field(wp_strip_all_tags($title)));
        if ($normalized_title === '') {
            return '';
        }

        $words = preg_split('/\s+/u', $normalized_title);
        if (!is_array($words)) {
            return $normalized_title;
        }

        $selected = [];
        foreach ($words as $word) {
            $clean = preg_replace('/[^a-z0-9\-]+/iu', '', (string) $word);
            if (!is_string($clean) || $clean === '') {
                continue;
            }

            $selected[] = $clean;
            if (count($selected) >= 6) {
                break;
            }
        }

        if (empty($selected)) {
            return $normalized_title;
        }

        return implode(' ', $selected);
    }

    private function update_seo_meta($post_id, $payload) {
        $meta_title = sanitize_text_field((string) ($payload['meta_title'] ?? ''));
        $meta_description = sanitize_textarea_field((string) ($payload['meta_description'] ?? ''));

        update_post_meta($post_id, '_yoast_wpseo_title', $meta_title);
        update_post_meta($post_id, '_yoast_wpseo_metadesc', $meta_description);
        update_post_meta($post_id, 'rank_math_title', $meta_title);
        update_post_meta($post_id, 'rank_math_description', $meta_description);
    }

    /**
     * @param array<string, mixed>|int|string $term
     */
    private function extract_term_id($term) {
        if (is_array($term) && !empty($term['term_id'])) {
            return (int) $term['term_id'];
        }
        if (is_int($term) || ctype_digit((string) $term)) {
            return (int) $term;
        }

        return 0;
    }

    private function ensure_featured_image($post_id, $image) {
        require_once ABSPATH . 'wp-admin/includes/media.php';
        require_once ABSPATH . 'wp-admin/includes/file.php';
        require_once ABSPATH . 'wp-admin/includes/image.php';

        $existing = (int) get_post_meta($post_id, '_sch_featured_attachment_id', true);
        $existing_unsplash_id = (string) get_post_meta($post_id, '_sch_unsplash_id', true);
        $incoming_unsplash_id = sanitize_text_field((string) ($image['unsplash_id'] ?? ''));
        if ($existing > 0 && $incoming_unsplash_id !== '' && $existing_unsplash_id === $incoming_unsplash_id && get_post($existing)) {
            return $existing;
        }

        $image_url = esc_url_raw((string) ($image['image_url'] ?? ''));
        if ($image_url === '') {
            return 0;
        }

        $tmp = download_url($image_url, 30);
        if (is_wp_error($tmp)) {
            throw new RuntimeException('Featured image download mislukt: ' . $tmp->get_error_message());
        }

        $filename = sanitize_title((string) get_post_field('post_name', $post_id)) . '-' . ($incoming_unsplash_id ?: wp_generate_password(6, false)) . '.jpg';
        $file_array = [
            'name' => $filename,
            'tmp_name' => $tmp,
        ];

        $caption = sanitize_text_field((string) ($image['caption'] ?? ''));
        $attachment_id = media_handle_sideload($file_array, $post_id, $caption, [
            'post_title' => wp_strip_all_tags(get_the_title($post_id) . ' featured image'),
            'post_excerpt' => $caption,
            'post_content' => sanitize_text_field((string) ($image['source_url'] ?? '')),
        ]);

        if (is_wp_error($attachment_id)) {
            @unlink($tmp);
            throw new RuntimeException('Attachment aanmaken mislukt: ' . $attachment_id->get_error_message());
        }

        $alt = sanitize_text_field((string) ($image['alt'] ?? get_the_title($post_id)));
        if ($alt !== '') {
            update_post_meta($attachment_id, '_wp_attachment_image_alt', $alt);
        }

        update_post_meta($post_id, '_sch_featured_attachment_id', (int) $attachment_id);
        update_post_meta($post_id, '_sch_unsplash_id', $incoming_unsplash_id);
        update_post_meta($attachment_id, '_sch_credit_name', sanitize_text_field((string) ($image['credit_name'] ?? '')));
        update_post_meta($attachment_id, '_sch_credit_url', esc_url_raw((string) ($image['credit_url'] ?? '')));
        update_post_meta($attachment_id, '_sch_source_url', esc_url_raw((string) ($image['source_url'] ?? '')));

        return (int) $attachment_id;
    }

    private function get_trusted_source_domain() {
        $saved = $this->normalize_site_url((string) get_option(self::OPTION_TRUSTED_SOURCE_DOMAIN, self::DEFAULT_TRUSTED_SOURCE_DOMAIN));
        if ($saved === '') {
            return self::DEFAULT_TRUSTED_SOURCE_DOMAIN;
        }
        return $saved;
    }

    private function normalize_site_url($url) {
        $url = trim($url);
        if ($url === '') {
            return '';
        }
        if (!preg_match('#^https?://#i', $url)) {
            $url = 'https://' . $url;
        }
        $normalized = esc_url_raw($url);
        if ($normalized === '') {
            return '';
        }
        return untrailingslashit($normalized);
    }

    private function normalize_host($url) {
        $host = wp_parse_url($url, PHP_URL_HOST);
        return strtolower((string) $host);
    }

    private function is_trusted_request() {
        $trusted_host = $this->normalize_host($this->get_trusted_source_domain());
        if ($trusted_host === '') {
            return false;
        }

        $candidates = $this->request_source_candidates();

        foreach ($candidates as $candidate) {
            if ($this->normalize_host($candidate) === $trusted_host) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return string[]
     */
    private function request_source_candidates() {
        $headers = [
            isset($_SERVER['HTTP_X_SCH_SOURCE_SITE']) ? (string) $_SERVER['HTTP_X_SCH_SOURCE_SITE'] : '',
            isset($_SERVER['HTTP_ORIGIN']) ? (string) $_SERVER['HTTP_ORIGIN'] : '',
            isset($_SERVER['HTTP_REFERER']) ? (string) $_SERVER['HTTP_REFERER'] : '',
        ];

        return array_values(array_filter($headers, function ($header) {
            return $header !== '';
        }));
    }

    private function json_response($data, $status_code) {
        status_header($status_code);
        header('Content-Type: application/json; charset=' . get_bloginfo('charset'));
        echo wp_json_encode($data);
        exit;
    }

    private function notify_orchestrator_about_receiver() {
        $trusted_source_domain = $this->get_trusted_source_domain();
        $url = untrailingslashit($trusted_source_domain) . '/wp-admin/admin-post.php?action=' . self::REGISTRATION_ACTION;

        $payload = [
            'blog_name' => get_bloginfo('name'),
            'blog_url' => home_url('/'),
            'receiver_url' => admin_url('admin-post.php?action=shortcut_receive_content'),
            'receiver_version' => '0.5.0',
            'installed_at' => gmdate('c'),
        ];

        $response = wp_remote_post($url, [
            'timeout' => self::REGISTRATION_TIMEOUT,
            'headers' => [
                'Content-Type' => 'application/json',
                'X-SCH-Source-Site' => home_url('/'),
            ],
            'body' => wp_json_encode($payload),
        ]);

        if (is_wp_error($response)) {
            error_log('SCH Receiver registratie ping mislukt: ' . $response->get_error_message());
            return;
        }

        $http_code = (int) wp_remote_retrieve_response_code($response);
        if ($http_code < 200 || $http_code >= 300) {
            error_log('SCH Receiver registratie ping gaf HTTP ' . $http_code . '.');
        }
    }
}

SCH_Receiver::instance();
