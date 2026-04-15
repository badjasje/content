<?php
/*
Plugin Name: Shortcut Content Hub Receiver
Description: Ontvangt gesigneerde content van de Shortcut Content Hub Orchestrator via admin-post en maakt of update WordPress posts, SEO-meta en featured images.
Version: 0.4.0
Author: OpenAI
*/



/// THIS IS THE RECEIVER PLUGIN


if (!defined('ABSPATH')) {
    exit;
}

final class SCH_Receiver {
    const OPTION_SECRET = 'sch_receiver_secret';

    private static ?SCH_Receiver $instance = null;

    public static function instance(): self {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function __construct() {
        add_action('admin_menu', [$this, 'admin_menu']);
        add_action('admin_post_shortcut_receive_content', [$this, 'handle_receive']);
        add_action('admin_post_nopriv_shortcut_receive_content', [$this, 'handle_receive']);
        add_action('admin_post_sch_receiver_save_settings', [$this, 'handle_save_settings']);
    }

    public function admin_menu(): void {
        add_options_page('Content Hub Receiver', 'Content Hub Receiver', 'manage_options', 'sch-receiver', [$this, 'render_settings']);
    }

    public function render_settings(): void {
        ?>
        <div class="wrap">
            <h1>Content Hub Receiver</h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <?php wp_nonce_field('sch_receiver_save_settings'); ?>
                <input type="hidden" name="action" value="sch_receiver_save_settings">
                <table class="form-table">
                    <tr>
                        <th>Shared secret</th>
                        <td><input type="text" name="receiver_secret" class="regular-text" value="<?php echo esc_attr((string) get_option(self::OPTION_SECRET, '')); ?>"></td>
                    </tr>
                </table>
                <p><button class="button button-primary">Opslaan</button></p>
                <p>Zorg dat deze exact gelijk is aan de receiver secret in de centrale orchestrator.</p>
            </form>
        </div>
        <?php
    }

    public function handle_save_settings(): void {
        if (!current_user_can('manage_options')) {
            wp_die('Geen toegang.');
        }
        check_admin_referer('sch_receiver_save_settings');
        update_option(self::OPTION_SECRET, sanitize_text_field((string) ($_POST['receiver_secret'] ?? '')));
        wp_safe_redirect(admin_url('options-general.php?page=sch-receiver'));
        exit;
    }

    public function handle_receive(): void {
        $secret = trim((string) get_option(self::OPTION_SECRET, ''));
        if ($secret === '') {
            $this->json_response(['success' => false, 'message' => 'Receiver secret ontbreekt.'], 500);
        }

        $raw = file_get_contents('php://input');
        if (!is_string($raw) || trim($raw) === '') {
            $this->json_response(['success' => false, 'message' => 'Lege payload.'], 400);
        }

        $signature = isset($_SERVER['HTTP_X_SCH_SIGNATURE']) ? (string) $_SERVER['HTTP_X_SCH_SIGNATURE'] : '';
        $expected = hash_hmac('sha256', $raw, $secret);
        if ($signature === '' || !hash_equals($expected, $signature)) {
            $this->json_response(['success' => false, 'message' => 'Ongeldige signature.'], 403);
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

    private function upsert_post(array $payload): int {
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
        update_post_meta($post_id, '_yoast_wpseo_title', sanitize_text_field((string) ($payload['meta_title'] ?? '')));
        update_post_meta($post_id, '_yoast_wpseo_metadesc', sanitize_textarea_field((string) ($payload['meta_description'] ?? '')));
        update_post_meta($post_id, 'rank_math_title', sanitize_text_field((string) ($payload['meta_title'] ?? '')));
        update_post_meta($post_id, 'rank_math_description', sanitize_textarea_field((string) ($payload['meta_description'] ?? '')));

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
            if (is_array($term) && !empty($term['term_id'])) {
                wp_set_post_terms($post_id, [(int) $term['term_id']], 'category', false);
            }
        }

        if (!empty($payload['featured_image']) && is_array($payload['featured_image'])) {
            $attachment_id = $this->ensure_featured_image($post_id, $payload['featured_image']);
            if ($attachment_id > 0) {
                set_post_thumbnail($post_id, $attachment_id);
            }
        }

        return $post_id;
    }

    private function ensure_featured_image(int $post_id, array $image): int {
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

    private function json_response(array $data, int $status_code): void {
        status_header($status_code);
        header('Content-Type: application/json; charset=' . get_bloginfo('charset'));
        echo wp_json_encode($data);
        exit;
    }
}

SCH_Receiver::instance();
