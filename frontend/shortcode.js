(function ($) {
    'use strict';

    const config = window.SCH_SHORTCODE_CONFIG || {};
    const canManage = !!config.canManage;

    function apiRequest(path, options) {
        return $.ajax(Object.assign({
            url: (config.restBase || '').replace(/\/$/, '') + path,
            method: 'GET',
            dataType: 'json',
            contentType: 'application/json',
            headers: {
                'X-WP-Nonce': config.restNonce || ''
            }
        }, options || {}));
    }

    function formatBool(value) {
        return value ? 'Aan' : 'Uit';
    }

    function renderMetrics($root, payload) {
        const metrics = (payload && payload.metrics) || {};
        const jobs = metrics.jobs || {};
        const cards = [
            ['Jobs queued', jobs.queued || 0],
            ['Jobs running', jobs.running || 0],
            ['Wachten op redactie', jobs.awaiting_approval || 0],
            ['Published', jobs.published || 0],
            ['Failed', jobs.failed || 0],
            ['Actieve klanten', metrics.clients_active || 0],
            ['Actieve sites', metrics.sites_active || 0],
            ['Open issues', metrics.open_issues || 0]
        ];

        const html = cards.map(function (item) {
            return '<div class="sch-shortcode-app__metric"><span>' + item[0] + '</span><strong>' + item[1] + '</strong></div>';
        }).join('');
        $root.find('.sch-shortcode-app__metrics').html(html);

        const integrations = payload.integrations || {};
        $root.find('.sch-shortcode-app__integrations').html(
            '<p><strong>Integraties:</strong> GSC: ' + formatBool(integrations.gsc_enabled) + ' · GA: ' + formatBool(integrations.ga_enabled) + ' · SERP provider: ' + (integrations.serp_provider || '-') + '</p>'
        );
    }

    function loadBootstrap($root) {
        apiRequest('/bootstrap').done(function (payload) {
            renderMetrics($root, payload || {});
        }).fail(function () {
            $root.find('.sch-shortcode-app__metrics').html('<div class="sch-shortcode-app__notice">' + (config.i18n && config.i18n.error ? config.i18n.error : 'Er ging iets mis.') + '</div>');
        });
    }

    function loadKeywords($root) {
        const search = $root.find('[data-role="keyword-search"]').val();
        apiRequest('/keywords?per_page=50&search=' + encodeURIComponent(search || '')).done(function (payload) {
            const items = (payload && payload.items) || [];
            const rows = items.map(function (item) {
                return '<tr>' +
                    '<td>' + (item.id || '') + '</td>' +
                    '<td>' + (item.client_name || '-') + '</td>' +
                    '<td>' + (item.main_keyword || '-') + '</td>' +
                    '<td>' + (item.status || '-') + '</td>' +
                    '<td>' + (item.lifecycle_status || '-') + '</td>' +
                    '</tr>';
            }).join('');
            $root.find('[data-role="keywords-body"]').html(rows || '<tr><td colspan="5">Geen data</td></tr>');
        });
    }

    function loadIssues($root) {
        const type = $root.find('[data-role="issue-type"]').val() || '';
        apiRequest('/issues?status=open&type=' + encodeURIComponent(type)).done(function (payload) {
            const items = (payload && payload.items) || [];
            const html = items.map(function (item) {
                return '<li><strong>' + (item.title || 'Zonder titel') + '</strong><span>' + (item.signal_type || '-') + ' · score ' + (item.priority_score || 0) + '</span></li>';
            }).join('');
            $root.find('[data-role="issues-list"]').html(html || '<li>Geen open issues.</li>');
        });
    }

    function loadQueue($root) {
        apiRequest('/queue').done(function (payload) {
            const jobs = (payload && payload.jobs) || [];
            const html = jobs.slice(0, 20).map(function (item) {
                return '<li><strong>#' + (item.id || '') + '</strong><span>' + (item.job_type || '-') + ' · ' + (item.status || '-') + '</span></li>';
            }).join('');
            $root.find('[data-role="queue-list"]').html(html || '<li>Geen queue items.</li>');
        });
    }

    function loadSettings($root) {
        apiRequest('/settings').done(function (payload) {
            const $form = $root.find('[data-role="settings-form"]');
            $form.find('[name="openai_model"]').val(payload.openai_model || '');
            $form.find('[name="openai_temperature"]').val(payload.openai_temperature || '0.6');
            $form.find('[name="enable_auto_discovery"]').prop('checked', !!payload.enable_auto_discovery);
        });
    }

    function saveSettings($root) {
        const $form = $root.find('[data-role="settings-form"]');
        const data = {
            openai_model: $form.find('[name="openai_model"]').val(),
            openai_temperature: $form.find('[name="openai_temperature"]').val(),
            enable_auto_discovery: $form.find('[name="enable_auto_discovery"]').is(':checked')
        };

        apiRequest('/settings', {
            method: 'POST',
            data: JSON.stringify(data)
        }).done(function () {
            $root.find('[data-role="settings-feedback"]').text((config.i18n && config.i18n.saved) || 'Opgeslagen.');
        }).fail(function () {
            $root.find('[data-role="settings-feedback"]').text((config.i18n && config.i18n.error) || 'Opslaan mislukt.');
        });
    }

    function initTabs($root) {
        $root.on('click', '.sch-shortcode-app__tabs button', function () {
            const tab = $(this).data('tab');
            $root.find('.sch-shortcode-app__tabs button').removeClass('is-active');
            $(this).addClass('is-active');
            $root.find('.sch-shortcode-app__panel').removeClass('is-active');
            $root.find('.sch-shortcode-app__panel[data-panel="' + tab + '"]').addClass('is-active');
        });
    }

    function bindEvents($root) {
        $root.on('click', '[data-role="keywords-refresh"]', function () { loadKeywords($root); });
        $root.on('click', '[data-role="issues-refresh"]', function () { loadIssues($root); });
        $root.on('click', '[data-role="queue-refresh"]', function () { loadQueue($root); });
        $root.on('click', '[data-role="run-worker"]', function () {
            apiRequest('/queue/run-worker', { method: 'POST', data: '{}' }).always(function () {
                loadQueue($root);
                loadBootstrap($root);
            });
        });
        $root.on('submit', '[data-role="settings-form"]', function (event) {
            event.preventDefault();
            saveSettings($root);
        });
    }

    function initBackendDirectory($root) {
        const $search = $root.find('[data-role="backend-search"]');
        const $grid = $root.find('[data-role="backend-grid"]');
        const $cards = $grid.find('.sch-shortcode-app__backend-card');
        const $filters = $root.find('[data-role="backend-filters"]');
        const $empty = $root.find('[data-role="backend-empty"]');
        const $count = $root.find('[data-role="backend-count"]');
        let activeFilter = 'all';

        if (!$cards.length) {
            return;
        }

        function applyBackendFilter() {
            const searchQuery = String($search.val() || '').toLowerCase().trim();
            let visible = 0;

            $cards.each(function () {
                const $card = $(this);
                const cardCategory = String($card.data('category') || '');
                const searchBlob = String($card.data('search') || '');
                const matchesCategory = activeFilter === 'all' || cardCategory === activeFilter;
                const matchesSearch = !searchQuery || searchBlob.indexOf(searchQuery) !== -1;
                const show = matchesCategory && matchesSearch;
                $card.toggleClass('is-hidden', !show);
                if (show) {
                    visible += 1;
                }
            });

            $count.text(visible);
            $empty.prop('hidden', visible > 0);
        }

        $filters.on('click', 'button', function () {
            activeFilter = String($(this).data('filter') || 'all');
            $filters.find('button').removeClass('is-active');
            $(this).addClass('is-active');
            applyBackendFilter();
        });
        $search.on('input', applyBackendFilter);
        applyBackendFilter();
    }

    $(function () {
        $('.sch-shortcode-app').each(function () {
            const $root = $(this);
            if (!canManage) {
                return;
            }
            initTabs($root);
            bindEvents($root);
            loadBootstrap($root);
            loadKeywords($root);
            loadIssues($root);
            loadQueue($root);
            loadSettings($root);
            initBackendDirectory($root);
        });
    });
})(jQuery);
