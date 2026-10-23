document.addEventListener('DOMContentLoaded', () => {
    'use strict';
    const escape = window.BasketSignal.escapeHtml;
    const form = document.querySelector('[data-rule-filters]');
    const results = document.querySelector('[data-rule-results]');
    const summary = document.querySelector('[data-rule-summary]');
    const previous = document.querySelector('[data-rules-previous]');
    const next = document.querySelector('[data-rules-next]');
    const liveSummary = document.querySelector('[data-live-summary]');
    const liveResults = document.querySelector('[data-live-results]');
    const liveButton = document.querySelector('[data-live-refresh]');
    const initial = new URLSearchParams(location.search);
    for (const key of ['q', 'eligible', 'sort', 'source']) if (initial.has(key)) form.elements[key].value = initial.get(key);
    let page = Math.max(1, Number(initial.get('page')) || 1);
    let requestId = 0;
    let livePending = false;
    let closed = false;
    let liveSignature = '';
    let loadedParams = null;
    let rulesPending = false;
    const percent = value => `${(Number(value) * 100).toFixed(2)}%`;
    const sourceLabel = source => ({historical_snapshot: 'Historical snapshot', live_storefront: 'Live storefront orders', live_synthetic: 'Live synthetic orders'}[source] || source);

    function miningStatus(data) {
        if (!data.live_rule_mining_enabled) return 'Live mining is disabled in this server.';
        if (!data.mining?.length) return `Live mining: ${data.mining_status}. Historical rules remain available.`;
        return data.mining.map(item => `${sourceLabel(item.source)}: ${item.independent_shoppers} independent shoppers / ${item.policy.min_transactions} minimum · ${item.discovery_transactions} discovery + ${item.validation_transactions} validation · ${item.rule_count} published rules`).join(' | ');
    }

    function evidence(rule) {
        if (!rule.validation) return '';
        return `<details class="rule-evidence"><summary>Validation evidence · ${escape(sourceLabel(rule.source))}</summary><p>Earlier cohort: ${rule.discovery.pair_count}/${rule.discovery.transactions} baskets contain both products. Later cohort: ${rule.validation.pair_count}/${rule.validation.transactions}. Per-run adjusted p: ${Number(rule.adjusted_p_value).toExponential(2)}. Version: ${escape(rule.version)}. Mined: ${escape(rule.mined_at)}.</p></details>`;
    }

    function eligibilityLabel(rule) {
        if (!rule.coupon_eligible) return 'Not coupon-eligible';
        if (rule.active_for_serving === false) return 'Validated · demo serving disabled';
        return 'Coupon-eligible pair';
    }

    async function loadRules(automatic = false) {
        if (automatic && (rulesPending || closed || document.visibilityState === 'hidden'
            || results.contains(document.activeElement) || form.contains(document.activeElement))) return;
        const id = ++requestId;
        const params = automatic && loadedParams ? new URLSearchParams(loadedParams) : new URLSearchParams(new FormData(form));
        params.set('page', String(page));
        rulesPending = true;
        results.setAttribute('aria-busy', 'true');
        if (!automatic) { previous.disabled = true; next.disabled = true; summary.textContent = 'Loading rules…'; }
        try {
            const response = await fetch(`/api/rules?${params}`);
            if (!response.ok) throw new Error();
            const data = await response.json();
            if (id !== requestId || closed) return;
            if (automatic && (results.contains(document.activeElement) || form.contains(document.activeElement))) return;
            page = data.pagination.page;
            params.set('page', String(page));
            history.replaceState({}, '', `/rules?${params}`);
            loadedParams = params.toString();
            summary.textContent = `${data.total.toLocaleString()} matches · ${data.eligible_total} coupon-eligible · ${data.historical_total.toLocaleString()} historical + ${data.live_total || 0} live rules`;
            document.querySelector('[data-mining-status]').textContent = miningStatus(data);
            document.querySelector('[data-rule-dwell]').textContent = data.policy.dwell_threshold_seconds;
            document.querySelector('[data-rule-thresholds]').textContent = `Coupon thresholds: lift ≥ ${data.policy.min_lift} · confidence ≥ ${percent(data.policy.min_confidence)} · support ≥ ${percent(data.policy.min_support)}. Discount: ${data.policy.discount_percent}%.`;
            document.querySelector('[data-rule-version]').textContent = `· ${data.snapshot_version}`;
            results.innerHTML = data.rules.length ? data.rules.map(rule => `<article class="rule-row"><div class="rule-pair"><a href="/product/${rule.antecedent.product_id}"><small>Add to basket</small>${escape(rule.antecedent.product_name)}</a><span aria-label="then">→</span><a href="/product/${rule.consequent.product_id}"><small>Inspect this product</small>${escape(rule.consequent.product_name)}</a></div><dl class="rule-metrics"><div><dt>Lift</dt><dd>${Number(rule.lift).toFixed(2)}×</dd></div><div><dt>Confidence</dt><dd>${percent(rule.confidence)}</dd></div><div><dt>Support</dt><dd>${percent(rule.support)}</dd></div></dl><p class="rule-status">${eligibilityLabel(rule)}<small>${escape(sourceLabel(rule.source))}</small><small>${rule.coupon_eligible ? 'Subject to shopper/session conditions' : `Below ${escape(rule.failed_thresholds.join(', '))} threshold`}</small></p>${evidence(rule)}</article>`).join('') : '<p>No matching rules. Try another product name or select all sources.</p>';
            document.querySelector('[data-rule-page]').textContent = `Page ${page} of ${data.pagination.pages}`;
            previous.disabled = page <= 1; next.disabled = page >= data.pagination.pages;
        } catch (_) {
            if (id !== requestId || closed) return;
            summary.textContent = automatic ? 'Live refresh unavailable; showing the last loaded rules.' : 'Rules could not be loaded.';
            if (!automatic) results.innerHTML = '<p>Check the server and select Find rules to retry.</p>';
        } finally { if (id === requestId) { rulesPending = false; results.setAttribute('aria-busy', 'false'); } }
    }

    async function loadLive() {
        if (livePending || closed || document.visibilityState === 'hidden') return;
        livePending = true; liveButton.disabled = true;
        try {
            const response = await fetch('/api/rules/live');
            const data = await response.json();
            if (closed) return;
            if (!response.ok || data.status !== 'live') throw new Error(data.message || 'Live ranking is unavailable.');
            liveSummary.textContent = `${data.products.length} products with retained signals · ${data.feedback_updates.toLocaleString()} learned feedback updates · checked ${new Date(data.as_of).toLocaleTimeString()}. Refreshes every 10 seconds.`;
            const signature = JSON.stringify(data.products);
            if (signature !== liveSignature && liveResults.contains(document.activeElement)) {
                liveSummary.textContent += ' Display refresh paused while a product link is focused.';
                return;
            }
            if (signature !== liveSignature) {
                liveSignature = signature;
                liveResults.innerHTML = data.products.length ? data.products.map(item => `<article class="live-signal"><a href="/product/${item.product_id}">${escape(item.product_name)}</a><span>${item.impressions}<small>Impressions · catalog ID</small></span><span>${Number(item.positive_weight).toFixed(2)}<small>Positive weight · catalog ID</small></span><span>${Number(item.legacy_positive_weight).toFixed(2)}<small>Legacy name-keyed weight</small></span></article>`).join('') : '<p>No retained signals yet. Browse products or leave the simulator running, then refresh.</p>';
            }
        } catch (error) {
            if (closed) return;
            liveSummary.textContent = `${error.message} Historical rules are still available.`;
            liveSignature = '';
            if (!liveResults.contains(document.activeElement)) liveResults.innerHTML = '';
        } finally { livePending = false; liveButton.disabled = false; }
    }

    form.addEventListener('submit', event => { event.preventDefault(); page = 1; loadRules(); });
    previous.addEventListener('click', () => { page -= 1; loadRules(); });
    next.addEventListener('click', () => { page += 1; loadRules(); });
    liveButton.addEventListener('click', loadLive);
    liveResults.addEventListener('focusout', () => { setTimeout(loadLive, 0); });
    document.addEventListener('visibilitychange', () => { if (document.visibilityState !== 'hidden') loadLive(); });
    const timer = setInterval(loadLive, 10_000);
    const ruleTimer = setInterval(() => loadRules(true), 15_000);
    addEventListener('pagehide', () => { closed = true; requestId += 1; clearInterval(timer); clearInterval(ruleTimer); }, {once: true});
    loadRules(); loadLive();
});
