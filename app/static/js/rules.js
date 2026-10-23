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
    for (const key of ['q', 'eligible', 'sort']) if (initial.has(key)) form.elements[key].value = initial.get(key);
    let page = Math.max(1, Number(initial.get('page')) || 1);
    let requestId = 0;
    let livePending = false;
    let closed = false;
    let liveSignature = '';
    const percent = value => `${(Number(value) * 100).toFixed(2)}%`;

    async function loadRules() {
        const id = ++requestId;
        const params = new URLSearchParams(new FormData(form));
        params.set('page', String(page));
        results.setAttribute('aria-busy', 'true');
        previous.disabled = true; next.disabled = true;
        summary.textContent = 'Loading rules…';
        try {
            const response = await fetch(`/api/rules?${params}`);
            if (!response.ok) throw new Error();
            const data = await response.json();
            if (id !== requestId || closed) return;
            page = data.pagination.page;
            params.set('page', String(page));
            history.replaceState({}, '', `/rules?${params}`);
            summary.textContent = `${data.total.toLocaleString()} matches · ${data.eligible_total} coupon-eligible / ${data.historical_total.toLocaleString()} historical rules`;
            document.querySelector('[data-rule-dwell]').textContent = data.policy.dwell_threshold_seconds;
            document.querySelector('[data-rule-thresholds]').textContent = `Coupon thresholds: lift ≥ ${data.policy.min_lift} · confidence ≥ ${percent(data.policy.min_confidence)} · support ≥ ${percent(data.policy.min_support)}. Discount: ${data.policy.discount_percent}%.`;
            document.querySelector('[data-rule-version]').textContent = `· ${data.snapshot_version}`;
            results.innerHTML = data.rules.length ? data.rules.map(rule => `<article class="rule-row"><div class="rule-pair"><a href="/product/${rule.antecedent.product_id}"><small>Add to basket</small>${escape(rule.antecedent.product_name)}</a><span aria-label="then">→</span><a href="/product/${rule.consequent.product_id}"><small>Inspect this product</small>${escape(rule.consequent.product_name)}</a></div><dl class="rule-metrics"><div><dt>Lift</dt><dd>${Number(rule.lift).toFixed(2)}×</dd></div><div><dt>Confidence</dt><dd>${percent(rule.confidence)}</dd></div><div><dt>Support</dt><dd>${percent(rule.support)}</dd></div></dl><p class="rule-status">${rule.coupon_eligible ? 'Coupon-eligible pair' : 'Not coupon-eligible'}<small>${rule.coupon_eligible ? 'Subject to shopper/session conditions' : `Below ${escape(rule.failed_thresholds.join(', '))} threshold`}</small></p></article>`).join('') : '<p>No matching rules. Try another product name or select all historical rules.</p>';
            document.querySelector('[data-rule-page]').textContent = `Page ${page} of ${data.pagination.pages}`;
            previous.disabled = page <= 1; next.disabled = page >= data.pagination.pages;
        } catch (_) {
            if (id !== requestId || closed) return;
            summary.textContent = 'Rules could not be loaded.';
            results.innerHTML = '<p>Check the server and select Find rules to retry.</p>';
        } finally { if (id === requestId) results.setAttribute('aria-busy', 'false'); }
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
    addEventListener('pagehide', () => { closed = true; requestId += 1; clearInterval(timer); }, {once: true});
    loadRules(); loadLive();
});
