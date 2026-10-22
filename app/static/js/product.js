document.addEventListener('DOMContentLoaded', async () => {
    'use strict';
    const Market = window.BasketSignal;
    const productId = Number(document.body.dataset.productId);
    const stage = document.querySelector('[data-product-stage]');
    const relatedGrid = document.querySelector('[data-related-grid]');
    const basketRecommendationCard = document.querySelector('[data-featured-pairing]');
    const basketRecommendationSection = document.querySelector('[data-featured-section]');
    const viewStartedAt = performance.now();
    let product;
    let viewEnded = false;
    let recommendationRequestId = 0;
    let unsubscribeFromBasket = null;

    function quantityFor(id) { return Market.getCart().find(item => Number(item.product_id) === Number(id))?.quantity || 0; }
    function renderStage() {
        const quantity = quantityFor(product.product_id);
        document.title = `${product.product_name} · Basket Signal`;
        document.querySelector('[data-product-breadcrumb]').textContent = product.product_name;
        stage.innerHTML = `<div class="product-stage__visual">${Market.productArtwork(product, true)}<span class="visual-sticker">Rule-covered<br>product #${product.product_id}</span></div>
            <div class="product-stage__copy"><p class="product-meta"><a href="/?department_id=${product.department_id}">${Market.escapeHtml(Market.titleCase(product.department))}</a><span>·</span><a href="/?aisle_id=${product.aisle_id}">${Market.escapeHtml(Market.titleCase(product.aisle))}</a></p><h1>${Market.escapeHtml(product.product_name)}</h1><p class="product-description">A catalog item from the <strong>${Market.escapeHtml(product.aisle)}</strong> aisle. Add it to your basket and the live market can use this choice to shape later pairings.</p><div class="product-price"><strong>${Market.money(product.price_inr)}</strong><span>illustrative INR price</span></div><div class="product-buy"><button class="button button--primary" type="button" data-stage-add>${quantity ? 'Add another' : 'Add to basket'}${Market.icon('plus')}</button>${quantity ? `<span class="in-basket">${quantity} already in your basket</span>` : '<span>Persistent in this browser</span>'}</div><div class="source-note"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8v5M12 17h.01"/><circle cx="12" cy="12" r="9"/></svg><p><strong>About this listing</strong>The product name, aisle, and department come from Instacart. Price and availability are simulated for this course demonstration.</p></div></div>`;
        stage.querySelector('[data-stage-add]').addEventListener('click', () => Market.addProduct(product, 1, 'product_detail'));
    }

    async function renderBasketRecommendation() {
        const requestId = ++recommendationRequestId;
        const cart = Market.getCart();
        const requestedCartIds = cart.map(item => Number(item.product_id)).sort((left, right) => left - right);
        basketRecommendationSection.hidden = true;
        basketRecommendationCard.innerHTML = '';
        if (!cart.length) return;

        try {
            const data = await Market.getCartRecommendations(cart);
            const currentCartIds = Market.getCart().map(item => Number(item.product_id)).sort((left, right) => left - right);
            if (requestId !== recommendationRequestId || requestedCartIds.join(',') !== currentCartIds.join(',')) return;
            const cartIds = new Set(cart.map(item => Number(item.product_id)));
            const recommendation = (data.recommendations || []).find(item => !cartIds.has(Number(item.product_id)));
            if (!recommendation) return;

            const sourceLabel = recommendation.recommendation_source === 'historical_rule' ? 'Historical association rule' : 'Catalog fallback';
            const rankingLabel = recommendation.policy_mode === 'adaptive_linucb' ? 'Live ranked' : 'Historical order';
            const lift = Number(recommendation.lift);
            const liftLabel = Number.isFinite(lift) ? `<span>${Market.escapeHtml(lift.toFixed(2))}× lift</span>` : '';
            basketRecommendationCard.innerHTML = `${Market.productArtwork(recommendation)}<div class="basket-result__copy"><p class="basket-result__because">Because <strong>${Market.escapeHtml(recommendation.supporting_product_name)}</strong> is in your basket</p><a href="/product/${recommendation.product_id}" data-product-link data-source="product_basket_recommendation" data-product-id="${recommendation.product_id}" data-product-name="${Market.escapeHtml(recommendation.product_name)}" data-impression-id="${Market.escapeHtml(recommendation.impression_id || '')}" data-policy-version="${Market.escapeHtml(recommendation.policy_version || '')}" data-snapshot-version="${Market.escapeHtml(recommendation.snapshot_version || '')}"><h3>${Market.escapeHtml(recommendation.product_name)}</h3></a><p>${Market.escapeHtml(recommendation.recommendation_reason || 'Recommended for this basket')}</p><div class="basket-result__evidence"><span>${sourceLabel}</span><span>${rankingLabel}</span>${liftLabel}</div></div><button type="button" class="button button--secondary" data-featured-add>Add recommendation</button>`;
            basketRecommendationSection.hidden = false;
            basketRecommendationCard.querySelector('[data-featured-add]').addEventListener('click', () => {
                Market.addProduct(recommendation, 1, 'product_basket_recommendation');
            });
            Market.publishRecommendationImpression(recommendation, 'product_basket_recommendation').catch(() => {});
        } catch (_) {
            if (requestId === recommendationRequestId) basketRecommendationSection.hidden = true;
        }
    }

    function endView() {
        if (!product || viewEnded) return; viewEnded = true;
        const payload = {event_id: `event-${crypto.randomUUID?.() || Date.now()}`, event_type: 'product_view_ended', event_time: new Date().toISOString(), user_id: Market.identity.userId, session_id: Market.identity.sessionId, product_id: product.product_name, catalog_product_id: product.product_id, product_name: product.product_name, dwell_seconds: Math.max(0, Number(((performance.now() - viewStartedAt) / 1000).toFixed(2)))};
        navigator.sendBeacon?.('/api/events', new Blob([JSON.stringify(payload)], {type: 'application/json'})) || Market.publish('product_view_ended', payload, true).catch(() => {});
    }

    try {
        const detailParams = new URLSearchParams({user_id: Market.identity.userId, session_id: Market.identity.sessionId});
        Market.getCart().forEach(item => detailParams.append('cart_product_id', String(item.product_id)));
        const response = await fetch(`/api/products/${productId}?${detailParams}`); if (!response.ok) throw new Error();
        const data = await response.json(); product = data.product;
        unsubscribeFromBasket = Market.subscribe(() => { renderStage(); renderBasketRecommendation(); });
        renderStage(); await renderBasketRecommendation();
        relatedGrid.innerHTML = data.related_products.map(item => Market.productCard(item, {source: 'product_related'})).join('');
        Market.publish('product_view_started', {product_id: product.product_name, catalog_product_id: product.product_id, product_name: product.product_name, referrer: document.referrer || null}).catch(() => {});
        data.related_products.forEach(item => Market.publish('recommendation_impression', {product_id: item.product_name, catalog_product_id: item.product_id, product_name: item.product_name, impression_id: item.impression_id, policy_version: item.policy_version, snapshot_version: item.snapshot_version, placement: 'product_related', supporting_product_id: product.product_id}).catch(() => {}));
    } catch (_) {
        stage.innerHTML = '<div class="load-error"><strong>This product could not be loaded.</strong><p>Return to the market and choose another item.</p><a class="button button--primary" href="/">Back to market</a></div>';
    }
    addEventListener('pagehide', () => { unsubscribeFromBasket?.(); endView(); }, {once: true});
});
