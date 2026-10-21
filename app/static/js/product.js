document.addEventListener('DOMContentLoaded', async () => {
    'use strict';
    const Market = window.BasketSignal;
    const productId = Number(document.body.dataset.productId);
    const stage = document.querySelector('[data-product-stage]');
    const relatedGrid = document.querySelector('[data-related-grid]');
    const featured = document.querySelector('[data-featured-pairing]');
    const viewStartedAt = performance.now();
    let product;
    let viewEnded = false;

    function quantityFor(id) { return Market.getCart().find(item => Number(item.product_id) === Number(id))?.quantity || 0; }
    function renderStage() {
        const quantity = quantityFor(product.product_id);
        document.title = `${product.product_name} · Basket Signal`;
        document.querySelector('[data-product-breadcrumb]').textContent = product.product_name;
        stage.innerHTML = `<div class="product-stage__visual">${Market.productArtwork(product, true)}<span class="visual-sticker">Full catalog<br>product #${product.product_id}</span></div>
            <div class="product-stage__copy"><p class="product-meta"><a href="/?department_id=${product.department_id}">${Market.escapeHtml(Market.titleCase(product.department))}</a><span>·</span><a href="/?aisle_id=${product.aisle_id}">${Market.escapeHtml(Market.titleCase(product.aisle))}</a></p><h1>${Market.escapeHtml(product.product_name)}</h1><p class="product-description">A catalog item from the <strong>${Market.escapeHtml(product.aisle)}</strong> aisle. Add it to your basket and the live market can use this choice to shape later pairings.</p><div class="product-price"><strong>${Market.money(product.price_inr)}</strong><span>illustrative INR price</span></div><div class="product-buy"><button class="button button--primary" type="button" data-stage-add>${quantity ? 'Add another' : 'Add to basket'}${Market.icon('plus')}</button>${quantity ? `<span class="in-basket">${quantity} already in your basket</span>` : '<span>Persistent in this browser</span>'}</div><div class="source-note"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8v5M12 17h.01"/><circle cx="12" cy="12" r="9"/></svg><p><strong>About this listing</strong>The product name, aisle, and department come from Instacart. Price and availability are simulated for this course demonstration.</p></div></div>`;
        stage.querySelector('[data-stage-add]').addEventListener('click', () => { Market.addProduct(product, 1, 'product_detail'); renderStage(); renderFeatured(); });
    }

    function renderFeatured() {
        const cart = Market.getCart();
        if (!cart.length) { featured.innerHTML = '<div class="pairing-placeholder">Add something to your basket and we’ll connect the dots here.</div>'; return; }
        const supporting = cart[cart.length - 1];
        featured.innerHTML = `<div class="pairing-rail__item"><small>In your basket</small>${Market.productArtwork(supporting)}<strong>${Market.escapeHtml(supporting.product_name)}</strong></div><div class="pairing-connector" aria-hidden="true"><span></span><svg viewBox="0 0 24 24"><path d="M5 12h14M13 6l6 6-6 6"/></svg></div><div class="pairing-rail__item pairing-rail__item--next"><small>You’re considering</small>${Market.productArtwork(product)}<strong>${Market.escapeHtml(product.product_name)}</strong><button type="button" class="text-button" data-featured-add>Add the pairing</button></div>`;
        featured.querySelector('[data-featured-add]').addEventListener('click', () => Market.addProduct(product, 1, 'intent_bridge'));
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
        renderStage(); renderFeatured();
        relatedGrid.innerHTML = data.related_products.map(item => Market.productCard(item, {source: 'product_related'})).join('');
        Market.publish('product_view_started', {product_id: product.product_name, catalog_product_id: product.product_id, product_name: product.product_name, referrer: document.referrer || null}).catch(() => {});
        data.related_products.forEach(item => Market.publish('recommendation_impression', {product_id: item.product_name, catalog_product_id: item.product_id, product_name: item.product_name, impression_id: item.impression_id, placement: 'product_related', supporting_product_id: product.product_id}).catch(() => {}));
    } catch (_) {
        stage.innerHTML = '<div class="load-error"><strong>This product could not be loaded.</strong><p>Return to the market and choose another item.</p><a class="button button--primary" href="/">Back to market</a></div>';
    }
    addEventListener('pagehide', endView, {once: true});
});
