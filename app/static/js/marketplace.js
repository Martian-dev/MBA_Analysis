(() => {
    'use strict';

    const catalogScope = document.querySelector('[data-catalog-scope]')?.dataset.catalogScope || 'unscoped';
    const CART_PREFIX = 'basket-signal-cart-';
    const COUPON_PREFIX = 'basket-signal-coupons-';
    const CART_KEY = `${CART_PREFIX}${catalogScope}`;
    const USER_KEY = 'basket-signal-user';
    const SESSION_KEY = 'basket-signal-session';
    const listeners = new Set();
    const offerListeners = new Set();
    let activitySource = null;
    let drawerReturnFocus = null;
    let cartRecommendationRequest = null;
    let publishQueue = Promise.resolve();

    const safeParse = (value, fallback) => {
        try { return JSON.parse(value) ?? fallback; } catch (_) { return fallback; }
    };
    const randomId = prefix => `${prefix}-${globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`}`;
    const storageValue = (storage, key, prefix) => {
        try {
            let value = storage.getItem(key);
            if (!value) { value = randomId(prefix); storage.setItem(key, value); }
            return value;
        } catch (_) { return randomId(prefix); }
    };
    const identity = {
        userId: document.querySelector('[data-shopper-id]').dataset.shopperId,
        sessionId: storageValue(sessionStorage, SESSION_KEY, 'session'),
    };
    const csrfToken = document.querySelector('[data-csrf-token]').dataset.csrfToken;
    const COUPON_KEY = `${COUPON_PREFIX}${catalogScope}-${identity.sessionId}`;
    const removeStaleCatalogState = () => {
        try {
            for (let index = localStorage.length - 1; index >= 0; index -= 1) {
                const key = localStorage.key(index);
                if ((key?.startsWith(CART_PREFIX) && key !== CART_KEY) || key?.startsWith(COUPON_PREFIX)) {
                    localStorage.removeItem(key);
                }
            }
            for (let index = sessionStorage.length - 1; index >= 0; index -= 1) {
                const key = sessionStorage.key(index);
                if (key?.startsWith(COUPON_PREFIX) && key !== COUPON_KEY) sessionStorage.removeItem(key);
            }
        } catch (_) { /* Storage can be unavailable in privacy-restricted browsers. */ }
    };
    removeStaleCatalogState();
    let cart = safeParse(localStorage.getItem(CART_KEY), {});
    let coupons = safeParse(sessionStorage.getItem(COUPON_KEY), {});

    const escapeHtml = value => String(value ?? '').replace(/[&<>'"]/g, character => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;'
    })[character]);
    const money = value => new Intl.NumberFormat('en-IN', {style: 'currency', currency: 'INR'}).format(Number(value || 0));
    const titleCase = value => String(value || '').replace(/\b\w/g, letter => letter.toUpperCase());
    const icon = (name, className = '') => {
        const icons = {
            plus: '<path d="M12 5v14M5 12h14"/>', minus: '<path d="M5 12h14"/>',
            chevron: '<path d="m9 18 6-6-6-6"/>', close: '<path d="M6 6l12 12M18 6 6 18"/>',
            leaf: '<path d="M4 13c3-7 9-10 16-9-1 8-6 14-16 16 0-3 0-5 0-7Z"/><path d="M5 18c4-5 8-8 13-11"/>',
            basket: '<path d="M4 9h16l-2 11H6zM8 9c0-4 1-6 4-6s4 2 4 6M8 13v3M12 13v3M16 13v3"/>',
        };
        return `<svg class="${className}" viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.leaf}</svg>`;
    };

    function productArtwork(product, large = false) {
        const department = String(product.department || '').toLowerCase();
        const palettes = {
            produce: ['#d9efae', '#225d45'], dairy: ['#dcebf1', '#315d72'], bakery: ['#f5d7a2', '#77411f'],
            beverages: ['#d8e8f0', '#275b72'], frozen: ['#dce9f5', '#31527c'], snacks: ['#f7dca4', '#7c4b1e'],
            pantry: ['#f2dfbd', '#6e4c2d'], meat: ['#f3d6d1', '#813e38'], breakfast: ['#f6e1ac', '#795819'],
            default: ['#e2eddd', '#315c46'],
        };
        const [background, ink] = palettes[department] || palettes.default;
        const initials = String(product.product_name || '?').split(/\s+/).filter(Boolean).slice(0, 2).map(word => word[0]).join('').toUpperCase();
        return `<div class="product-art${large ? ' product-art--large' : ''}" style="--art-bg:${background};--art-ink:${ink}" aria-hidden="true">
            <svg viewBox="0 0 160 160"><path class="art-leaf" d="M88 48c7-22 24-31 47-26-5 23-20 35-47 26Z"/><path class="art-pack" d="M41 48h78l-7 83H48z"/><path d="M49 67h63M55 118h50"/><text x="80" y="101" text-anchor="middle">${escapeHtml(initials)}</text></svg>
        </div>`;
    }

    function productCard(product, options = {}) {
        const source = options.source || 'catalog';
        const impressionId = options.impressionId || product.impression_id || '';
        return `<article class="product-card" data-product-card="${product.product_id}">
            <a class="product-card__link" href="/product/${product.product_id}" data-product-link data-product-id="${product.product_id}" data-product-name="${escapeHtml(product.product_name)}" data-source="${escapeHtml(source)}" data-impression-id="${escapeHtml(impressionId)}" data-policy-version="${escapeHtml(product.policy_version || '')}" data-snapshot-version="${escapeHtml(product.snapshot_version || '')}">
                ${productArtwork(product)}
                <span class="product-card__aisle">${escapeHtml(titleCase(product.aisle))}</span>
                <h3>${escapeHtml(product.product_name)}</h3>
            </a>
            <div class="product-card__foot"><div><strong>${money(product.price_inr)}</strong><small>illustrative INR</small></div><button class="round-add" type="button" data-add-product='${escapeHtml(JSON.stringify(product))}' data-source="${escapeHtml(source)}" aria-label="Add ${escapeHtml(product.product_name)} to basket">${icon('plus')}</button></div>
        </article>`;
    }

    function persist() {
        localStorage.setItem(CART_KEY, JSON.stringify(cart));
        sessionStorage.setItem(COUPON_KEY, JSON.stringify(coupons));
        renderCart();
        listeners.forEach(listener => listener({cart: getCart(), coupons: getCoupons()}));
    }
    const getCart = () => Object.values(cart);
    const getCoupons = () => Object.values(coupons);
    const cartQuantity = () => getCart().reduce((sum, item) => sum + item.quantity, 0);

    async function publish(eventType, details = {}, keepalive = false) {
        const payload = {
            event_id: randomId('event'), event_type: eventType, event_time: new Date().toISOString(),
            user_id: identity.userId, session_id: identity.sessionId, csrf_token: csrfToken, ...details,
        };
        publishQueue = publishQueue.catch(() => {}).then(() => fetch('/api/events', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload), keepalive,
        }));
        const response = await publishQueue;
        if (!response.ok) throw new Error('Live event stream unavailable');
        return payload;
    }

    function toast(message) {
        const region = document.querySelector('[data-toast-region]');
        if (!region) return;
        const element = document.createElement('div');
        element.className = 'toast'; element.textContent = message;
        region.append(element);
        setTimeout(() => { element.classList.add('toast--leaving'); setTimeout(() => element.remove(), 180); }, 3200);
    }

    function addProduct(product, quantity = 1, source = 'catalog') {
        const id = String(product.product_id);
        const current = cart[id];
        cart[id] = {...product, quantity: Math.min(99, (current?.quantity || 0) + quantity)};
        persist();
        publish('cart_item_added', {
            product_id: product.product_name, catalog_product_id: product.product_id,
            product_name: product.product_name, quantity, cart_quantity: cart[id].quantity, source,
            impression_id: product.impression_id, policy_version: product.policy_version,
            snapshot_version: product.snapshot_version,
        }).catch(() => toast('Added locally. The live event stream is offline.'));
        toast(`${product.product_name} added to your basket`);
    }

    function changeQuantity(productId, change) {
        const id = String(productId); const product = cart[id];
        if (!product) return;
        const next = product.quantity + change;
        if (next <= 0) { delete cart[id]; delete coupons[id]; }
        else product.quantity = Math.min(next, 99);
        persist();
        publish(change > 0 ? 'cart_item_added' : 'cart_item_removed', {
            product_id: product.product_name, catalog_product_id: product.product_id,
            product_name: product.product_name, quantity: Math.abs(change), cart_quantity: Math.max(0, next), source: 'basket',
        }).catch(() => {});
    }

    function applyCoupon(coupon, product) {
        const id = String(product.product_id);
        coupons[id] = {...coupon, product_id: product.product_id, product_name: product.product_name};
        if (!cart[id]) cart[id] = {...product, quantity: 1};
        persist();
        synchronizeCart().then(() => publish('coupon_applied', {
            product_id: product.product_name, catalog_product_id: product.product_id,
            product_name: product.product_name, coupon_id: coupon.coupon_id,
            discount_percent: Number(coupon.discount_percent),
        })).catch(() => {});
        toast(`${coupon.discount_percent}% coupon applied`);
    }

    function totals() {
        let subtotal = 0; let savings = 0;
        getCart().forEach(item => {
            const line = Number(item.price_inr) * item.quantity; subtotal += line;
            if (coupons[String(item.product_id)]) savings += line * Number(coupons[String(item.product_id)].discount_percent || 0) / 100;
        });
        return {subtotal, savings, total: Math.max(0, subtotal - savings)};
    }

    function renderCart() {
        const items = getCart(); const quantity = cartQuantity(); const values = totals();
        document.querySelectorAll('[data-cart-count]').forEach(element => { element.textContent = quantity; });
        const lines = document.querySelector('[data-cart-lines]');
        const empty = document.querySelector('[data-cart-empty]');
        if (lines) {
            lines.innerHTML = items.map(item => `<li class="cart-line">${productArtwork(item)}<div class="cart-line__copy"><a href="/product/${item.product_id}">${escapeHtml(item.product_name)}</a><small>${money(item.price_inr)} each${coupons[String(item.product_id)] ? ` · ${coupons[String(item.product_id)].discount_percent}% off` : ''}</small><div class="quantity-control" aria-label="Quantity for ${escapeHtml(item.product_name)}"><button type="button" data-cart-change="-1" data-product-id="${item.product_id}" aria-label="Remove one ${escapeHtml(item.product_name)}">${icon('minus')}</button><span>${item.quantity}</span><button type="button" data-cart-change="1" data-product-id="${item.product_id}" aria-label="Add one ${escapeHtml(item.product_name)}">${icon('plus')}</button></div></div><strong>${money(Number(item.price_inr) * item.quantity)}</strong></li>`).join('');
            lines.hidden = !items.length;
        }
        if (empty) empty.hidden = Boolean(items.length);
        document.querySelectorAll('[data-cart-subtotal]').forEach(element => { element.textContent = money(values.subtotal); });
        document.querySelectorAll('[data-cart-savings]').forEach(element => { element.textContent = `−${money(values.savings)}`; });
        document.querySelectorAll('[data-cart-total]').forEach(element => { element.textContent = money(values.total); });
        document.querySelectorAll('[data-checkout-link]').forEach(link => {
            link.classList.toggle('is-disabled', !items.length); link.setAttribute('aria-disabled', String(!items.length));
        });
        if (items.length) fetchPairings(); else {
            const section = document.querySelector('[data-cart-pairings]'); if (section) section.hidden = true;
        }
    }

    async function getCartRecommendations(cartItems = getCart()) {
        const cartIds = cartItems.map(item => Number(item.product_id));
        if (!cartIds.length) return {recommendations: []};
        const requestKey = [...cartIds].sort((left, right) => left - right).join(',');
        if (cartRecommendationRequest?.key === requestKey) return cartRecommendationRequest.promise;

        const promise = fetch('/api/cart-recommend', {
            method: 'POST',
            headers: {'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken},
            body: JSON.stringify({cart: cartIds, user_id: identity.userId, session_id: identity.sessionId}),
        }).then(response => {
            if (!response.ok) throw new Error('Basket recommendations unavailable');
            return response.json();
        });
        cartRecommendationRequest = {key: requestKey, promise};
        try { return await promise; }
        finally {
            if (cartRecommendationRequest?.promise === promise) cartRecommendationRequest = null;
        }
    }

    function publishRecommendationImpression(product, placement) {
        return publish('recommendation_impression', {
            product_id: product.product_name, catalog_product_id: product.product_id,
            product_name: product.product_name, impression_id: product.impression_id,
            policy_version: product.policy_version, snapshot_version: product.snapshot_version,
            placement, supporting_product_id: product.supporting_product_id,
        });
    }

    async function fetchPairings() {
        const section = document.querySelector('[data-cart-pairings]');
        const list = document.querySelector('[data-pairing-list]');
        if (!section || !list) return;
        try {
            const data = await getCartRecommendations(); const pairings = data.recommendations?.slice(0, 3) || [];
            section.hidden = !pairings.length;
            list.innerHTML = pairings.map(product => `<div class="mini-pairing">${productArtwork(product)}<a href="/product/${product.product_id}" data-product-link data-source="cart_pairing" data-product-id="${product.product_id}" data-product-name="${escapeHtml(product.product_name)}" data-impression-id="${escapeHtml(product.impression_id)}" data-policy-version="${escapeHtml(product.policy_version || '')}" data-snapshot-version="${escapeHtml(product.snapshot_version || '')}"><strong>${escapeHtml(product.product_name)}</strong><small>with ${escapeHtml(product.supporting_product_name)}</small></a><button type="button" data-add-product='${escapeHtml(JSON.stringify(product))}' data-source="cart_pairing" aria-label="Add ${escapeHtml(product.product_name)}">${icon('plus')}</button></div>`).join('');
            pairings.forEach(product => publishRecommendationImpression(product, 'cart_drawer').catch(() => {}));
        } catch (_) { section.hidden = true; }
    }

    function openCart() {
        const drawer = document.querySelector('[data-cart-drawer], #cart-drawer');
        const scrim = document.querySelector('[data-cart-scrim]'); if (!drawer || !scrim) return;
        if (drawer.classList.contains('is-open')) return;
        drawerReturnFocus = document.activeElement; scrim.hidden = false;
        requestAnimationFrame(() => { drawer.classList.add('is-open'); scrim.classList.add('is-open'); });
        drawer.removeAttribute('inert'); drawer.setAttribute('aria-hidden', 'false'); document.body.classList.add('drawer-open');
        document.querySelectorAll('[data-open-cart]').forEach(button => button.setAttribute('aria-expanded', 'true'));
        drawer.querySelector('[data-close-cart]')?.focus();
        publish('basket_opened').catch(() => {});
    }
    function closeCart() {
        const drawer = document.querySelector('#cart-drawer'); const scrim = document.querySelector('[data-cart-scrim]');
        if (!drawer || !scrim) return;
        if (!drawer.classList.contains('is-open')) return;
        drawer.classList.remove('is-open'); scrim.classList.remove('is-open'); drawer.setAttribute('aria-hidden', 'true'); drawer.setAttribute('inert', '');
        document.body.classList.remove('drawer-open'); document.querySelectorAll('[data-open-cart]').forEach(button => button.setAttribute('aria-expanded', 'false'));
        setTimeout(() => { scrim.hidden = true; }, 220);
        drawerReturnFocus?.focus?.(); drawerReturnFocus = null;
    }

    async function handleOffer(payload) {
        offerListeners.forEach(listener => listener());
        let product = getCart().find(item => item.product_name === payload.product_id);
        if (!product) {
            try {
                const response = await fetch(`/api/products?q=${encodeURIComponent(payload.product_id)}&per_page=20`);
                const data = await response.json(); product = data.products.find(item => item.product_name === payload.product_id);
            } catch (_) { return; }
        }
        if (!product) return;
        const region = document.querySelector('[data-live-offer]'); if (!region) return;
        region.hidden = false;
        region.innerHTML = `<div class="coupon-stamp"><small>Live basket offer</small><strong>${escapeHtml(payload.discount_percent)}% off</strong></div><div><strong>${escapeHtml(product.product_name)}</strong><p>Because ${escapeHtml(payload.supporting_cart_item || 'your basket')} is already in your basket.</p><button class="text-button" type="button" data-apply-live-coupon>Apply to basket</button></div>`;
        region.querySelector('[data-apply-live-coupon]').addEventListener('click', () => applyCoupon(payload, product));
    }

    async function connectActivity() {
        if (!document.querySelector('[data-live-offer]')) return;
        try {
            const params = new URLSearchParams({user_id: identity.userId, session_id: identity.sessionId});
            const response = await fetch(`/api/activity?${params}`); if (!response.ok) return;
            const data = await response.json(); data.events.forEach(entry => { if (entry.kind === 'coupon_issued') handleOffer(entry.payload); });
            params.set('since', data.last_id); activitySource = new EventSource(`/api/activity/stream?${params}`);
            activitySource.onmessage = event => { const entry = JSON.parse(event.data); if (entry.kind === 'coupon_issued') handleOffer(entry.payload); };
        } catch (_) { /* The storefront remains usable without the live stack. */ }
    }

    document.addEventListener('click', event => {
        const add = event.target.closest('[data-add-product]');
        if (add) { event.preventDefault(); addProduct(safeParse(add.dataset.addProduct, {}), 1, add.dataset.source || 'catalog'); return; }
        const change = event.target.closest('[data-cart-change]');
        if (change) { changeQuantity(change.dataset.productId, Number(change.dataset.cartChange)); return; }
        if (event.target.closest('[data-open-cart]')) { openCart(); return; }
        if (event.target.closest('[data-close-cart], [data-cart-scrim]')) { closeCart(); return; }
        const productLink = event.target.closest('[data-product-link]');
        if (!productLink || productLink.dataset.source === 'catalog' || event.defaultPrevented) return;
        const sameTab = event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey
            && (!productLink.target || productLink.target === '_self');
        if (sameTab) event.preventDefault();
        const delivery = publish('recommendation_clicked', {
            product_id: productLink.dataset.productName, catalog_product_id: Number(productLink.dataset.productId),
            product_name: productLink.dataset.productName, impression_id: productLink.dataset.impressionId || undefined,
            policy_version: productLink.dataset.policyVersion || undefined,
            snapshot_version: productLink.dataset.snapshotVersion || undefined,
            placement: productLink.dataset.source,
        }, true).catch(() => {});
        if (sameTab) {
            // Keep the cart and click in order before leaving this document.
            // An offline event pipeline must not prevent product browsing.
            let navigationTimer;
            Promise.race([delivery, new Promise(resolve => { navigationTimer = setTimeout(resolve, 2500); })])
                .then(() => { clearTimeout(navigationTimer); window.location.assign(productLink.href); });
        }
    });
    document.addEventListener('keydown', event => {
        const drawer = document.querySelector('#cart-drawer');
        const drawerOpen = drawer?.classList.contains('is-open');
        if (event.key === 'Escape' && drawerOpen) { closeCart(); return; }
        if (event.key === 'Tab' && drawerOpen) {
                const focusable = [...drawer.querySelectorAll('a[href]:not([aria-disabled="true"]), button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])')]
                    .filter(element => !element.closest('[hidden]'));
                const first = focusable[0]; const last = focusable[focusable.length - 1];
                if (!first) { event.preventDefault(); drawer.focus(); }
                else if (!drawer.contains(document.activeElement)) { event.preventDefault(); first.focus(); }
                else if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
                else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
        }
        if (!drawerOpen && event.key === '/' && !/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)) { event.preventDefault(); document.querySelector('#catalog-search, #header-search')?.focus(); }
    });
    document.querySelectorAll('[data-checkout-link]').forEach(link => link.addEventListener('click', event => { if (!getCart().length) event.preventDefault(); }));

    window.BasketSignal = {
        identity, csrfToken, escapeHtml, money, titleCase, icon, productArtwork, productCard,
        publish, publishRecommendationImpression, getCartRecommendations,
        toast, addProduct, changeQuantity, getCart, getCoupons, totals, persist,
        subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
        subscribeOffers(listener) { offerListeners.add(listener); return () => offerListeners.delete(listener); },
        async getAvailableOffers() {
            const params = new URLSearchParams({user_id: identity.userId, session_id: identity.sessionId});
            const response = await fetch(`/api/coupons/available?${params}`);
            if (!response.ok) throw new Error('Available offers could not be checked');
            return (await response.json()).coupons || [];
        },
        clearOrder() { cart = {}; coupons = {}; localStorage.removeItem(CART_KEY); sessionStorage.removeItem(COUPON_KEY); persist(); synchronizeCart().catch(() => {}); },
        newOrderId() { return `BS-${(globalThis.crypto?.randomUUID?.() || randomId('order')).replace(/[^A-Za-z0-9-]/g, '')}`; },
    };
    function synchronizeCart() {
        return publish('cart_synchronized', {cart: getCart().map(item => ({
            product_id: item.product_name, catalog_product_id: item.product_id, quantity: item.quantity,
        }))});
    }
    window.BasketSignal.ready = synchronizeCart().catch(() => {});
    renderCart(); connectActivity();
})();
