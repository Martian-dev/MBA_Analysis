document.addEventListener('DOMContentLoaded', () => {
    let allProducts = [];
    let visibleProducts = [];
    const cart = {};
    const liveOffers = {};
    const appliedCoupons = {};
    let latestOffer = null;
    let activitySource = null;

    const gridContainer = document.querySelector('#product-grid');
    const searchBar = document.querySelector('#search-bar');
    const cartItemsContainer = document.querySelector('#cart-items');
    const cartCountElement = document.querySelector('#cart-count');
    const catalogCount = document.querySelector('#catalog-count');
    const liveOfferContainer = document.querySelector('#live-offer');
    const decisionState = document.querySelector('#decision-state');
    const storeStatus = document.querySelector('#store-status');

    const text = value => String(value ?? '');
    const escapeHtml = value => text(value).replace(/[&<>'"]/g, character => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;'
    }[character]));

    // The source data contains no prices. This stable synthetic price lets the
    // demo show a savings amount without presenting it as historical data.
    function demoPrice(productName) {
        let hash = 0;
        for (const character of productName) hash = ((hash << 5) - hash + character.charCodeAt(0)) | 0;
        return 2.49 + (Math.abs(hash) % 1351) / 100;
    }

    function money(value) {
        return `$${Number(value || 0).toFixed(2)}`;
    }

    function getEmoji(name) {
        const value = name.toLowerCase();
        if (value.includes('banana') || value.includes('apple') || value.includes('berry')) return '🍎';
        if (value.includes('organic')) return '🌱';
        if (value.includes('water')) return '💧';
        if (value.includes('cheese')) return '🧀';
        if (value.includes('milk')) return '🥛';
        if (value.includes('bread') || value.includes('tortilla')) return '🥖';
        return '📦';
    }

    function offerFor(productName) {
        return liveOffers[productName] || null;
    }

    function renderProducts(productsToRender) {
        visibleProducts = productsToRender;
        gridContainer.innerHTML = '';
        catalogCount.textContent = `${productsToRender.length} of ${allProducts.length} representative products`;

        productsToRender.forEach(productName => {
            const offer = offerFor(productName);
            const isApplied = Boolean(appliedCoupons[productName] && cart[productName]);
            const card = document.createElement('article');
            card.className = 'product-card';
            card.dataset.product = productName;
            card.innerHTML = `
                <div>
                    <div class="product-card-top"><span class="product-kicker">Basket item</span><span class="price">${money(demoPrice(productName))}</span></div>
                    <div class="product-icon" aria-hidden="true">${getEmoji(productName)}</div>
                    ${offer ? `<span class="offer-badge${isApplied ? ' applied' : ''}">${isApplied ? 'Offer applied' : `${escapeHtml(offer.discount_percent)}% offer live`}</span>` : ''}
                    <div class="product-name">${escapeHtml(productName)}</div>
                    <span class="price-note">illustrative demo price</span>
                </div>
                <div class="action-area" data-action-item="${escapeHtml(productName)}"></div>
            `;
            gridContainer.appendChild(card);
            updateActionUI(productName);
        });
    }

    function updateActionUI(productName) {
        const qty = cart[productName] || 0;
        document.querySelectorAll('[data-action-item]').forEach(area => {
            if (area.dataset.actionItem !== productName) return;
            const isSubRow = area.closest('.sub-row') !== null;
            if (qty === 0) {
                area.innerHTML = `<button class="add-to-cart-btn" type="button">Add to basket</button>`;
                area.querySelector('button').addEventListener('click', event => modifyCart(event, productName, 1, !isSubRow));
            } else {
                area.innerHTML = `
                    <div class="qty-control" aria-label="Quantity for ${escapeHtml(productName)}">
                        <button class="qty-btn" type="button" aria-label="Remove one ${escapeHtml(productName)}">−</button>
                        <span class="qty-count">${qty}</span>
                        <button class="qty-btn" type="button" aria-label="Add one ${escapeHtml(productName)}">+</button>
                    </div>`;
                const buttons = area.querySelectorAll('button');
                buttons[0].addEventListener('click', event => modifyCart(event, productName, -1, false));
                buttons[1].addEventListener('click', event => modifyCart(event, productName, 1, false));
            }
        });
    }

    searchBar.addEventListener('input', event => {
        const term = event.target.value.trim().toLowerCase();
        renderProducts(allProducts.filter(product => product.toLowerCase().includes(term)));
        document.querySelectorAll('.sub-row').forEach(element => element.remove());
    });

    async function modifyCart(event, productName, change, canTriggerSubRow) {
        if (event) event.stopPropagation();
        cart[productName] = (cart[productName] || 0) + change;
        if (cart[productName] <= 0) {
            delete cart[productName];
            delete appliedCoupons[productName];
        }
        updateActionUI(productName);
        updateCartSidebar();
        if (change > 0 && canTriggerSubRow) await triggerSubRow(productName);
    }

    function updateCartSidebar() {
        cartItemsContainer.innerHTML = '';
        let count = 0;
        let subtotal = 0;
        let savings = 0;
        const items = Object.entries(cart);

        if (items.length === 0) {
            cartItemsContainer.innerHTML = '<li class="empty-cart">Add an item to start the decision loop.</li>';
        } else {
            items.forEach(([item, quantity]) => {
                const price = demoPrice(item);
                const offer = appliedCoupons[item];
                const itemSavings = offer ? price * (Number(offer.discount_percent) / 100) * quantity : 0;
                count += quantity;
                subtotal += price * quantity;
                savings += itemSavings;
                cartItemsContainer.innerHTML += `
                    <li>
                        <span><span class="cart-item-name">${escapeHtml(item)}</span><span class="cart-item-price">${money(price)} each${offer ? ` · ${escapeHtml(offer.discount_percent)}% off` : ''}</span>${offer ? '<span class="cart-offer-tag">coupon applied</span>' : ''}</span>
                        <span class="cart-item-side"><strong>${money(price * quantity - itemSavings)}</strong>x${quantity}</span>
                    </li>`;
            });
        }

        cartCountElement.textContent = count;
        document.querySelector('#basket-state').textContent = count ? `${count} item${count === 1 ? '' : 's'}` : 'Empty';
        document.querySelector('#cart-subtotal').textContent = money(subtotal);
        document.querySelector('#cart-savings').textContent = `−${money(savings)}`;
        document.querySelector('#cart-total').textContent = money(Math.max(0, subtotal - savings));
        document.querySelector('.checkout-btn').disabled = count === 0;
        fetchCartRecommendations();
        renderLiveOffer();
    }

    async function triggerSubRow(productName) {
        document.querySelectorAll('.sub-row').forEach(element => element.remove());
        const response = await fetch(`/api/recommend?item=${encodeURIComponent(productName)}`);
        const data = await response.json();
        if (!data.recommendations || data.recommendations.length === 0) return;

        const subRow = document.createElement('div');
        subRow.className = 'sub-row';
        const miniCards = data.recommendations.map(item => `
            <div class="mini-card">
                <div aria-hidden="true" style="font-size: 2rem;">${getEmoji(item)}</div>
                <div style="font-size: .78rem; font-weight: 700;">${escapeHtml(item)}</div>
                <div style="font: 500 10px var(--mono);">${money(demoPrice(item))}</div>
                <div style="width: 100%;" data-action-item="${escapeHtml(item)}"></div>
            </div>`).join('');
        subRow.innerHTML = `<h4>Because you added ${escapeHtml(productName)}…</h4><div class="sub-row-items">${miniCards}</div>`;

        const activeCard = Array.from(document.querySelectorAll('.product-card')).find(card => card.dataset.product === productName);
        if (!activeCard) return;
        let lastCardInRow = activeCard;
        let nextCard = activeCard.nextElementSibling;
        while (nextCard && nextCard.classList.contains('product-card')) {
            if (nextCard.offsetTop > activeCard.offsetTop) break;
            lastCardInRow = nextCard;
            nextCard = nextCard.nextElementSibling;
        }
        lastCardInRow.insertAdjacentElement('afterend', subRow);
        data.recommendations.forEach(updateActionUI);
    }

    async function fetchCartRecommendations() {
        let globalRecContainer = document.querySelector('#global-recs');
        if (!globalRecContainer) {
            globalRecContainer = document.createElement('div');
            globalRecContainer.id = 'global-recs';
            globalRecContainer.className = 'cart-recommendations';
            document.querySelector('.cart-container').appendChild(globalRecContainer);
        }
        const itemsInCart = Object.keys(cart);
        if (itemsInCart.length === 0) {
            globalRecContainer.innerHTML = '';
            return;
        }
        const response = await fetch('/api/cart-recommend', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({cart: itemsInCart})
        });
        const data = await response.json();
        if (!data.recommendations || data.recommendations.length === 0) {
            globalRecContainer.innerHTML = '';
            return;
        }
        globalRecContainer.innerHTML = `<h3>Suggested from your basket</h3>${data.recommendations.map(item => `
            <div class="cart-rec-item"><span>${getEmoji(item)} ${escapeHtml(item)}</span><button class="add-rec-btn" type="button">+ Add</button></div>`).join('')}`;
        globalRecContainer.querySelectorAll('.cart-rec-item').forEach((row, index) => {
            row.querySelector('button').addEventListener('click', event => modifyCart(event, data.recommendations[index], 1, false));
        });
    }

    function setStoreStatus(state, label) {
        storeStatus.className = `store-status ${state}`;
        storeStatus.innerHTML = `<i></i> ${escapeHtml(label)}`;
    }

    function renderLiveOffer() {
        if (!latestOffer) {
            decisionState.textContent = 'Listening';
            liveOfferContainer.className = 'live-offer empty-offer';
            liveOfferContainer.innerHTML = '<span class="offer-spark" aria-hidden="true">✦</span><strong>Waiting for a qualifying view</strong><p>When the stream sees a long view on a correlated product, the offer will appear here with its evidence.</p>';
            return;
        }

        const product = latestOffer.product_id;
        const applied = Boolean(appliedCoupons[product] && cart[product]);
        const price = demoPrice(product);
        const savings = price * Number(latestOffer.discount_percent || 0) / 100;
        decisionState.textContent = applied ? 'Applied' : 'Offer ready';
        liveOfferContainer.className = 'live-offer offer-card';
        liveOfferContainer.innerHTML = `
            <div class="offer-kicker">COUPON ISSUED · LIVE</div>
            <h3>${escapeHtml(product)}</h3>
            <p class="offer-subtitle">A targeted offer is ready for this hesitant shopper.</p>
            <div class="offer-value-row"><span class="offer-value">${escapeHtml(latestOffer.discount_percent)}%</span><span class="offer-value-copy">off this item<br><small>≈ ${money(savings)} at demo price</small></span></div>
            <dl class="offer-evidence"><div><dt>Cart evidence</dt><dd>${escapeHtml(latestOffer.supporting_cart_item)}</dd></div><div><dt>Dwell</dt><dd>${escapeHtml(latestOffer.reason?.dwell_seconds)}s</dd></div><div><dt>Rule lift</dt><dd>${Number(latestOffer.lift).toFixed(2)}</dd></div><div><dt>Confidence</dt><dd>${(Number(latestOffer.confidence) * 100).toFixed(1)}%</dd></div></dl>
            <span class="offer-id">ID · ${escapeHtml(latestOffer.coupon_id)}</span>
            <button class="offer-action${applied ? ' applied' : ''}" type="button">${applied ? 'Coupon applied to this basket' : `Add ${escapeHtml(product)} & apply ${escapeHtml(latestOffer.discount_percent)}% off`}</button>`;
        if (!applied) liveOfferContainer.querySelector('button').addEventListener('click', () => applyLatestOffer());
    }

    function applyLatestOffer() {
        if (!latestOffer) return;
        const product = latestOffer.product_id;
        cart[product] = (cart[product] || 0) + 1;
        appliedCoupons[product] = latestOffer;
        renderProducts(visibleProducts);
        updateCartSidebar();
    }

    function receiveActivity(entry) {
        if (entry.kind === 'coupon_issued') {
            latestOffer = entry.payload;
            liveOffers[entry.payload.product_id] = entry.payload;
            renderProducts(visibleProducts);
            renderLiveOffer();
        } else if (entry.kind === 'coupon_suppressed' && !latestOffer) {
            decisionState.textContent = 'Offer withheld';
        }
    }

    function connectActivity(cursor) {
        if (activitySource) activitySource.close();
        activitySource = new EventSource(`/api/activity/stream?since=${encodeURIComponent(cursor)}`);
        activitySource.onopen = () => setStoreStatus('live', 'Personalization online');
        activitySource.onmessage = event => receiveActivity(JSON.parse(event.data));
        activitySource.onerror = () => setStoreStatus('down', 'Reconnecting to stream');
    }

    async function connectToPipeline() {
        try {
            const response = await fetch('/api/activity');
            if (!response.ok) throw new Error('Pipeline unavailable');
            const data = await response.json();
            data.events.forEach(receiveActivity);
            connectActivity(data.last_id);
        } catch (error) {
            setStoreStatus('down', 'Pipeline offline');
            setTimeout(connectToPipeline, 4000);
        }
    }

    fetch('/api/products')
        .then(response => response.json())
        .then(data => {
            allProducts = data.products;
            visibleProducts = allProducts;
            document.querySelector('#loading-message').remove();
            renderProducts(allProducts);
        })
        .catch(() => {
            document.querySelector('#loading-message').textContent = 'The catalog could not be loaded. Check the Flask log.';
        });

    connectToPipeline();
    updateCartSidebar();
});
