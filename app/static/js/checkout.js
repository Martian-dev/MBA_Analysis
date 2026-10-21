document.addEventListener('DOMContentLoaded', () => {
    'use strict';
    const Market = window.BasketSignal;
    const form = document.querySelector('[data-checkout-form]');
    const lines = document.querySelector('[data-checkout-lines]');
    const errorBox = document.querySelector('[data-form-error]');
    const cart = Market.getCart(); const coupons = Market.getCoupons(); const totals = Market.totals();
    const pendingKey = 'basket-signal-pending-order';
    let orderId = sessionStorage.getItem(pendingKey) || Market.newOrderId();
    sessionStorage.setItem(pendingKey, orderId);

    if (!cart.length) {
        form.innerHTML = '<div class="checkout-empty"><strong>Your basket is empty.</strong><p>Choose a few groceries before checking out.</p><a class="button button--primary" href="/">Browse the market</a></div>';
    }
    lines.innerHTML = cart.map(item => `<li>${Market.productArtwork(item)}<div><strong>${Market.escapeHtml(item.product_name)}</strong><small>${item.quantity} × ${Market.money(item.price_inr)}${coupons.find(coupon => Number(coupon.product_id) === Number(item.product_id)) ? ' · coupon applied' : ''}</small></div><b>${Market.money(item.quantity * Number(item.price_inr))}</b></li>`).join('');
    document.querySelector('[data-checkout-subtotal]').textContent = Market.money(totals.subtotal);
    document.querySelector('[data-checkout-savings]').textContent = `−${Market.money(totals.savings)}`;
    document.querySelector('[data-checkout-total]').textContent = Market.money(totals.total);
    Market.publish('checkout_started', {order_id: orderId, product_ids: cart.map(item => item.product_name), catalog_product_ids: cart.map(item => item.product_id), basket_quantity: cart.reduce((sum, item) => sum + item.quantity, 0)}).catch(() => {});

    function validateField(field) {
        const error = field.parentElement.querySelector('[data-error-for]'); let message = '';
        if (!field.value.trim()) message = 'This field is required.';
        else if (field.type === 'email' && !field.validity.valid) message = 'Enter a valid email address.';
        field.setAttribute('aria-invalid', String(Boolean(message))); error.textContent = message; return !message;
    }
    form.querySelectorAll('input, textarea').forEach(field => field.addEventListener('blur', () => validateField(field)));
    form.addEventListener('submit', async event => {
        event.preventDefault(); const fields = [...form.querySelectorAll('input, textarea')]; const invalid = fields.filter(field => !validateField(field));
        if (invalid.length) { errorBox.textContent = 'Check the highlighted delivery details.'; errorBox.hidden = false; invalid[0].focus(); return; }
        const submit = form.querySelector('[type="submit"]'); submit.disabled = true; submit.classList.add('is-loading'); submit.firstChild.textContent = 'Confirming order ';
        const values = Object.fromEntries(new FormData(form));
        const payload = {...values, order_id: orderId, user_id: Market.identity.userId, session_id: Market.identity.sessionId, cart: cart.map(item => ({product_id: Number(item.product_id), quantity: item.quantity})), coupons};
        try {
            const response = await fetch('/api/checkout', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)}); const data = await response.json();
            if (!response.ok) throw new Error(data.error || 'The order could not be saved.');
            Market.publish('checkout_completed', {order_id: orderId, product_ids: cart.map(item => item.product_name), catalog_product_ids: cart.map(item => item.product_id), basket_quantity: cart.reduce((sum, item) => sum + item.quantity, 0), total_inr: data.order.total_inr}, true).catch(() => {});
            Market.clearOrder(); sessionStorage.removeItem(pendingKey); location.assign(`/order/${encodeURIComponent(orderId)}?user_id=${encodeURIComponent(Market.identity.userId)}`);
        } catch (error) { errorBox.textContent = `${error.message} Check the server and try again.`; errorBox.hidden = false; submit.disabled = false; submit.classList.remove('is-loading'); submit.firstChild.textContent = 'Confirm demo order '; }
    });
});
