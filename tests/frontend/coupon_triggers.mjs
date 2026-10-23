import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync('app/static/js/marketplace.js', 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
const storage = () => {
    const values = new Map();
    return {
        get length() { return values.size; },
        key: index => [...values.keys()][index],
        getItem: key => values.get(key) ?? null,
        setItem: (key, value) => values.set(key, value),
        removeItem: key => values.delete(key),
    };
};

function storefront() {
    const handlers = {};
    const requests = [];
    const navigation = [];
    const timers = new Map();
    const classes = new Set();
    const drawer = {
        classList: {contains: key => classes.has(key), add: key => classes.add(key), remove: key => classes.delete(key)},
        removeAttribute() {}, setAttribute() {}, querySelector: () => ({focus() {}}),
    };
    const scrim = {classList: {add() {}, remove() {}}, hidden: true};
    let releaseSnapshot;
    const window = {location: {assign: href => navigation.push(href)}};
    vm.runInNewContext(source, {
        window, localStorage: storage(), sessionStorage: storage(), URLSearchParams,
        document: {
            activeElement: null, body: {classList: {add() {}, remove() {}}},
            querySelectorAll: () => [],
            querySelector: selector => ({
                '[data-shopper-id]': {dataset: {shopperId: 'shopper-test'}},
                '[data-csrf-token]': {dataset: {csrfToken: 'csrf'}},
                '[data-cart-drawer], #cart-drawer': drawer,
                '#cart-drawer': drawer, '[data-cart-scrim]': scrim,
            })[selector] || null,
            addEventListener: (kind, listener) => { handlers[kind] = listener; },
        },
        requestAnimationFrame: callback => callback(),
        setTimeout: callback => { const id = timers.size + 1; timers.set(id, callback); return id; },
        clearTimeout: id => timers.delete(id),
        fetch: async (url, options) => {
            const payload = JSON.parse(options.body);
            requests.push(payload);
            if (payload.event_type === 'cart_synchronized') await new Promise(resolve => { releaseSnapshot = resolve; });
            return {ok: true};
        },
    }, {filename: 'marketplace.js'});
    return {
        requests, navigation, timers,
        releaseSnapshot: () => releaseSnapshot(),
        click(selector, options = {}) {
            const link = {href: '/product/13176', dataset: {source: 'product_next_pick', productId: '13176', productName: 'Bag of Organic Bananas'}, ...options.link};
            const event = {button: 0, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; },
                target: {closest: value => value === selector ? link : null}, ...options};
            handlers.click(event);
            return event;
        },
    };
}

const page = storefront();
await tick();
const click = page.click('[data-product-link]');
assert.equal(click.defaultPrevented, true);
assert.deepEqual(page.navigation, []);
assert.deepEqual(page.requests.map(item => item.event_type), ['cart_synchronized']);
page.releaseSnapshot();
await tick();
assert.deepEqual(page.requests.map(item => item.event_type), ['cart_synchronized', 'recommendation_clicked']);
assert.deepEqual(page.navigation, ['/product/13176']);
assert.equal(page.timers.size, 0);

const offline = storefront();
await tick();
offline.click('[data-product-link]');
[...offline.timers.values()][0]();
await tick();
assert.deepEqual(offline.navigation, ['/product/13176'], 'Unavailable event delivery must not trap navigation');

const modified = storefront();
await tick();
assert.equal(modified.click('[data-product-link]', {ctrlKey: true}).defaultPrevented, false);
assert.equal(modified.click('[data-product-link]', {link: {dataset: {source: 'catalog'}}}).defaultPrevented, false);
modified.releaseSnapshot();
await tick();
assert.deepEqual(modified.navigation, []);

const basket = storefront();
await tick();
basket.releaseSnapshot();
await tick();
assert.equal(basket.requests.some(item => item.event_type === 'basket_opened'), false);
basket.click('[data-open-cart]');
basket.click('[data-open-cart]');
await tick();
assert.equal(basket.requests.filter(item => item.event_type === 'basket_opened').length, 1);
basket.click('[data-close-cart], [data-cart-scrim]');
basket.click('[data-open-cart]');
await tick();
assert.equal(basket.requests.filter(item => item.event_type === 'basket_opened').length, 2);
console.log('PASS: basket opens emit intent; recommendation clicks preserve event ordering and navigation');
