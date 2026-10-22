import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const currentProduct = {
    product_id: 16,
    product_name: 'Mint Chocolate Flavored Syrup',
    price_inr: 1197,
    aisle: 'ice cream toppings',
    aisle_id: 103,
    department: 'snacks',
    department_id: 19,
    quantity: 1,
};
const recommendation = {
    product_id: 3549,
    product_name: 'Magic Shell Caramel Flavored Topping',
    price_inr: 536,
    aisle: 'ice cream toppings',
    department: 'snacks',
    recommendation_reason: 'Often paired with Mint Chocolate Flavored Syrup',
    recommendation_source: 'historical_rule',
    supporting_product_id: 16,
    supporting_product_name: 'Mint Chocolate Flavored Syrup',
    lift: 2.31,
    impression_id: 'impression-test',
    policy_version: 'linucb-v1',
    snapshot_version: 'snapshot-test',
};

let onReady;
const requestedUrls = [];
const element = () => ({
    hidden: false,
    innerHTML: '',
    textContent: '',
    addEventListener() {},
    querySelector() { return {addEventListener() {}}; },
});
const elements = {
    '[data-product-stage]': element(),
    '[data-related-grid]': element(),
    '[data-featured-pairing]': element(),
    '[data-featured-section]': element(),
    '[data-product-breadcrumb]': element(),
};

globalThis.document = {
    body: {dataset: {productId: '16'}},
    title: '',
    referrer: '',
    addEventListener(type, callback) {
        if (type === 'DOMContentLoaded') onReady = callback;
    },
    querySelector(selector) { return elements[selector] || element(); },
};
globalThis.window = {BasketSignal: {
    identity: {userId: 'shopper-test', sessionId: 'session-test'},
    getCart: () => [currentProduct],
    productArtwork: product => `<span data-art-product="${product.product_id}"></span>`,
    escapeHtml: String,
    titleCase: String,
    money: value => `INR ${value}`,
    icon: () => '',
    addProduct() {},
    productCard: () => '',
    publish: async () => {},
}};
globalThis.fetch = async (url, options = {}) => {
    requestedUrls.push(String(url));
    if (String(url).startsWith('/api/products/16')) {
        return {ok: true, json: async () => ({product: currentProduct, related_products: []})};
    }
    if (url === '/api/cart-recommend') {
        assert.deepEqual(JSON.parse(options.body).cart, [16]);
        return {ok: true, json: async () => ({recommendations: [recommendation]})};
    }
    throw new Error(`Unexpected request: ${url}`);
};
globalThis.addEventListener = () => {};
Object.defineProperty(globalThis, 'navigator', {value: {}, configurable: true});

vm.runInThisContext(fs.readFileSync('app/static/js/product.js', 'utf8'), {filename: 'product.js'});
await onReady();

const featured = elements['[data-featured-pairing]'];
assert.equal(elements['[data-featured-section]'].hidden, false);
assert.match(featured.innerHTML, /Magic Shell Caramel Flavored Topping/);
assert.match(featured.innerHTML, /Mint Chocolate Flavored Syrup/);
assert.doesNotMatch(featured.innerHTML, /You’re considering/);
assert.doesNotMatch(featured.innerHTML, /Add the pairing/);
assert.ok(requestedUrls.includes('/api/cart-recommend'));
console.log('PASS: product page renders a distinct basket-supported recommendation');
