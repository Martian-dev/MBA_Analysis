import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const productPageSource = fs.readFileSync('app/static/js/product.js', 'utf8');
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
    policy_mode: 'adaptive_linucb',
    policy_version: 'linucb-v1',
    snapshot_version: 'snapshot-test',
};

const element = () => ({
    hidden: false,
    innerHTML: '',
    textContent: '',
    addEventListener() {},
    querySelector() { return {addEventListener() {}}; },
});
const nextTask = () => new Promise(resolve => setTimeout(resolve, 0));

async function renderProductPage({cart: initialCart, recommendations = [], recommendationLoader = null}) {
    let cart = initialCart;
    let onReady;
    let basketListener = () => {};
    let recommendationCall = 0;
    const requestedCarts = [];
    const elements = {
        '[data-product-stage]': element(),
        '[data-related-grid]': element(),
        '[data-featured-pairing]': element(),
        '[data-featured-section]': element(),
        '[data-product-breadcrumb]': element(),
    };
    const recommendationResponses = Array.isArray(recommendations[0]) ? recommendations : [recommendations];
    const market = {
        identity: {userId: 'shopper-test', sessionId: 'session-test'},
        getCart: () => cart,
        getCartRecommendations: async requestedCart => {
            requestedCarts.push(requestedCart.map(item => item.product_id));
            if (recommendationLoader) {
                return recommendationLoader({
                    callIndex: recommendationCall++,
                    replaceCart(nextCart) { cart = nextCart; basketListener(); },
                    requestedCart,
                });
            }
            const items = recommendationResponses[Math.min(recommendationCall, recommendationResponses.length - 1)];
            recommendationCall += 1;
            return {recommendations: items};
        },
        subscribe(listener) { basketListener = listener; return () => { basketListener = () => {}; }; },
        productArtwork: product => `<span data-art-product="${product.product_id}"></span>`,
        escapeHtml: String,
        titleCase: String,
        money: value => `INR ${value}`,
        icon: () => '',
        addProduct() {},
        productCard: () => '',
        publish: async () => {},
        publishRecommendationImpression: async () => {},
    };
    const document = {
        body: {dataset: {productId: '16'}},
        title: '',
        referrer: '',
        addEventListener(type, callback) { if (type === 'DOMContentLoaded') onReady = callback; },
        querySelector(selector) { return elements[selector] || element(); },
    };
    const context = vm.createContext({
        console,
        document,
        window: {BasketSignal: market},
        fetch: async url => {
            if (String(url).startsWith('/api/products/16')) {
                return {ok: true, json: async () => ({product: currentProduct, related_products: []})};
            }
            throw new Error(`Unexpected direct request: ${url}`);
        },
        addEventListener() {},
        navigator: {},
        performance,
        URLSearchParams,
    });

    vm.runInContext(productPageSource, context, {filename: 'product.js'});
    await onReady();

    return {
        elements,
        requestedCarts,
        async replaceCart(nextCart) {
            cart = nextCart;
            basketListener();
            await nextTask();
        },
    };
}

const adaptive = await renderProductPage({cart: [currentProduct], recommendations: [recommendation]});
const card = adaptive.elements['[data-featured-pairing]'];
assert.equal(adaptive.elements['[data-featured-section]'].hidden, false);
assert.match(card.innerHTML, /Magic Shell Caramel Flavored Topping/);
assert.match(card.innerHTML, /Because <strong>Mint Chocolate Flavored Syrup<\/strong> is in your basket/);
assert.match(card.innerHTML, /Historical association rule/);
assert.match(card.innerHTML, /Live ranked/);
assert.doesNotMatch(card.innerHTML, /You’re considering|Add the pairing/);
assert.deepEqual(adaptive.requestedCarts, [[16]]);

const noValidResult = await renderProductPage({cart: [currentProduct], recommendations: [currentProduct]});
assert.equal(noValidResult.elements['[data-featured-section]'].hidden, true);

const historical = await renderProductPage({
    cart: [currentProduct],
    recommendations: [{...recommendation, policy_mode: 'historical_fallback'}],
});
assert.match(historical.elements['[data-featured-pairing]'].innerHTML, /Historical order/);
assert.doesNotMatch(historical.elements['[data-featured-pairing]'].innerHTML, /Live ranked/);

await adaptive.replaceCart([currentProduct, recommendation]);
assert.equal(adaptive.elements['[data-featured-section]'].hidden, true);
assert.deepEqual(adaptive.requestedCarts, [[16], [16, 3549]]);

const failedRequest = await renderProductPage({
    cart: [currentProduct],
    recommendationLoader: async () => { throw new Error('offline'); },
});
assert.equal(failedRequest.elements['[data-featured-section]'].hidden, true);

let resolveFirstRequest;
const staleResponse = await renderProductPage({
    cart: [currentProduct],
    recommendationLoader: ({callIndex, replaceCart}) => {
        if (callIndex === 0) {
            const firstRequest = new Promise(resolve => { resolveFirstRequest = resolve; });
            queueMicrotask(() => {
                replaceCart([currentProduct, recommendation]);
                setTimeout(() => resolveFirstRequest({recommendations: [recommendation]}), 0);
            });
            return firstRequest;
        }
        return {recommendations: []};
    },
});
assert.equal(staleResponse.elements['[data-featured-section]'].hidden, true);
assert.deepEqual(staleResponse.requestedCarts, [[16], [16, 3549]]);

console.log('PASS: product page keeps basket-supported recommendations distinct and current');
