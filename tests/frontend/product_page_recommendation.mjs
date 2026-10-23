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

const element = () => {
    const children = new Map();
    return {
        hidden: false, html: '', htmlWrites: 0, textContent: '',
        get innerHTML() { return this.html; },
        set innerHTML(value) { this.html = value; this.htmlWrites += 1; children.clear(); },
        addEventListener() {},
        querySelector(selector) {
            if (!children.has(selector)) children.set(selector, element());
            return children.get(selector);
        },
    };
};
const offerText = page => page.elements['[data-next-pick]'].querySelector('[data-next-offer]').textContent;
const nextTask = () => new Promise(resolve => setTimeout(resolve, 0));

async function renderProductPage({cart: initialCart, recommendations = [], recommendationLoader = null, relatedProducts = [], offers = [], offersOffline = false, withObserver = false}) {
    let cart = initialCart;
    let onReady;
    let basketListener = () => {};
    let offerListener = () => {};
    let recommendationCall = 0;
    const requestedCarts = [];
    const published = [];
    let now = 0;
    let timer;
    const documentHandlers = {};
    const pageHandlers = {};
    const observers = [];
    const window = {BasketSignal: null};
    if (withObserver) window.IntersectionObserver = class {
        constructor(callback) { this.callback = callback; this.disconnected = false; observers.push(this); }
        observe() {}
        disconnect() { this.disconnected = true; }
    };
    const elements = {
        '[data-product-stage]': element(),
        '[data-related-grid]': element(),
        '[data-related-section]': element(),
        '[data-next-section]': element(),
        '[data-next-pick]': element(),
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
        subscribeOffers(listener) { offerListener = listener; return () => { offerListener = () => {}; }; },
        async getAvailableOffers() { if (offersOffline) throw new Error('offline'); return offers; },
        productArtwork: product => `<span data-art-product="${product.product_id}"></span>`,
        escapeHtml: String,
        titleCase: String,
        money: value => `INR ${value}`,
        icon: () => '',
        addProduct() {},
        productCard: product => `<article>${product.product_name}</article>`,
        publish: async (kind, payload) => { published.push({kind, payload}); },
        publishRecommendationImpression: async (product, placement) => { published.push({kind: 'recommendation_impression', payload: {...product, placement}}); },
    };
    const document = {
        body: {dataset: {productId: '16'}},
        title: '',
        referrer: '',
        visibilityState: 'visible',
        addEventListener(type, callback) { if (type === 'DOMContentLoaded') onReady = callback; else documentHandlers[type] = callback; },
        querySelector(selector) { return elements[selector] || element(); },
    };
    window.BasketSignal = market;
    const context = vm.createContext({
        console,
        document,
        window,
        fetch: async url => {
            if (String(url).startsWith('/api/products/16')) {
                return {ok: true, json: async () => ({product: currentProduct, related_products: relatedProducts})};
            }
            throw new Error(`Unexpected direct request: ${url}`);
        },
        addEventListener(type, callback) { pageHandlers[type] = callback; },
        navigator: {},
        performance: {now: () => now},
        URLSearchParams,
        setTimeout(callback, delay) { timer = {callback, due: now + delay}; return 1; },
        clearTimeout() { timer = null; },
    });

    vm.runInContext(productPageSource, context, {filename: 'product.js'});
    await onReady();

    return {
        elements,
        requestedCarts,
        published,
        observers,
        intersect(index, ratio = 1) { observers[index].callback([{isIntersecting: ratio > 0, intersectionRatio: ratio}]); },
        closePage() { pageHandlers.pagehide(); },
        async replaceOffers(nextOffers) { offers = nextOffers; offerListener(); await nextTask(); },
        async advanceTime(milliseconds) {
            now += milliseconds;
            if (timer && timer.due <= now) { const callback = timer.callback; timer = null; callback(); }
            await nextTask();
        },
        async setVisibility(value) { document.visibilityState = value; documentHandlers.visibilitychange(); await nextTask(); },
        async replaceCart(nextCart) {
            cart = nextCart;
            basketListener();
            await nextTask();
        },
    };
}

const adaptive = await renderProductPage({cart: [currentProduct], recommendations: [recommendation]});
await adaptive.advanceTime(44_000);
assert.equal(adaptive.published.filter(event => event.kind === 'product_view_ended').length, 0);
await adaptive.advanceTime(1_250);
assert.equal(adaptive.published.filter(event => event.kind === 'product_view_ended').length, 1);
assert.equal(adaptive.published.find(event => event.kind === 'product_view_ended').payload.dwell_seconds, 45.25);
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

const alternative = {...recommendation, product_id: 100, product_name: 'Alternative topping'};
const nextPick = await renderProductPage({cart: [], relatedProducts: [currentProduct, recommendation, alternative]});
assert.equal(nextPick.elements['[data-next-section]'].hidden, false);
assert.match(nextPick.elements['[data-next-pick]'].innerHTML, /Magic Shell Caramel Flavored Topping/);
assert.match(nextPick.elements['[data-next-pick]'].innerHTML, /Often paired with Mint Chocolate Flavored Syrup/);
assert.match(offerText(nextPick), /Explore for potential offers/);
assert.doesNotMatch(offerText(nextPick), /Offers available/);
assert.doesNotMatch(nextPick.elements['[data-related-grid]'].innerHTML, /Magic Shell/);
assert.match(nextPick.elements['[data-related-grid]'].innerHTML, /Alternative topping/);
await nextPick.replaceOffers([{product_id: alternative.product_name, discount_percent: 5}]);
assert.doesNotMatch(offerText(nextPick), /Offers available/);
const cardWrites = nextPick.elements['[data-next-pick]'].htmlWrites;
const gridWrites = nextPick.elements['[data-related-grid]'].htmlWrites;
await nextPick.replaceOffers([{product_id: recommendation.product_name, discount_percent: 5}]);
assert.match(offerText(nextPick), /\* Offers available/);
assert.equal(nextPick.elements['[data-next-pick]'].htmlWrites, cardWrites, 'Offer changes must preserve focused product links');
assert.equal(nextPick.elements['[data-related-grid]'].htmlWrites, gridWrites, 'Offer changes must preserve related product controls');
await nextPick.replaceOffers([]);
assert.doesNotMatch(offerText(nextPick), /Offers available/);
assert.equal(nextPick.published.filter(event => event.payload.placement === 'product_next_pick').length, 1);
await nextPick.replaceCart([recommendation]);
assert.match(nextPick.elements['[data-next-pick]'].innerHTML, /Alternative topping/);
await nextPick.replaceCart([recommendation, alternative]);
assert.equal(nextPick.elements['[data-next-section]'].hidden, true);
assert.equal(nextPick.elements['[data-related-section]'].hidden, true);
const offlinePick = await renderProductPage({cart: [], relatedProducts: [recommendation], offersOffline: true});
assert.equal(offlinePick.elements['[data-next-section]'].hidden, false);
assert.doesNotMatch(offerText(offlinePick), /Offers available/);
assert.equal(noValidResult.elements['[data-next-section]'].hidden, true);

const scrolled = await renderProductPage({cart: [], relatedProducts: [recommendation, alternative], withObserver: true});
const nextImpressions = () => scrolled.published.filter(event => event.payload.placement === 'product_next_pick').length;
assert.equal(nextImpressions(), 0, 'Below-fold recommendations must wait until seen');
scrolled.intersect(0, 0.1);
assert.equal(nextImpressions(), 0);
scrolled.intersect(0, 0.5);
scrolled.intersect(0, 1);
assert.equal(nextImpressions(), 1);
await scrolled.replaceCart([recommendation]);
scrolled.intersect(0, 1);
assert.equal(nextImpressions(), 1, 'A stale observer must not record the previous product');
scrolled.closePage();
scrolled.intersect(1, 1);
assert.equal(nextImpressions(), 1, 'A closed page must not record impressions');

console.log('PASS: product page shows a ranked next pick, truthful live offers, and distinct basket recommendations');
