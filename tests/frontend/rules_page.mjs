import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const handlers = {};
const nodes = new Map();
const element = () => ({
    innerHTML: '', textContent: '', disabled: false, elements: {}, focusedChild: null,
    contains(node) { return node !== null && node === this.focusedChild; },
    setAttribute() {}, addEventListener(kind, listener) { this[kind] = listener; },
});
const node = selector => {
    if (!nodes.has(selector)) nodes.set(selector, element());
    return nodes.get(selector);
};
const form = node('[data-rule-filters]');
form.elements = {q: {value: ''}, eligible: {value: 'true'}, sort: {value: 'lift'}};
const document = {
    activeElement: null, visibilityState: 'visible', querySelector: node,
    addEventListener(kind, listener) { handlers[kind] = listener; },
};
let weight = 1;
let unavailable = false;
let holdRules = false;
let failRules = false;
let releaseRules;
const intervals = [];
vm.runInNewContext(fs.readFileSync('app/static/js/rules.js', 'utf8'), {
    document, window: {BasketSignal: {escapeHtml: String}}, URLSearchParams,
    location: {search: ''}, history: {replaceState() {}},
    FormData: class { *[Symbol.iterator]() { for (const [key, field] of Object.entries(form.elements)) yield [key, field.value]; } },
    setInterval(callback) { intervals.push(callback); return intervals.length; }, clearInterval() {}, setTimeout,
    addEventListener() {},
    fetch: async url => {
        if (url === '/api/rules/live') return {ok: !unavailable, json: async () => ({
            status: unavailable ? 'unavailable' : 'live', message: 'Offline', feedback_updates: 1,
            as_of: '2026-10-23T12:00:00Z', products: [{product_id: 1, product_name: 'Banana',
                impressions: 1, positive_weight: weight, legacy_positive_weight: 0}],
        })};
        if (holdRules) await new Promise(resolve => { releaseRules = resolve; });
        if (failRules) throw new Error('rules unavailable');
        return {ok: true, json: async () => ({pagination: {page: 1, pages: 1}, total: 0,
            eligible_total: 0, historical_total: 0, rules: [], snapshot_version: 'fixture',
            policy: {dwell_threshold_seconds: 45, min_lift: 1.5, min_confidence: .1, min_support: .012, discount_percent: 5}})};
    },
}, {filename: 'rules.js'});
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
await handlers.DOMContentLoaded();
await tick();
const results = node('[data-live-results]');
const original = results.innerHTML;
results.focusedChild = {};
document.activeElement = results.focusedChild;
weight = 2;
handlers.visibilitychange();
await tick();
assert.equal(results.innerHTML, original, 'An automatic counter update must preserve the focused link');
assert.match(node('[data-live-summary]').textContent, /Display refresh paused/);
unavailable = true;
handlers.visibilitychange();
await tick();
assert.equal(results.innerHTML, original, 'A live outage must also preserve the focused link');
unavailable = false;
document.activeElement = null;
results.focusout();
await tick(); await tick();
assert.notEqual(results.innerHTML, original);
assert.match(results.innerHTML, /2\.00/);
const ruleResults = node('[data-rule-results]');
const previousRows = ruleResults.innerHTML;
document.activeElement = null;
holdRules = true;
intervals[1]();
await tick();
ruleResults.focusedChild = {};
document.activeElement = ruleResults.focusedChild;
failRules = true;
releaseRules();
await tick();
assert.equal(ruleResults.innerHTML, previousRows, 'An in-flight automatic rule failure must preserve the focused link');
assert.match(node('[data-rule-summary]').textContent, /showing the last loaded rules/);
console.log('PASS: live rules signals preserve keyboard focus during updates and outages');
