const list = document.querySelector('#event-list');
const connection = document.querySelector('#connection');
const counts = {event_received: 0, coupon_issued: 0, coupon_suppressed: 0};
let source;

const text = value => String(value ?? '—');
const time = value => value ? new Date(value).toISOString().slice(11, 19) : '—';
const actionLabel = value => ({cart_item_added:'Cart add',cart_item_removed:'Cart remove',product_view_started:'View started',product_view_ended:'View ended'}[value] || value);
const reasonLabel = value => ({cart_updated:'Cart updated',view_started:'Watching',below_dwell_threshold:'Short view',product_in_cart:'Already in cart',coupon_already_issued:'Already offered',below_confidence_threshold:'Confidence too low',below_support_threshold:'Support too low',no_qualifying_rule:'No rule match',coupon_issued:'Coupon issued'}[value] || value || 'Observed');

function setConnection(state, label) {
  connection.className = `connection ${state}`;
  connection.innerHTML = `<i aria-hidden="true"></i> ${label}`;
}

function updateMetrics() {
  document.querySelector('#event-count').textContent = counts.event_received || 0;
  document.querySelector('#coupon-count').textContent = counts.coupon_issued || 0;
  document.querySelector('#suppressed-count').textContent = counts.coupon_suppressed || 0;
}

function rowFor(entry, isNew=false) {
  const payload = entry.payload;
  const li = document.createElement('li');
  li.className = `event-row${isNew ? ' new' : ''}`;
  const outcomeClass = entry.kind === 'coupon_issued' ? 'issued' : entry.kind === 'coupon_suppressed' ? 'suppressed' : '';
  li.innerHTML = `<span class="time"></span><span class="who"><strong></strong><span></span></span><span class="product"></span><span class="outcome ${outcomeClass}"></span>`;
  li.querySelector('.time').textContent = time(entry.observed_at);
  li.querySelector('.who strong').textContent = text(payload.user_id);
  li.querySelector('.who span').textContent = actionLabel(payload.event_type);
  li.querySelector('.product').textContent = text(payload.product_id);
  li.querySelector('.outcome').textContent = entry.kind === 'coupon_issued'
    ? `${text(payload.discount_percent)}% off · Issued`
    : reasonLabel(payload.reason);
  return li;
}

function showDecision(entry) {
  if (entry.kind !== 'coupon_issued') return;
  const p = entry.payload;
  const target = document.querySelector('#latest-decision');
  target.innerHTML = '';
  const offer = document.createElement('div');
  offer.className = 'offer';
  offer.innerHTML = `<div class="offer-kicker">COUPON ISSUED · MOCK OFFER</div><h3 class="offer-product"></h3><div class="offer-value"><span></span><small> off</small></div><p class="offer-summary">This offer is ready to be applied to the viewed product.</p><dl class="evidence"><div><dt>Cart evidence</dt><dd class="support"></dd></div><div><dt>Observed dwell</dt><dd class="dwell"></dd></div><div><dt>Rule lift</dt><dd class="lift"></dd></div><div><dt>Confidence</dt><dd class="confidence"></dd></div></dl><span class="offer-id"></span>`;
  offer.querySelector('.offer-product').textContent = p.product_id;
  offer.querySelector('.offer-value span').textContent = `${p.discount_percent}%`;
  offer.querySelector('.support').textContent = p.supporting_cart_item;
  offer.querySelector('.dwell').textContent = `${p.reason.dwell_seconds}s`;
  offer.querySelector('.lift').textContent = Number(p.lift).toFixed(2);
  offer.querySelector('.confidence').textContent = `${(Number(p.confidence) * 100).toFixed(1)}%`;
  offer.querySelector('.offer-id').textContent = `Coupon ID · ${p.coupon_id}`;
  target.append(offer);
}

function appendEntry(entry, isNew=false) {
  if (list.querySelector('.empty-state')) list.innerHTML = '';
  list.prepend(rowFor(entry, isNew));
  while (list.children.length > 60) list.lastElementChild.remove();
  document.querySelector('#last-seen').textContent = time(entry.observed_at);
  showDecision(entry);
}

async function start() {
  try {
    const response = await fetch('/api/activity');
    if (!response.ok) throw new Error('The live stack is not running');
    const data = await response.json();
    Object.assign(counts, data.counts);
    updateMetrics();
    data.events.forEach(entry => appendEntry(entry));
    connect(data.last_id);
  } catch (error) {
    setConnection('down', 'Pipeline offline');
    list.innerHTML = `<li class="empty-state">${error.message}. Run <code>bash scripts/run_live.sh</code>, then reload this page.</li>`;
    setTimeout(start, 4000);
  }
}

function connect(cursor) {
  if (source) source.close();
  source = new EventSource(`/api/activity/stream?since=${encodeURIComponent(cursor)}`);
  source.onopen = () => setConnection('live', 'Live stream');
  source.onmessage = event => {
    const entry = JSON.parse(event.data);
    counts[entry.kind] = (counts[entry.kind] || 0) + 1;
    updateMetrics();
    appendEntry(entry, true);
  };
  source.onerror = () => setConnection('down', 'Reconnecting');
}

start();
