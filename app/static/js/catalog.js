document.addEventListener('DOMContentLoaded', () => {
    'use strict';
    const Market = window.BasketSignal;
    const grid = document.querySelector('[data-product-grid]');
    const count = document.querySelector('[data-catalog-count]');
    const empty = document.querySelector('[data-catalog-empty]');
    const pagination = document.querySelector('[data-pagination]');
    const search = document.querySelector('#catalog-search');
    const headerSearch = document.querySelector('#header-search');
    const department = document.querySelector('[data-department-filter]');
    const aisle = document.querySelector('[data-aisle-filter]');
    const params = new URLSearchParams(location.search);
    const state = {q: params.get('q') || '', departmentId: params.get('department_id') || '', aisleId: params.get('aisle_id') || '', page: Number(params.get('page') || 1)};
    let searchTimer;

    search.value = state.q; headerSearch.value = state.q;
    document.querySelector('.header-search')?.addEventListener('submit', event => { event.preventDefault(); state.q = headerSearch.value.trim(); search.value = state.q; state.page = 1; loadProducts(true); document.querySelector('#market-catalog').scrollIntoView(); });

    async function loadFilters() {
        const filterParams = new URLSearchParams(); if (state.departmentId) filterParams.set('department_id', state.departmentId);
        const response = await fetch(`/api/catalog/filters?${filterParams}`); const data = await response.json();
        department.innerHTML = '<option value="">All departments</option>' + data.departments.map(item => `<option value="${item.department_id}">${Market.escapeHtml(Market.titleCase(item.department))} (${item.product_count.toLocaleString()})</option>`).join('');
        aisle.innerHTML = '<option value="">All aisles</option>' + data.aisles.map(item => `<option value="${item.aisle_id}">${Market.escapeHtml(Market.titleCase(item.aisle))} (${item.product_count.toLocaleString()})</option>`).join('');
        department.value = state.departmentId; aisle.value = state.aisleId;
    }

    function updateUrl() {
        const next = new URLSearchParams(); if (state.q) next.set('q', state.q); if (state.departmentId) next.set('department_id', state.departmentId); if (state.aisleId) next.set('aisle_id', state.aisleId); if (state.page > 1) next.set('page', state.page);
        history.replaceState({}, '', `${location.pathname}${next.size ? `?${next}` : ''}`);
    }

    function renderPagination(meta) {
        if (meta.pages <= 1) { pagination.innerHTML = ''; return; }
        const start = Math.max(1, Math.min(meta.page - 2, meta.pages - 4)); const end = Math.min(meta.pages, start + 4);
        let html = `<button type="button" data-page="${meta.page - 1}" ${meta.has_previous ? '' : 'disabled'} aria-label="Previous catalog page">${Market.icon('chevron', 'icon-flip')}</button>`;
        for (let page = start; page <= end; page += 1) html += `<button type="button" data-page="${page}" class="${page === meta.page ? 'is-current' : ''}" ${page === meta.page ? 'aria-current="page"' : ''}>${page}</button>`;
        html += `<span>of ${meta.pages.toLocaleString()}</span><button type="button" data-page="${meta.page + 1}" ${meta.has_next ? '' : 'disabled'} aria-label="Next catalog page">${Market.icon('chevron')}</button>`;
        pagination.innerHTML = html;
    }

    async function loadProducts(trackSearch = false) {
        grid.setAttribute('aria-busy', 'true');
        const query = new URLSearchParams({page: state.page, per_page: 24}); if (state.q) query.set('q', state.q); if (state.departmentId) query.set('department_id', state.departmentId); if (state.aisleId) query.set('aisle_id', state.aisleId);
        updateUrl();
        try {
            const response = await fetch(`/api/products?${query}`); if (!response.ok) throw new Error(); const data = await response.json();
            grid.innerHTML = data.products.map(product => Market.productCard(product)).join(''); grid.hidden = !data.products.length; empty.hidden = Boolean(data.products.length);
            count.textContent = data.total ? `${data.total.toLocaleString()} product${data.total === 1 ? '' : 's'} · page ${data.pagination.page} of ${data.pagination.pages.toLocaleString()}` : 'No products found';
            renderPagination(data.pagination);
            if (trackSearch && state.q) Market.publish('search_performed', {query: state.q, result_count: data.total}).catch(() => {});
        } catch (_) { grid.innerHTML = '<div class="load-error"><strong>The market shelves could not be loaded.</strong><p>Refresh the page or check the Flask server.</p></div>'; count.textContent = 'Catalog unavailable'; }
        finally { grid.setAttribute('aria-busy', 'false'); }
    }

    search.addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.q = search.value.trim(); headerSearch.value = state.q; state.page = 1; loadProducts(true); }, 350); });
    department.addEventListener('change', async () => { state.departmentId = department.value; state.aisleId = ''; state.page = 1; await loadFilters(); loadProducts(); });
    aisle.addEventListener('change', () => { state.aisleId = aisle.value; state.page = 1; loadProducts(); });
    pagination.addEventListener('click', event => { const button = event.target.closest('[data-page]'); if (!button || button.disabled) return; state.page = Number(button.dataset.page); loadProducts(); document.querySelector('#market-catalog').scrollIntoView(); });
    document.querySelector('[data-clear-filters]').addEventListener('click', async () => { state.q = ''; state.departmentId = ''; state.aisleId = ''; state.page = 1; search.value = ''; headerSearch.value = ''; await loadFilters(); loadProducts(); });
    Promise.all([loadFilters(), loadProducts()]);
});
