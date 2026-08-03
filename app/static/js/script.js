document.addEventListener("DOMContentLoaded", () => {
    let allProducts = [];
    let cart = {}; 

    const gridContainer = document.getElementById("product-grid");
    const searchBar = document.getElementById("search-bar");
    const cartItemsContainer = document.getElementById("cart-items");
    const cartCountElement = document.getElementById("cart-count");

    // Fetch products
    fetch('/api/products')
        .then(res => res.json())
        .then(data => {
            allProducts = data.products;
            document.getElementById("loading-message").style.display = 'none';
            renderProducts(allProducts);
        });

    function getEmoji(name) {
        if(name.toLowerCase().includes("organic")) return "🌱";
        if(name.toLowerCase().includes("water")) return "💧";
        if(name.toLowerCase().includes("cheese")) return "🧀";
        if(name.toLowerCase().includes("milk")) return "🥛";
        return "📦";
    }

    // --- RENDER MAIN GRID ---
    function renderProducts(productsToRender) {
        gridContainer.innerHTML = ''; 
        productsToRender.forEach(productName => {
            const card = document.createElement("div");
            card.className = "product-card";
            card.dataset.product = productName; 
            
            card.innerHTML = `
                <div>
                    <div class="product-icon">${getEmoji(productName)}</div>
                    <div class="product-name">${productName}</div>
                </div>
                <div class="action-area" data-action-item="${productName}"></div>
            `;
            gridContainer.appendChild(card);
            updateActionUI(productName); 
        });
    }

    searchBar.addEventListener('input', (e) => {
        const term = e.target.value.toLowerCase();
        renderProducts(allProducts.filter(p => p.toLowerCase().includes(term)));
        // Close any open sub-rows when searching
        document.querySelectorAll('.sub-row').forEach(el => el.remove());
    });

    // --- DYNAMIC BUTTON UI (+ / -) ---
    function updateActionUI(productName) {
        const qty = cart[productName] || 0;
        // Find ALL places this product exists on screen (main grid AND sub-rows)
        const areas = document.querySelectorAll(`[data-action-item="${productName}"]`);
        
        areas.forEach(area => {
            // Check if this button is inside a sub-row
            const isSubRow = area.closest('.sub-row') !== null;
            
            if (qty === 0) {
                // If it's a main grid button, it can trigger a sub-row (!isSubRow = true)
                area.innerHTML = `<button class="add-to-cart-btn" onclick="modifyCart(event, '${productName}', 1, ${!isSubRow})">Add to Cart</button>`;
            } else {
                // + and - buttons should NEVER trigger a new sub-row, just update quantities (false)
                area.innerHTML = `
                    <div class="qty-control">
                        <button class="qty-btn" onclick="modifyCart(event, '${productName}', -1, false)">-</button>
                        <span class="qty-count">${qty}</span>
                        <button class="qty-btn" onclick="modifyCart(event, '${productName}', 1, false)">+</button>
                    </div>
                `;
            }
        });
    }

    // --- CART STATE MANAGEMENT ---
    window.modifyCart = async function(event, productName, change, canTriggerSubRow) {
        if(event) event.stopPropagation(); // Stop the page from scrolling/jumping

        cart[productName] = (cart[productName] || 0) + change;
        if (cart[productName] <= 0) delete cart[productName];

        updateActionUI(productName);
        updateCartSidebar();

        // Only trigger the expanding row if it was an initial "Add to Cart" from the main grid
        if (change > 0 && canTriggerSubRow) {
            await triggerSubRow(productName);
        }
    };

    function updateCartSidebar() {
        cartItemsContainer.innerHTML = '';
        let count = 0;
        
        if (Object.keys(cart).length === 0) {
            cartItemsContainer.innerHTML = '<li class="empty-cart">Cart is empty</li>';
        } else {
            for (const [item, qty] of Object.entries(cart)) {
                count += qty;
                cartItemsContainer.innerHTML += `<li><span>${item}</span><strong>x${qty}</strong></li>`;
            }
        }
        cartCountElement.innerText = count;
        fetchCartRecommendations();
    }

    // --- THE SUB-ROW DROPDOWN (NETFLIX STYLE) ---
    async function triggerSubRow(productName) {
        document.querySelectorAll('.sub-row').forEach(el => el.remove());

        const response = await fetch(`/api/recommend?item=${encodeURIComponent(productName)}`);
        const data = await response.json();

        if (data.recommendations && data.recommendations.length > 0) {
            const subRow = document.createElement("div");
            subRow.className = "sub-row";
            
            let miniCardsHTML = data.recommendations.map(item => `
                <div class="mini-card">
                    <div style="font-size: 2rem;">${getEmoji(item)}</div>
                    <div style="font-size: 0.85rem; text-align: center; font-weight: bold;">${item}</div>
                    <div style="width: 100%;" data-action-item="${item}"></div>
                </div>
            `).join('');

            subRow.innerHTML = `
                <h4 style="margin-bottom: 0.5rem;">Because you bought ${productName}...</h4>
                <div class="sub-row-items">${miniCardsHTML}</div>
            `;

            const activeCard = Array.from(document.querySelectorAll('.product-card'))
                                    .find(card => card.dataset.product === productName);
            
            if (activeCard) {
                // NETFLIX MATH: Find the last card in the current visual row
                let lastCardInRow = activeCard;
                let nextCard = activeCard.nextElementSibling;
                
                while (nextCard && nextCard.classList.contains('product-card')) {
                    // If the next card's Y-position is lower, it means it wrapped to a new line
                    if (nextCard.offsetTop > activeCard.offsetTop) {
                        break; 
                    }
                    lastCardInRow = nextCard;
                    nextCard = nextCard.nextElementSibling;
                }

                // Insert the sub-row immediately after the last card in that row
                lastCardInRow.insertAdjacentElement('afterend', subRow);
                
                // Initialize the buttons inside the new mini-cards
                data.recommendations.forEach(item => updateActionUI(item));
            }
        }
    }

    // --- GLOBAL CART RECOMMENDATIONS ---
    async function fetchCartRecommendations() {
        let globalRecContainer = document.getElementById("global-recs");
        if (!globalRecContainer) {
            globalRecContainer = document.createElement("div");
            globalRecContainer.id = "global-recs";
            globalRecContainer.className = "cart-recommendations";
            document.querySelector(".cart-container").appendChild(globalRecContainer);
        }

        const itemsInCart = Object.keys(cart);
        if (itemsInCart.length === 0) {
            globalRecContainer.innerHTML = '';
            return;
        }

        const response = await fetch('/api/cart-recommend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ cart: itemsInCart })
        });
        const data = await response.json();

        if (data.recommendations && data.recommendations.length > 0) {
            let recHTML = data.recommendations.map(item => `
                <div class="cart-rec-item">
                    <span>${getEmoji(item)} ${item}</span>
                    <button class="add-rec-btn" onclick="modifyCart(event, '${item}', 1, false)">+ Add</button>
                </div>
            `).join('');

            globalRecContainer.innerHTML = `
                <h3>Suggested for your cart:</h3>
                ${recHTML}
            `;
        } else {
            globalRecContainer.innerHTML = '';
        }
    }
});