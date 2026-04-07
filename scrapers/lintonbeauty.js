window.allProducts = [];

async function scrapeLintonBeauty() {
    function sleep(ms) {
        return new Promise(r => setTimeout(r, ms));
    }

    function cleanPrice(text) {
        if (!text) return null;
        return parseFloat(text.replace(/[^0-9.]/g, ""));
    }

    function extractAndAppend() {
        const cards = document.querySelectorAll('li[data-hook="product-list-grid-item"]');

        console.log(`📦 Found ${cards.length} cards`);

        cards.forEach(card => {
            try {
                const name = card.querySelector('[data-hook="product-item-name"]')?.innerText.trim();
                const link = card.querySelector('[data-hook="product-item-container"]')?.href;

                // Deduplicate
                if (!name || window.allProducts.find(p => p.url === link)) return;

                const img = card.querySelector('wow-image img')?.src;

                const priceEl = card.querySelector('[data-hook="product-item-price-to-pay"]');
                const origEl = card.querySelector('[data-hook="product-item-price-before-discount"]');

                let current_price = null;
                let original_price = null;

                if (priceEl) {
                    current_price = cleanPrice(
                        priceEl.getAttribute("data-wix-price") || priceEl.innerText
                    );
                }

                if (origEl) {
                    original_price = cleanPrice(
                        origEl.getAttribute("data-wix-original-price") || origEl.innerText
                    );
                }

                // Handle no-sale case
                if (!current_price && original_price) {
                    current_price = original_price;
                    original_price = null;
                }

                const badge = card
                    .querySelector('[data-hook="RibbonDataHook.RibbonOnImage"]')
                    ?.innerText.trim();

                let discount_pct = null;
                if (current_price && original_price) {
                    discount_pct = (
                        (original_price - current_price) / original_price * 100
                    ).toFixed(2);
                }

                if (name && current_price) {
                    window.allProducts.push({
                        scrape_date: new Date().toISOString(),
                        product_name: name,
                        current_price,
                        original_price,
                        discount_percentage: discount_pct,
                        discount_badge: badge,
                        rating: null,
                        reviews: null,
                        url: link,
                        image_url: img,
                        full_html_snippet: card.innerHTML.slice(0, 300)
                    });
                }

            } catch (e) {
                console.log("❌ Error parsing product", e);
            }
        });

        console.log("📊 Total collected:", window.allProducts.length);
    }

    async function clickLoadMore() {
        while (true) {
            extractAndAppend();

            const btn = document.querySelector('button[data-hook="load-more-button"]');

            if (!btn) {
                console.log("✅ No more Load More button");
                break;
            }

            console.log("👉 Clicking Load More...");
            btn.click();

            await sleep(3000);
        }
    }

    await clickLoadMore();

    console.log("🎉 FINAL COUNT:", window.allProducts.length);

    return window.allProducts;
}

// Run it
scrapeLintonBeauty();