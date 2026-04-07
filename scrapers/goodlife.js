window.allProducts = [];
window.lastSavedIndex = 0; // 👈 track last saved batch

async function autoScrapeLive() {
    function sleep(ms) {
        return new Promise(r => setTimeout(r, ms));
    }

    function cleanPrice(text) {
        if (!text) return null;
        return parseFloat(text.replace(/[^0-9.]/g, ""));
    }

    function downloadBatch(data, batchNumber) {
        const blob = new Blob([JSON.stringify(data, null, 2)], {
            type: "application/json"
        });

        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `products_batch_${batchNumber}.json`;
        a.click();
    }

    function maybeSaveBatch() {
        const newItems = window.allProducts.length - window.lastSavedIndex;

        if (newItems >= 100) {
            const batch = window.allProducts.slice(
                window.lastSavedIndex,
                window.lastSavedIndex + 100
            );

            const batchNumber = Math.floor(window.lastSavedIndex / 100) + 1;

            console.log(`💾 Saving batch ${batchNumber}...`);

            downloadBatch(batch, batchNumber);

            window.lastSavedIndex += 100;
        }
    }

    function extractAndAppend() {
        const cards = document.querySelectorAll("ul.products li.product");

        cards.forEach(card => {
            try {
                const name = card.querySelector(".woocommerce-loop-product__title")?.innerText.trim();
                const link = card.querySelector("a")?.href;

                if (!name || window.allProducts.find(p => p.url === link)) return;

                const img =
                    card.querySelector("img.attachment-woocommerce_thumbnail")?.src ||
                    card.querySelector("img")?.src;

                const cartBtn = card.querySelector("a.add_to_cart_button");

                const product_id = cartBtn?.getAttribute("data-product_id");
                const product_sku = cartBtn?.getAttribute("data-product_sku");

                const badge = card.querySelector(".onsale")?.innerText.trim();

                let current_price = null;
                let original_price = null;

                const priceContainer = card.querySelector(".price");

                if (priceContainer) {
                    const ins = priceContainer.querySelector("ins .woocommerce-Price-amount bdi");
                    const del = priceContainer.querySelector("del .woocommerce-Price-amount bdi");

                    if (ins) {
                        current_price = cleanPrice(ins.innerText);
                        original_price = del ? cleanPrice(del.innerText) : null;
                    } else {
                        const single = priceContainer.querySelector(".woocommerce-Price-amount bdi");
                        current_price = single
                            ? cleanPrice(single.innerText)
                            : cleanPrice(priceContainer.innerText);
                    }
                }

                window.allProducts.push({
                    scrape_date: new Date().toISOString(),
                    product_id,
                    product_sku,
                    product_name: name,
                    current_price,
                    original_price,
                    discount_badge: badge,
                    url: link,
                    image_url: img
                });

            } catch (e) {}
        });

        console.log("📦 Current total:", window.allProducts.length);

        // 👇 Save every 100
        maybeSaveBatch();
    }

    while (true) {
        extractAndAppend();

        const btn = [...document.querySelectorAll("a")]
            .find(el => el.innerText.trim().toLowerCase() === "load more");

        if (!btn) {
            console.log("✅ Finished loading all products");

            
            break;
        }

        console.log("👉 Clicking Load More...");
        btn.click();

        await sleep(2500);
    }

    console.log("🎉 FINAL COUNT:", window.allProducts.length);
}

autoScrapeLive();