const blob = new Blob(
  [JSON.stringify(window.allProducts, null, 2)],
  { type: "application/json" }
);

const a = document.createElement("a");
a.href = URL.createObjectURL(blob);
a.download = "all_products.json";
a.click();