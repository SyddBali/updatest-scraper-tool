import httpx
import asyncio
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class ShopifyCatalogIndexer:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.products_url = f"{self.base_url}/products.json"
        self.catalog: Dict[str, Dict[str, Any]] = {}
        self.indexed = False

    async def fetch_catalog(self, limit_per_page: int = 250, delay_ms: int = 100) -> int:
        """
        Fetches the entire product catalog from Shopify's products.json endpoint.
        Populates self.catalog with a mapping of SKU -> Product Data.
        Returns the number of SKUs indexed.
        """
        page = 1
        total_skus = 0
        
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    url = f"{self.products_url}?limit={limit_per_page}&page={page}"
                    logger.info(f"Fetching catalog page {page}: {url}")
                    
                    response = await client.get(url, timeout=30.0)
                    if response.status_code != 200:
                        logger.error(f"Failed to fetch catalog page {page}: {response.status_code}")
                        break
                    
                    data = response.json()
                    products = data.get("products", [])
                    
                    if not products:
                        break
                    
                    for product in products:
                        self._index_product(product)
                    
                    total_skus = len(self.catalog)
                    page += 1
                    await asyncio.sleep(delay_ms / 1000.0)
                    
                except Exception as e:
                    logger.error(f"Error fetching catalog page {page}: {e}")
                    break
        
        self.indexed = True
        return total_skus

    def _index_product(self, product: Dict[str, Any]):
        """
        Parses a single product object and adds all its variants to the index.
        """
        product_id = product.get("id")
        handle = product.get("handle")
        title = product.get("title")
        product_type = product.get("product_type")
        published_at = product.get("published_at")
        
        # Get main image
        images = product.get("images", [])
        main_image = images[0].get("src") if images else None
        
        # Process variants
        variants = product.get("variants", [])
        for variant in variants:
            sku = variant.get("sku")
            if not sku:
                continue
            
            # Normalize SKU for indexing
            # STRIP LEADING ZEROS to handle "073302" vs "73302" mismatch
            # We verified this is safe (no collisions)
            sku_str = str(sku).strip().lower()
            sku_key = sku_str.lstrip("0")
            if not sku_key: # Handle case where SKU is just "0" or "00"
                sku_key = sku_str
            
            # Construct variant-specific data
            variant_data = {
                "sku": sku,  # Keep original case/format for display
                "product_id": product_id,
                "variant_id": variant.get("id"),
                "handle": handle,
                "title": title,
                "variant_title": variant.get("title"),
                "name": f"{title} - {variant.get('title')}" if variant.get("title") != "Default Title" else title,
                "price": variant.get("price"),
                "rrp": variant.get("compare_at_price"),
                "available": variant.get("available"),
                "product_type": product_type,
                "published_at": published_at,
                "image_url": main_image, # Variant might have specific image, but main is safe fallback
                "product_url": f"{self.base_url}/products/{handle}?variant={variant.get('id')}"
            }
            
            # Store in catalog
            self.catalog[sku_key] = variant_data

    def lookup_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Looks up a SKU in the index. Returns the product data if found, else None.
        Handles leading zero normalization.
        """
        if not self.indexed:
            logger.warning("Catalog not indexed yet. Call fetch_catalog() first.")
            return None
            
        sku_str = str(sku).strip().lower()
        sku_key = sku_str.lstrip("0")
        if not sku_key:
            sku_key = sku_str
            
        return self.catalog.get(sku_key)
