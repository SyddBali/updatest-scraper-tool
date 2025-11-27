import asyncio
import logging
from scraper.pipeline import scrape_items

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    # Test SKUs
    # 72787: Should be missing (Strict match failed)
    # 117461: Should be found
    # 73302: Should be found (normalized from 073302)
    items = [
        {"sku": "72787", "url": None},
        {"sku": "117461", "url": None},
        {"sku": "73302", "url": None}
    ]
    
    print("Running Catalog Verification...")
    results = await scrape_items(
        items=items,
        cms_choice="Shopify",
        origin="https://legear.com.au",
        url_pattern=None,
        concurrency=1,
        delay_ms=100
    )
    
    print("\nResults:")
    for r in results:
        print(f"SKU: {r.get('sku')}")
        print(f"Name: {r.get('name')}")
        print(f"Price: {r.get('price')}")
        print(f"Error: {r.get('error')}")
        print("-" * 20)

if __name__ == "__main__":
    asyncio.run(main())
