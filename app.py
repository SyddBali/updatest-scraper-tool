import asyncio
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import streamlit as st
import nest_asyncio

# Fix for Streamlit's asyncio loop
nest_asyncio.apply()

from scraper.pipeline import scrape_items, scrape_by_page
from scraper.config import SITE_CONFIGS

def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def _normalise_rows(rows: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    seen = set()
    out = []
    for r in rows:
        # Case-insensitive key lookup
        sku = (r.get("SKU") or r.get("sku") or "").strip() or None
        url = (r.get("URL") or r.get("url") or "").strip() or None
        
        if not sku and not url:
            continue
            
        key = (sku or "").lower(), (url or "").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"sku": sku, "url": url})
    return out

def main():
    st.set_page_config(page_title="Universal SKU Scraper", layout="wide")

    # Custom CSS for "Premium" look
    st.markdown("""
    <style>
    .main {
        background-color: #f8f9fa;
    }
    h1 {
        color: #1f77b4;
        font-family: 'Helvetica Neue', sans-serif;
    }
    .stButton>button {
        background-color: #1f77b4;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #155a8a;
    }
    </style>
    """, unsafe_allow_html=True)

    st.title("Universal SKU Scraper")
    st.markdown("### Extract product data from Neto, Shopify, and WooCommerce")

    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        mode = st.radio("Mode", ["SKUs", "Page Crawler", "Catalog Indexer"])
        
        cms_choice = st.selectbox(
            "CMS / Site Type",
            ["Neto", "Shopify", "WordPress (WooCommerce)"],
            index=1  # Default to Shopify
        )
        
        concurrency = st.slider("Concurrency", 1, 10, 3)
        delay_ms = st.number_input("Delay (ms)", 0, 5000, 0)
        
        fast_mode = False
        if cms_choice == "Shopify":
            st.markdown("---")
            fast_mode = st.checkbox("Fast Mode (Catalog Only)", 
                                  help="Skip page visits to avoid 429 errors. No breadcrumbs, but instant results.")
        
        st.markdown("---")
        if st.button("Clear Cache", help="Force re-download of catalog data"):
            st.cache_resource.clear()
            st.success("Cache cleared!")

    if mode == "SKUs":
        col1, col2 = st.columns(2)
        with col1:
            origin = st.text_input("Base URL (Origin)", "https://legear.com.au")
        with col2:
            url_pattern = st.text_input("URL Pattern (Optional)", "", help="e.g. https://site.com/p/{sku}")

        # Input Tabs
        tab1, tab2 = st.tabs(["Manual Input", "CSV Upload"])
        
        sku_input = ""
        url_input = ""
        csv_file = None

        with tab1:
            col_a, col_b = st.columns(2)
            with col_a:
                sku_input = st.text_area("Enter SKUs (one per line)", height=150, placeholder="ABC-123\nXYZ-789")
            with col_b:
                url_input = st.text_area("Enter URLs (one per line)", height=150, placeholder="https://site.com/p/abc")
        
        with tab2:
            csv_file = st.file_uploader("Upload CSV (must have 'sku' or 'url' column)", type=["csv"])

        if st.button("Scrape Items", use_container_width=True):
            # Gather inputs
            raw_rows = []
            
            # 1. CSV
            if csv_file:
                try:
                    df_in = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
                    raw_rows.extend(df_in.to_dict(orient="records"))
                except Exception as e:
                    st.error(f"Failed reading CSV: {e}")
                    return

            # 2. Manual SKUs
            if sku_input.strip():
                raw_rows.extend([{"sku": s.strip()} for s in sku_input.splitlines() if s.strip()])

            # 3. Manual URLs
            if url_input.strip():
                raw_rows.extend([{"url": u.strip()} for u in url_input.splitlines() if u.strip()])

            items = _normalise_rows(raw_rows)

            if not items:
                st.warning("Please provide at least one SKU or URL.")
                return

            # 4. Prepare Indexer (Cached)
            indexer = None
            if cms_choice == "Shopify" and origin:
                from scraper.shopify_catalog import ShopifyCatalogIndexer
                
                @st.cache_resource(ttl=3600, show_spinner="Indexing Shopify Catalog...")
                def get_cached_indexer(url: str):
                    idx = ShopifyCatalogIndexer(url)
                    # We need to run async fetch in a sync wrapper for st.cache_resource?
                    # Or we can cache the object and run fetch if not indexed?
                    # Better: Run the fetch here using _run
                    _run(idx.fetch_catalog())
                    return idx
                
                try:
                    indexer = get_cached_indexer(origin)
                    if not indexer.catalog:
                        st.warning("Catalog download blocked (429). Switching to slow search mode.")
                        indexer = None # Force fallback to legacy search
                    else:
                        st.success(f"Using cached catalog ({len(indexer.catalog)} variants)")
                except Exception as e:
                    st.error(f"Failed to index catalog: {e}")
                    indexer = None

            with st.spinner(f"Scraping {len(items)} items..."):
                results = _run(scrape_items(
                    items, cms_choice, origin, url_pattern, concurrency, delay_ms, indexer=indexer, fast_mode=fast_mode
                ))
            
            st.success(f"Completed! Processed {len(results)} items.")
            
            # Display results
            if results:
                df = pd.DataFrame(results)
                
                # Ensure all_variant_ids is string to avoid Arrow errors
                if "all_variant_ids" in df.columns:
                    df["all_variant_ids"] = df["all_variant_ids"].astype(str)

                # Reorder columns if possible
                preferred = ["sku", "product_url", "name", "price", "rrp", "discount_percent", 
                           "group_id", "variant_id", "all_variant_ids",
                           "category", "breadcrumbs", "image_url", "error", "url"]
                cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
                df = df[cols]

                st.dataframe(df, use_container_width=True)
                
                # CSV Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download CSV",
                    csv,
                    "results.csv",
                    "text/csv",
                    key='download-csv'
                )
            else:
                st.info("No results found.")

    else:
        col1, col2 = st.columns(2)
        with col1:
            page_url = st.text_input("Category Page URL")
        with col2:
            max_items = st.number_input("Max Items", 1, 1000, 50)

        if st.button("Crawl Page", use_container_width=True):
            if not page_url:
                st.warning("Please enter a URL.")
                return

            with st.spinner("Crawling page..."):
                results = _run(scrape_by_page(
                    page_url, cms_choice, max_items, concurrency, delay_ms
                ))

            st.success(f"Crawled {len(results)} items.")
            
            if results:
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download CSV",
                    csv,
                    "crawl_results.csv",
                    "text/csv"
                )

    if mode == "Catalog Indexer":
        st.info("This mode fetches the entire product catalog from a Shopify site's `products.json` endpoint.")
        
        target_url = st.text_input("Shopify Site URL", "https://legear.com.au")
        
        if st.button("Index Catalog", use_container_width=True):
            from scraper.shopify_catalog import ShopifyCatalogIndexer
            
            indexer = ShopifyCatalogIndexer(target_url)
            
            with st.spinner("Indexing Shopify Catalog... (this may take a while for large sites)"):
                # We need to run the async method
                count = _run(indexer.fetch_catalog())
            
            st.success(f"Successfully indexed {count} variants!")
            
            if indexer.catalog:
                # Convert catalog dict to list of dicts
                data = list(indexer.catalog.values())
                df = pd.DataFrame(data)
                
                # Show preview
                st.dataframe(df, use_container_width=True)
                
                col_d1, col_d2 = st.columns(2)
                
                with col_d1:
                    # 1. Full CSV
                    csv_full = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download Full Catalog (CSV)",
                        csv_full,
                        "shopify_catalog.csv",
                        "text/csv",
                        key="dl_cat_csv"
                    )
                
                with col_d2:
                    # 2. SKUs TXT
                    # Get sorted SKUs
                    all_skus = sorted(indexer.catalog.keys())
                    txt_content = "\n".join(all_skus)
                    st.download_button(
                        "Download SKUs List (.txt)",
                        txt_content,
                        "all_skus.txt",
                        "text/plain",
                        key="dl_cat_txt"
                    )

if __name__ == "__main__":
    main()
