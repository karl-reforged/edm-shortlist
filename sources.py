#!/usr/bin/env python3
"""
Multi-source property ingestion — normalizes listings from Domain, Farmbuy,
REA, and web-scraped sources into a common format for the scoring pipeline.

Each source returns a list of NormalizedProperty dicts with consistent fields.
The main pipeline (search.py) deduplicates and scores them all together.

Sources:
  - Domain API (official, requires production access)
  - Domain Web (scrapes __NEXT_DATA__ from search pages — no API key needed)
  - Farmbuy.com (scrapes embedded JSON from listing pages)
  - REA Web (scrapes ArgonautExchange from realestate.com.au — no API)
  - REA Manual (manually captured listings from email alerts)
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_DIR = Path(__file__).parent
CRITERIA_PATH = BASE_DIR / "criteria.json"

# Headless mode — Domain blocks headless Chrome as of 12 Mar 2026.
# Set PLAYWRIGHT_HEADLESS=true to override (e.g. in CI).
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "false").lower() == "true"


def _retry_session(retries=3, backoff=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    """Create a requests Session with automatic retry and exponential backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def load_criteria():
    with open(CRITERIA_PATH) as f:
        return json.load(f)


# ── Normalized property format ─────────────────────────────────────────────
#
# Every source converts its raw data into this shape:
#
# {
#     "source": "domain" | "farmbuy" | "rea" | "elders" | "str",
#     "source_id": str,          # unique ID from the source
#     "address": str,
#     "suburb": str,
#     "postcode": str,
#     "state": str,
#     "price": int | None,       # numeric price if available
#     "display_price": str,      # human-readable price text
#     "land_sqm": float | None,  # land area in sqm for gate checks
#     "land_ha": float | None,
#     "land_acres": float | None,
#     "bedrooms": int | None,
#     "bathrooms": int | None,
#     "headline": str,
#     "description": str,
#     "listing_url": str,
#     "photo_url": str | None,
#     "lat": float | None,
#     "lng": float | None,
#     "date_listed": str | None,
#     "agent": str | None,
#     "raw": dict,               # original source data for debugging
# }


# ── Source 1: Domain API ───────────────────────────────────────────────────

DOMAIN_AUTH_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_API_BASE = "https://api.domain.com.au"


def _domain_auth():
    """Get Domain OAuth2 bearer token with retry."""
    cid = os.getenv("DOMAIN_CLIENT_ID")
    csec = os.getenv("DOMAIN_CLIENT_SECRET")
    if not cid or not csec:
        return None
    session = _retry_session(retries=3, backoff=1.0)
    resp = session.post(DOMAIN_AUTH_URL, data={
        "client_id": cid, "client_secret": csec,
        "grant_type": "client_credentials", "scope": "api_listings_read",
    }, timeout=15)
    if resp.status_code == 200:
        return resp.json().get("access_token")
    print(f"Domain auth failed: {resp.status_code} — {resp.text[:200]}")
    return None


def fetch_domain(criteria):
    """Fetch listings from Domain API and normalize."""
    token = _domain_auth()
    if not token:
        print("Domain: skipping (no credentials or auth failed)")
        return []

    gates = criteria["gates"]
    postcodes = gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]

    payload = {
        "listingType": "Sale",
        "propertyTypes": ["AcreageSemiRural", "Farm", "Rural"],
        "minPrice": gates["budget"]["min_price"],
        "maxPrice": gates["budget"]["max_price"],
        "minLandArea": gates["land_size"]["min_hectares"] * 10000,
        "maxLandArea": gates["land_size"]["max_hectares"] * 10000,
        "locations": [{"postCode": pc, "state": "NSW"} for pc in postcodes],
        "pageSize": 200,
        "sort": {"sortKey": "DateListed", "direction": "Descending"},
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    session = _retry_session(retries=2, backoff=1.0)
    resp = session.post(f"{DOMAIN_API_BASE}/v1/listings/residential/_search",
                        json=payload, headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"Domain search error: {resp.status_code} — {resp.text[:200]}")
        return []

    results = resp.json()
    print(f"Domain: {len(results)} listings returned")

    normalized = []
    for item in results:
        d = item.get("listing", {})
        prop = d.get("propertyDetails", {})
        price_d = d.get("priceDetails", {})
        media = d.get("media", [])
        photo = next((m.get("url") for m in media if m.get("category") == "Image"), None)

        land_sqm = prop.get("landArea")
        land_ha = round(land_sqm / 10000, 2) if land_sqm else None
        land_acres = round(land_ha * 2.471, 1) if land_ha else None

        addr_parts = [prop.get("streetNumber", ""), prop.get("street", ""),
                      prop.get("suburb", ""), prop.get("state", ""), prop.get("postcode", "")]

        normalized.append({
            "source": "domain",
            "source_id": str(d.get("id", "")),
            "address": " ".join(p for p in addr_parts if p).strip(),
            "suburb": prop.get("suburb", ""),
            "postcode": prop.get("postcode", ""),
            "state": prop.get("state", "NSW"),
            "price": price_d.get("price"),
            "display_price": price_d.get("displayPrice", "Price on application"),
            "land_sqm": land_sqm,
            "land_ha": land_ha,
            "land_acres": land_acres,
            "bedrooms": prop.get("bedrooms"),
            "bathrooms": prop.get("bathrooms"),
            "headline": d.get("headline", ""),
            "description": (d.get("description", "") or "")[:1000],
            "listing_url": f"https://www.domain.com.au/{d.get('listingSlug', '')}",
            "photo_url": photo,
            "lat": prop.get("latitude"),
            "lng": prop.get("longitude"),
            "date_listed": d.get("dateListed"),
            "agent": None,
            "raw": item,
        })

    return normalized


# ── Source 1b: Domain Web (scrape __NEXT_DATA__) ─────────────────────────
#
# Domain uses Next.js — every search page embeds the full listing data in a
# <script id="__NEXT_DATA__"> tag as JSON. This gives us descriptions,
# coordinates, and photos without needing API production access.

DOMAIN_WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


def _domain_web_search_url(postcode, gates):
    """Build a Domain.com.au search URL for a postcode."""
    min_price = gates["budget"]["min_price"]
    max_price = gates["budget"]["max_price"]
    min_land = int(gates["land_size"]["min_hectares"] * 10000)  # sqm
    return (
        f"https://www.domain.com.au/sale/"
        f"?postcode={postcode}"
        f"&ptype=acreage-semi-rural,rural,farm"
        f"&price={min_price}-{max_price}"
        f"&landsize={min_land}-"
        f"&sort=dateupdated-desc"
    )


def _parse_domain_next_data(html):
    """Extract listing data from __NEXT_DATA__ script tag.

    Confirmed structure (10 Mar 2026):
      props.pageProps.componentProps.listingsMap  — dict keyed by listing ID
      props.pageProps.componentProps.listingSearchResultIds  — ordered ID list
      props.pageProps.componentProps.totalPages  — pagination count
    """
    match = re.search(
        r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
        html, re.DOTALL,
    )
    if not match:
        return [], 0

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return [], 0

    try:
        cp = data["props"]["pageProps"]["componentProps"]
        listings_map = cp.get("listingsMap", {})
        result_ids = cp.get("listingSearchResultIds", [])
        total_pages = cp.get("totalPages", 1)
    except (KeyError, TypeError):
        return [], 0

    # Return listings in search result order
    listings = []
    for lid in result_ids:
        entry = listings_map.get(str(lid))
        if entry:
            listings.append(entry)

    # If no result IDs, fall back to all map values
    if not listings and listings_map:
        listings = list(listings_map.values())

    return listings, total_pages


def _normalize_domain_web_listing(item):
    """Normalize a single listing from Domain's __NEXT_DATA__.

    Confirmed structure per listingsMap entry:
      item.listingModel.address  (street, suburb, state, postcode, lat, lng)
      item.listingModel.price    (STRING — "$1,550,000" or "Auction...")
      item.listingModel.features (beds, baths, parking, landSize, landUnit, isRural)
      item.listingModel.images   (array of URL strings)
      item.listingModel.branding.agencyName
      item.listingModel.url      (relative slug)
    """
    lm = item.get("listingModel", {})
    listing_id = str(item.get("id", ""))

    # Address
    addr = lm.get("address", {})
    street = addr.get("street", "")
    suburb = addr.get("suburb", "")
    state = addr.get("state", "NSW")
    postcode = str(addr.get("postcode", ""))
    lat = addr.get("lat")
    lng = addr.get("lng")
    address = f"{street} {suburb} {state} {postcode}".strip()

    # Price — always a string in web data
    price_str = lm.get("price", "")
    display_price = price_str if price_str else "Price on application"
    price = None
    if price_str:
        match = re.search(r'\$[\d,]+', price_str.replace(' ', ''))
        if match:
            try:
                price = int(match.group().replace('$', '').replace(',', ''))
            except ValueError:
                pass

    # Land area — features.landSize + features.landUnit
    features = lm.get("features", {})
    land_size = features.get("landSize")
    land_unit = (features.get("landUnit") or "").lower()
    land_sqm, land_ha, land_acres = None, None, None
    if land_size and isinstance(land_size, (int, float)) and land_size > 0:
        if land_unit == "ha":
            land_ha = round(land_size, 2)
            land_sqm = land_ha * 10000
            land_acres = round(land_ha * 2.471, 1)
        elif land_unit in ("ac", "acres"):
            land_acres = round(land_size, 1)
            land_ha = round(land_size / 2.471, 2)
            land_sqm = land_ha * 10000
        else:
            # Assume sqm
            land_sqm = float(land_size)
            land_ha = round(land_sqm / 10000, 2)
            land_acres = round(land_ha * 2.471, 1)

    beds = features.get("beds")
    baths = features.get("baths")

    # Images — array of URL strings
    images = lm.get("images", [])
    photo_url = images[0] if images else None

    # URL
    slug = lm.get("url", "")
    if slug and not slug.startswith("http"):
        listing_url = f"https://www.domain.com.au{slug}"
    elif slug:
        listing_url = slug
    else:
        listing_url = f"https://www.domain.com.au/{listing_id}"

    # Agent
    branding = lm.get("branding", {})
    agent_name = branding.get("brandName", branding.get("agencyName"))

    return {
        "source": "domain_web",
        "source_id": listing_id,
        "address": address,
        "suburb": suburb,
        "postcode": postcode,
        "state": state,
        "price": price,
        "display_price": display_price,
        "land_sqm": land_sqm,
        "land_ha": land_ha,
        "land_acres": land_acres,
        "bedrooms": beds,
        "bathrooms": baths,
        "headline": "",
        "description": "",
        "listing_url": listing_url,
        "photo_url": photo_url,
        "lat": lat,
        "lng": lng,
        "date_listed": None,
        "agent": agent_name,
        "raw": item,
    }


def _fetch_json_via_browser(page, url):
    """Fetch a URL as JSON using the browser's fetch() API.

    Domain returns full Next.js page data as JSON when Accept: application/json
    is sent. Using the browser's fetch() bypasses TLS fingerprint detection
    that blocks requests/curl.
    """
    try:
        result = page.evaluate("""async (url) => {
            try {
                const resp = await fetch(url, {
                    headers: { 'Accept': 'application/json' }
                });
                if (!resp.ok) return { error: resp.status, body: null };
                const data = await resp.json();
                return { error: null, body: data };
            } catch (e) {
                return { error: e.message, body: null };
            }
        }""", url)
        if result.get("error"):
            return None, result["error"]
        return result.get("body"), None
    except Exception as e:
        return None, str(e)


def _parse_search_json(data):
    """Parse Domain search page JSON for listings and pagination.

    When fetched with Accept: application/json, Domain returns data at two
    possible locations depending on the response format:
      - props.listingsMap (JSON fetch via Accept header)
      - props.pageProps.componentProps.listingsMap (HTML __NEXT_DATA__)
    """
    try:
        props = data.get("props", {})

        # Try direct props path first (JSON fetch response)
        listings_map = props.get("listingsMap")
        result_ids = props.get("listingSearchResultIds")
        total_pages = props.get("totalPages", 1)

        # Fallback to nested pageProps path (__NEXT_DATA__ format)
        if listings_map is None:
            cp = props.get("pageProps", {}).get("componentProps", {})
            listings_map = cp.get("listingsMap", {})
            result_ids = cp.get("listingSearchResultIds", [])
            total_pages = cp.get("totalPages", 1)

        if not listings_map:
            return [], 0

        if not result_ids:
            result_ids = []
    except (KeyError, TypeError, AttributeError):
        return [], 0

    listings = []
    for lid in result_ids:
        entry = listings_map.get(str(lid))
        if entry:
            listings.append(entry)

    if not listings and listings_map:
        listings = list(listings_map.values())

    return listings, total_pages


def _parse_detail_json(data):
    """Parse Domain detail page JSON for description, headline, and coordinates.

    When fetched with Accept: application/json, detail pages return data at
    two possible locations:
      - props.description / props.headline / props.map (JSON fetch)
      - props.pageProps.componentProps.description (HTML __NEXT_DATA__)
    """
    try:
        props = data.get("props", {})
    except (AttributeError, TypeError):
        return {}

    # Try direct props path first (JSON fetch response)
    desc = props.get("description", "")
    headline = props.get("headline", "")
    map_data = props.get("map", {})

    # Fallback to nested pageProps path
    if not desc:
        cp = props.get("pageProps", {}).get("componentProps", {})
        desc = cp.get("description", "")
        if not headline:
            headline = cp.get("headline", "")
        if not map_data:
            map_data = cp.get("map", {})

    # Description can be a list of paragraphs or a single string
    if isinstance(desc, list):
        desc = "\n".join(str(s) for s in desc if s)

    # Fallback: rootGraphQuery.listingByIdV2.description
    if not desc:
        try:
            rg = props.get("rootGraphQuery", {})
            if not rg:
                rg = props.get("pageProps", {}).get("rootGraphQuery", {})
            desc = rg.get("listingByIdV2", {}).get("description", "")
        except (KeyError, TypeError, AttributeError):
            pass

    lat = map_data.get("latitude") if isinstance(map_data, dict) else None
    lng = map_data.get("longitude") if isinstance(map_data, dict) else None

    # Check if listing has been archived/removed
    is_archived = props.get("isArchived", False)

    return {
        "description": desc[:2000] if desc else "",
        "headline": headline or "",
        "lat": lat,
        "lng": lng,
        "is_archived": is_archived,
    }


def _batch_fetch_details(page, urls, batch_size=10):
    """Fetch multiple detail page URLs concurrently using browser fetch().

    Returns a dict of {url: parsed_detail_data}.
    Fetches in batches to avoid overwhelming Domain.
    """
    results = {}

    for batch_start in range(0, len(urls), batch_size):
        batch = urls[batch_start:batch_start + batch_size]

        try:
            batch_results = page.evaluate("""async (urls) => {
                const results = await Promise.allSettled(
                    urls.map(async (url) => {
                        const resp = await fetch(url, {
                            headers: { 'Accept': 'application/json' }
                        });
                        if (!resp.ok) return { url, error: resp.status, body: null };
                        const data = await resp.json();
                        return { url, error: null, body: data };
                    })
                );
                return results.map((r, i) => {
                    if (r.status === 'fulfilled') return r.value;
                    return { url: urls[i], error: r.reason?.message || 'failed', body: null };
                });
            }""", batch)
        except Exception as e:
            print(f"  Batch fetch error: {e}")
            continue

        for item in batch_results:
            url = item.get("url", "")
            if item.get("body"):
                detail = _parse_detail_json(item["body"])
                if detail.get("is_archived"):
                    continue  # skip removed/archived listings
                if detail.get("description"):
                    results[url] = detail

        # Brief delay between batches
        if batch_start + batch_size < len(urls):
            time.sleep(0.5)

    return results


def _fetch_page_with_playwright(browser, url):
    """Fetch a page using Playwright and return the HTML content.

    Legacy fallback — used only if JSON fetch approach fails.
    """
    page = browser.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector('script#__NEXT_DATA__', state="attached", timeout=10000)
        html = page.content()
        return html
    except Exception as e:
        print(f"  Playwright error: {e}")
        return None
    finally:
        page.close()


def fetch_domain_web(criteria):
    """Fetch Domain.com.au listings using browser-based JSON fetch.

    Opens one Playwright page, then uses fetch() with Accept: application/json
    to get search results and detail pages concurrently. This is ~6x faster than
    navigating to each page individually.

    Pipeline: search pages (JSON) → normalize → batch detail fetch → enrich descriptions
    """
    gates = criteria["gates"]
    postcodes = gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]
    target_postcodes = set(postcodes)

    all_listings = []
    seen_ids = set()

    print(f"Domain Web: searching {len(postcodes)} postcodes via JSON fetch...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        try:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )

            # Navigate to Domain once to establish browser context/cookies
            page = context.new_page()
            try:
                page.goto("https://www.domain.com.au/sale/", wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                print(f"  Domain Web: initial page load failed — {e}")
                return []

            consecutive_empty = 0

            # ── Phase 1: Fetch search pages via JSON ──
            for i, pc in enumerate(postcodes):
                url = _domain_web_search_url(pc, gates)
                data, err = _fetch_json_via_browser(page, url)

                if err or not data:
                    print(f"  [{pc}] fetch failed: {err}")
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        print("  WARNING: 5 consecutive failures — Domain may be blocking. Stopping.")
                        break
                    continue

                listings, max_page = _parse_search_json(data)
                page_count = len(listings)

                if page_count > 0:
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        print("  WARNING: 5 consecutive empty results — page structure may have changed. Stopping.")
                        break

                # Fetch page 2+ if available (limit to 5 pages per postcode)
                for page_num in range(2, min(max_page + 1, 6)):
                    page_url = f"{url}&page={page_num}"
                    more_data, err2 = _fetch_json_via_browser(page, page_url)
                    if more_data:
                        more, _ = _parse_search_json(more_data)
                        listings.extend(more)
                        if more:
                            print(f"  [{pc}] +{len(more)} from page {page_num}")
                    time.sleep(0.3)  # respectful delay between pages

                for item in listings:
                    try:
                        prop = _normalize_domain_web_listing(item)
                    except Exception:
                        continue

                    if prop["source_id"] in seen_ids:
                        continue
                    seen_ids.add(prop["source_id"])

                    if prop["postcode"] and prop["postcode"] not in target_postcodes:
                        continue

                    all_listings.append(prop)

                if page_count > 0:
                    print(f"  [{pc}] {page_count} listings (page 1)")

                # Brief delay between postcodes
                if i < len(postcodes) - 1:
                    time.sleep(0.3)

            # ── Phase 2: Batch fetch detail pages for descriptions ──
            need_details = [p for p in all_listings if not p.get("description")]
            if need_details:
                print(f"  Fetching descriptions for {len(need_details)} listings (batches of 10)...")
                detail_urls = [p["listing_url"] for p in need_details]
                details = _batch_fetch_details(page, detail_urls, batch_size=10)

                enriched = 0
                for prop in need_details:
                    detail = details.get(prop["listing_url"])
                    if detail:
                        if detail.get("description"):
                            prop["description"] = detail["description"]
                            enriched += 1
                        if detail.get("headline"):
                            prop["headline"] = detail["headline"]
                        if detail.get("lat") and not prop.get("lat"):
                            prop["lat"] = detail["lat"]
                        if detail.get("lng") and not prop.get("lng"):
                            prop["lng"] = detail["lng"]

                print(f"  Enriched {enriched}/{len(need_details)} with descriptions")
            else:
                print("  All listings already have descriptions")

            page.close()

        finally:
            browser.close()

    print(f"Domain Web: {len(all_listings)} total listings across all postcodes")
    return all_listings


# ── Source 2: Farmbuy.com ──────────────────────────────────────────────────

FARMBUY_URL = "https://www.farmbuy.com/find-rural-property"


def _parse_farmbuy_price(price_text):
    """Extract numeric price from Farmbuy price text."""
    if not price_text:
        return None
    # Match patterns like "$1,200,000", "$850,000", "From $900,000"
    match = re.search(r'\$[\d,]+', price_text.replace(' ', ''))
    if match:
        return int(match.group().replace('$', '').replace(',', ''))
    return None


def _parse_farmbuy_area(area_text):
    """Parse land area text into sqm. Returns (sqm, ha, acres)."""
    if not area_text:
        return None, None, None
    text = area_text.lower().strip()

    # Try acres first
    match = re.search(r'([\d,.]+)\s*(?:ac|acres?)', text)
    if match:
        acres = float(match.group(1).replace(',', ''))
        ha = acres / 2.471
        return ha * 10000, round(ha, 2), round(acres, 1)

    # Try hectares
    match = re.search(r'([\d,.]+)\s*(?:ha|hectares?)', text)
    if match:
        ha = float(match.group(1).replace(',', ''))
        return ha * 10000, round(ha, 2), round(ha * 2.471, 1)

    # Try just a number (assume acres for rural)
    match = re.search(r'([\d,.]+)', text)
    if match:
        val = float(match.group(1).replace(',', ''))
        if val > 500:  # probably sqm
            ha = val / 10000
            return val, round(ha, 2), round(ha * 2.471, 1)
        else:  # probably acres
            ha = val / 2.471
            return ha * 10000, round(ha, 2), round(val, 1)

    return None, None, None


def fetch_farmbuy(criteria):
    """Fetch listings from Farmbuy.com and normalize."""
    gates = criteria["gates"]
    postcodes = set(
        gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]
    )

    # Farmbuy search — lifestyle + mixed farming in NSW
    params = {
        "state": "nsw",
        "min_price": str(gates["budget"]["min_price"]),
        "max_price": str(gates["budget"]["max_price"]),
    }

    try:
        session = _retry_session(retries=2, backoff=1.0)
        resp = session.get(FARMBUY_URL, params=params, timeout=30,
                           headers={"User-Agent": "Mozilla/5.0 (property search tool)"})
        if resp.status_code != 200:
            print(f"Farmbuy: HTTP {resp.status_code}")
            return []
    except requests.RequestException as e:
        print(f"Farmbuy: request failed: {e}")
        return []

    # Farmbuy embeds property data in <script type="application/json"> blocks
    properties_raw = []
    json_blocks = re.findall(
        r'<script\s+type="application/json"[^>]*>(.*?)</script>', resp.text, re.DOTALL
    )
    for block in json_blocks:
        block = block.strip()
        if '"address"' not in block or '"priceText"' not in block:
            continue
        try:
            data = json.loads(block)
            if isinstance(data, list):
                properties_raw.extend(data)
            elif isinstance(data, dict) and "address" in data:
                properties_raw.append(data)
        except json.JSONDecodeError:
            continue

    print(f"Farmbuy: {len(properties_raw)} listings parsed from page")

    normalized = []
    for item in properties_raw:
        addr = item.get("address", {})
        postcode = str(addr.get("postcode", ""))

        # Filter to our target postcodes
        if postcode and postcode not in postcodes:
            continue

        price = _parse_farmbuy_price(item.get("priceText"))
        land_sqm, land_ha, land_acres = _parse_farmbuy_area(item.get("landArea"))
        meta = item.get("meta", {})

        listing_url = item.get("url", "")
        if listing_url and not listing_url.startswith("http"):
            listing_url = f"https://www.farmbuy.com{listing_url}"

        normalized.append({
            "source": "farmbuy",
            "source_id": str(item.get("id", "")),
            "address": addr.get("full", ""),
            "suburb": addr.get("suburb", ""),
            "postcode": postcode,
            "state": addr.get("state", "NSW"),
            "price": price,
            "display_price": item.get("priceText", "Price on application"),
            "land_sqm": land_sqm,
            "land_ha": land_ha,
            "land_acres": land_acres,
            "bedrooms": meta.get("bed"),
            "bathrooms": meta.get("bath"),
            "headline": item.get("heading", ""),
            "description": item.get("description", "")[:1000] if item.get("description") else "",
            "listing_url": listing_url,
            "photo_url": item.get("mainTileImageURL"),
            "lat": None,  # Farmbuy doesn't expose coords in list view
            "lng": None,
            "date_listed": None,
            "agent": item.get("realestate"),
            "raw": item,
        })

    print(f"Farmbuy: {len(normalized)} in target postcodes")
    return normalized


# ── Source 3: REA Web (scrape ArgonautExchange) ──────────────────────────
#
# realestate.com.au embeds all listing data in a window.ArgonautExchange
# JSON object. This is the same data the React frontend renders from.
# No API key needed.

REA_WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


def _rea_search_url(postcode, gates, page=1):
    """Build a realestate.com.au search URL for a postcode."""
    min_price = gates["budget"]["min_price"]
    max_price = gates["budget"]["max_price"]
    min_land = int(gates["land_size"]["min_hectares"] * 10000)  # sqm
    base = (
        f"https://www.realestate.com.au/buy/"
        f"property-acreage+semi-rural-rural/"
        f"in-nsw+{postcode}/"
        f"list-{page}"
        f"?maxPrice={max_price}&minPrice={min_price}"
        f"&minLandArea={min_land}"
    )
    return base


def _parse_rea_argonaut(html):
    """Extract listing data from REA's ArgonautExchange."""
    # Find the ArgonautExchange script
    match = re.search(r'window\.ArgonautExchange\s*=\s*(\{.+?\});\s*</script>', html, re.DOTALL)
    if not match:
        return [], 0

    try:
        exchange = json.loads(match.group(1))
    except json.JSONDecodeError:
        return [], 0

    listings = []
    max_page = 1

    # ArgonautExchange contains urqlClientCache with nested JSON
    # Try the search experience key first
    for cache_key in exchange:
        if "search" in cache_key.lower() or "property" in cache_key.lower():
            cache_data = exchange[cache_key]
            if isinstance(cache_data, dict) and "urqlClientCache" in cache_data:
                try:
                    urql = cache_data["urqlClientCache"]
                    if isinstance(urql, str):
                        urql = json.loads(urql)
                    # urql is a dict of query hashes -> results
                    for query_hash, query_data in urql.items():
                        if isinstance(query_data, dict) and "data" in query_data:
                            data_str = query_data["data"]
                            if isinstance(data_str, str):
                                data = json.loads(data_str)
                            else:
                                data = data_str
                            # Look for results in the data
                            items, mp = _extract_rea_results(data)
                            if items:
                                listings.extend(items)
                            if mp > max_page:
                                max_page = mp
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

    # Fallback: search all exchange keys for listing data
    if not listings:
        for key, val in exchange.items():
            if isinstance(val, dict):
                items = _find_listings_in_dict(val)
                if items:
                    listings.extend(items)

    return listings, max_page


def _extract_rea_results(data, depth=0):
    """Extract listing results from REA's decoded data."""
    if depth > 5:
        return [], 1
    max_page = 1

    if isinstance(data, dict):
        # Check for results.exact.items pattern
        results = data.get("results", {})
        if isinstance(results, dict):
            exact = results.get("exact", results)
            if isinstance(exact, dict):
                items = exact.get("items", exact.get("results", []))
                pagination = exact.get("pagination", results.get("pagination", {}))
                if isinstance(pagination, dict):
                    max_page = pagination.get("maxPageNumberAvailable",
                                             pagination.get("totalPages", 1))
                if isinstance(items, list) and items:
                    return items, max_page

        # Check for listings array
        for key in ["listings", "items", "propertyListings"]:
            if key in data and isinstance(data[key], list):
                return data[key], max_page

        # Recurse into values
        for v in data.values():
            items, mp = _extract_rea_results(v, depth + 1)
            if items:
                return items, mp

    return [], max_page


def _normalize_rea_listing(item):
    """Normalize a single listing from REA's ArgonautExchange."""
    # REA listings can be wrapped in a "listing" key
    d = item.get("listing", item)

    # Address
    addr_obj = d.get("address", {})
    if isinstance(addr_obj, str):
        address = addr_obj
        suburb = ""
        postcode = ""
        state = "NSW"
        lat, lng = None, None
    else:
        suburb = addr_obj.get("suburb", addr_obj.get("suburbName", ""))
        postcode = str(addr_obj.get("postcode", ""))
        state = addr_obj.get("state", "NSW")
        street = addr_obj.get("display", addr_obj.get("streetAddress", ""))
        address = street if street else f"{suburb} {state} {postcode}"
        geocode = addr_obj.get("geocode", addr_obj.get("location", {}))
        if isinstance(geocode, dict):
            lat = geocode.get("latitude", geocode.get("lat"))
            lng = geocode.get("longitude", geocode.get("lng"))
        else:
            lat, lng = None, None

    # Price
    price_obj = d.get("price", {})
    if isinstance(price_obj, dict):
        display_price = price_obj.get("display", price_obj.get("displayPrice", ""))
        price = price_obj.get("value", price_obj.get("from"))
    elif isinstance(price_obj, str):
        display_price = price_obj
        price = None
    else:
        display_price = "Price on application"
        price = None

    # Extract numeric price from display
    if price is None and display_price:
        match = re.search(r'\$[\d,]+', str(display_price).replace(' ', ''))
        if match:
            try:
                price = int(match.group().replace('$', '').replace(',', ''))
            except ValueError:
                pass

    # Land area
    land_sqm, land_ha, land_acres = None, None, None
    sizes = d.get("propertySizes", d.get("propertySize", {}))
    if isinstance(sizes, dict):
        land_obj = sizes.get("land", sizes.get("landSize", {}))
        if isinstance(land_obj, dict):
            display_val = land_obj.get("displayValue", land_obj.get("value", ""))
            size_unit = land_obj.get("sizeUnit", land_obj.get("unit", ""))
            if display_val:
                area_text = f"{display_val} {size_unit}".strip()
                land_sqm, land_ha, land_acres = _parse_farmbuy_area(area_text)
    elif isinstance(sizes, list):
        for s in sizes:
            if isinstance(s, dict) and s.get("type", "").lower() == "land":
                area_text = f"{s.get('displayValue', '')} {s.get('sizeUnit', '')}".strip()
                land_sqm, land_ha, land_acres = _parse_farmbuy_area(area_text)
                break

    # Features
    gen_features = d.get("generalFeatures", d.get("features", {}))
    if isinstance(gen_features, dict):
        beds = gen_features.get("bedrooms", gen_features.get("beds",
               gen_features.get("bed", {}).get("value")))
        baths = gen_features.get("bathrooms", gen_features.get("baths",
                gen_features.get("bath", {}).get("value")))
    else:
        beds, baths = None, None

    # Description
    description = d.get("description", "")
    if isinstance(description, dict):
        description = description.get("text", description.get("value", ""))
    if description:
        description = description[:1000]
    headline = d.get("title", d.get("headline", ""))

    # Photos
    media = d.get("media", d.get("images", {}))
    photo_url = None
    if isinstance(media, dict):
        images = media.get("images", media.get("photos", []))
        if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, dict):
                # REA uses templated URLs with {size} — use a reasonable size
                tmpl = first.get("templatedUrl", first.get("url", first.get("uri", "")))
                photo_url = tmpl.replace("{size}", "800x600") if tmpl else None
            elif isinstance(first, str):
                photo_url = first
    elif isinstance(media, list) and media:
        first = media[0]
        if isinstance(first, dict):
            photo_url = first.get("url", first.get("uri"))
        elif isinstance(first, str):
            photo_url = first

    # URL
    links = d.get("_links", d.get("links", {}))
    listing_url = ""
    if isinstance(links, dict):
        canonical = links.get("canonical", links.get("self", {}))
        if isinstance(canonical, dict):
            listing_url = canonical.get("href", canonical.get("url", ""))
        elif isinstance(canonical, str):
            listing_url = canonical
    if not listing_url:
        listing_url = d.get("url", d.get("prettyUrl", ""))
    if listing_url and not listing_url.startswith("http"):
        listing_url = f"https://www.realestate.com.au{listing_url}"

    # ID
    listing_id = str(d.get("id", d.get("listingId", "")))

    # Agent
    company = d.get("listingCompany", d.get("agency", {}))
    agent_name = None
    if isinstance(company, dict):
        agent_name = company.get("name", company.get("agencyName"))

    return {
        "source": "rea_web",
        "source_id": listing_id,
        "address": address,
        "suburb": suburb,
        "postcode": postcode,
        "state": state,
        "price": price,
        "display_price": display_price or "Price on application",
        "land_sqm": land_sqm,
        "land_ha": land_ha,
        "land_acres": land_acres,
        "bedrooms": beds,
        "bathrooms": baths,
        "headline": headline or "",
        "description": description or "",
        "listing_url": listing_url,
        "photo_url": photo_url,
        "lat": lat,
        "lng": lng,
        "date_listed": d.get("dateListed", d.get("listDate")),
        "agent": agent_name,
        "raw": item,
    }


def fetch_rea_web(criteria):
    """Scrape realestate.com.au search pages via Playwright.

    NOTE: REA aggressively blocks automated browsers (returns 429).
    This source is kept for future attempts but currently returns 0 listings.
    Use REA saved search email alerts for coverage instead.
    """
    gates = criteria["gates"]
    postcodes = gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]
    target_postcodes = set(postcodes)

    all_listings = []
    seen_ids = set()

    print(f"REA Web: testing access with first postcode...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        try:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )

            # Test first postcode — if blocked, skip all
            test_url = _rea_search_url(postcodes[0], gates)
            page = context.new_page()
            try:
                resp = page.goto(test_url, wait_until="domcontentloaded", timeout=30000)
                if resp and resp.status == 429:
                    print("REA Web: blocked (429) — use saved search email alerts instead")
                    return []
            except Exception as e:
                print(f"REA Web: connection failed — {e}")
                return []
            finally:
                page.close()

            page = context.new_page()
            try:
                page.goto(test_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)
                html = page.content()
            finally:
                page.close()

            listings, max_page = _parse_rea_argonaut(html)
            if not listings:
                print("REA Web: no listings found in page data — likely blocked")
                return []

            # If we got here, REA is accessible — process all postcodes
            print(f"REA Web: access OK, searching {len(postcodes)} postcodes...")
            for i, pc in enumerate(postcodes):
                url = _rea_search_url(pc, gates)
                rea_page = context.new_page()
                try:
                    resp = rea_page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    if not resp or resp.status != 200:
                        continue
                    rea_page.wait_for_timeout(2000)
                    page_html = rea_page.content()
                except Exception:
                    continue
                finally:
                    rea_page.close()

                pc_listings, _ = _parse_rea_argonaut(page_html)
                for item in pc_listings:
                    try:
                        prop = _normalize_rea_listing(item)
                    except Exception:
                        continue
                    if prop["source_id"] in seen_ids:
                        continue
                    seen_ids.add(prop["source_id"])
                    if prop["postcode"] and prop["postcode"] not in target_postcodes:
                        continue
                    all_listings.append(prop)

                if pc_listings:
                    print(f"  [{pc}] {len(pc_listings)} listings")

                if i < len(postcodes) - 1:
                    time.sleep(1.0)

        finally:
            browser.close()

    print(f"REA Web: {len(all_listings)} total listings across all postcodes")
    return all_listings


# ── Source 3b: REA Manual (legacy) ───────────────────────────────────────

REA_MANUAL_PATH = BASE_DIR / "data" / "listings" / "rea_manual.json"


def fetch_rea_manual(criteria):
    """Load manually captured REA listings."""
    if not REA_MANUAL_PATH.exists():
        return []
    with open(REA_MANUAL_PATH) as f:
        items = json.load(f)
    print(f"REA (manual): {len(items)} listings loaded")
    for item in items:
        item["source"] = "rea"
    return items


# ── Deduplication ──────────────────────────────────────────────────────────

def deduplicate(properties):
    """Remove duplicate listings across sources. Keeps the richest version."""
    seen = {}
    for prop in properties:
        # Deduplicate by address (normalized) or by exact postcode+suburb+price
        key_parts = [
            prop.get("suburb", "").lower().strip(),
            prop.get("postcode", ""),
        ]
        # If we have a price, include it in the key
        if prop.get("price"):
            key_parts.append(str(prop["price"]))
        # If we have an address, use the street part
        addr = prop.get("address", "").lower().strip()
        if addr:
            # Extract street number + name (first two words typically)
            addr_words = addr.split()[:3]
            key_parts.extend(addr_words)

        key = "|".join(key_parts)

        if key in seen:
            existing = seen[key]
            # Prefer Domain API = Domain Web (richest) > REA Web > Farmbuy > REA manual
            source_priority = {"domain": 4, "domain_web": 4, "rea_web": 3, "farmbuy": 2, "rea_alert": 2, "listing_loop": 2, "property_whispers": 2, "rea": 1}
            if source_priority.get(prop["source"], 0) > source_priority.get(existing["source"], 0):
                seen[key] = prop
            # If same source priority, prefer the one with more data
            elif prop.get("lat") and not existing.get("lat"):
                seen[key] = prop
        else:
            seen[key] = prop

    deduped = list(seen.values())
    return deduped


# ── Detail page fetcher ───────────────────────────────────────────────────

def enrich_with_descriptions(properties):
    """Fetch detail pages for Domain Web listings that still lack descriptions.

    Since fetch_domain_web() now fetches descriptions in-line via JSON fetch,
    this is a fallback for any listings that slipped through (e.g. batch fetch
    errors). Uses the same concurrent JSON fetch approach.
    """
    need_fetch = [
        p for p in properties
        if p.get("source") == "domain_web"
        and not p.get("description")
        and p.get("listing_url")
    ]

    if not need_fetch:
        print("Detail fetch: all listings already have descriptions")
        return properties

    print(f"Detail fetch (fallback): {len(need_fetch)} listings still need descriptions...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        try:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )

            page = context.new_page()
            try:
                page.goto("https://www.domain.com.au/sale/", wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                print(f"  Detail fetch: initial page load failed — {e}")
                return properties

            detail_urls = [prop["listing_url"] for prop in need_fetch]
            details = _batch_fetch_details(page, detail_urls, batch_size=10)

            enriched = 0
            for prop in need_fetch:
                detail = details.get(prop["listing_url"])
                if detail:
                    if detail.get("description"):
                        prop["description"] = detail["description"]
                        enriched += 1
                    if detail.get("headline"):
                        prop["headline"] = detail["headline"]
                    if detail.get("lat") and not prop.get("lat"):
                        prop["lat"] = detail["lat"]
                    if detail.get("lng") and not prop.get("lng"):
                        prop["lng"] = detail["lng"]

            page.close()
            print(f"Detail fetch (fallback): {enriched}/{len(need_fetch)} enriched")

        finally:
            browser.close()

    return properties


# ── Source 4: Email alerts (REA, Listing Loop, Property Whispers) ─────────

GMAIL_IMAP_HOST = "imap.gmail.com"

# Sender patterns — match From: addresses for each alert service
_ALERT_SENDERS = {
    "rea": ["noreply@realestate.com.au", "alerts@realestate.com.au"],
    "listing_loop": ["noreply@listingloop.com.au", "alerts@listingloop.com.au"],
    "property_whispers": ["noreply@propertywhispers.com.au", "alerts@propertywhispers.com.au"],
}

# Label to apply after processing (avoids re-reading same emails)
_PROCESSED_LABEL = "BoltHole/Processed"


def _connect_gmail_imap():
    """Connect to Gmail IMAP using app password from .env."""
    import imaplib
    user = os.getenv("GMAIL_USER")
    password = os.getenv("GMAIL_APP_PASSWORD")
    if not user or not password:
        return None

    try:
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, timeout=30)
        mail.login(user, password)
        return mail
    except Exception as e:
        print(f"Gmail IMAP login failed: {e}")
        return None


def _search_emails_from(mail, senders, days_back=8):
    """Search Gmail for unprocessed emails from given senders within N days."""
    import email as email_mod
    from datetime import timedelta
    from email.utils import parsedate_to_datetime

    since = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    results = []

    mail.select("INBOX")

    for sender in senders:
        query = f'(FROM "{sender}" SINCE {since})'
        status, data = mail.search(None, query)
        if status != "OK" or not data[0]:
            continue

        msg_ids = data[0].split()
        for msg_id in msg_ids:
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue
            raw = msg_data[0][1]
            msg = email_mod.message_from_bytes(raw)

            # Extract HTML body
            html_body = None
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        html_body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                        break
            elif msg.get_content_type() == "text/html":
                html_body = msg.get_payload(decode=True).decode("utf-8", errors="replace")

            if html_body:
                results.append({
                    "msg_id": msg_id,
                    "sender": sender,
                    "subject": msg.get("Subject", ""),
                    "html": html_body,
                })

    return results


def _parse_rea_alert(html, criteria):
    """Parse REA saved search alert email HTML into normalized properties.

    REA alert emails contain property cards with links like:
        realestate.com.au/property-rural-nsw-<suburb>-<id>
    Each card typically has: address, price, land size, beds/baths, photo.
    """
    properties = []
    target_postcodes = set(
        criteria["gates"]["geography"]["postcodes_west"]
        + criteria["gates"]["geography"]["postcodes_south"]
    )

    # REA alert emails embed property links with IDs
    # Pattern: /property-<type>-<state>-<suburb>-<id>
    listing_links = re.findall(
        r'href="(https?://www\.realestate\.com\.au/property-[^"]+)"',
        html
    )
    # Deduplicate URLs
    seen_urls = set()
    unique_links = []
    for link in listing_links:
        clean = link.split("?")[0]  # strip tracking params
        if clean not in seen_urls:
            seen_urls.add(clean)
            unique_links.append(clean)

    # Extract listing ID from URL
    for url in unique_links:
        match = re.search(r'property-\w+-\w+-[\w-]+-(\d+)', url)
        if not match:
            continue
        listing_id = match.group(1)

        # Try to extract suburb from URL
        suburb_match = re.search(r'property-\w+-\w+-([\w-]+)-\d+', url)
        suburb = suburb_match.group(1).replace("-", " ").title() if suburb_match else ""

        # Extract surrounding text for price/land hints
        # Find the link in HTML and grab nearby text
        idx = html.find(url)
        context_html = html[max(0, idx-500):idx+1000] if idx > -1 else ""
        context_text = re.sub(r'<[^>]+>', ' ', context_html)

        price = _extract_price_from_text(context_text)
        land_acres = _extract_land_from_text(context_text)

        # Extract photo URL from nearby img tag
        photo = None
        img_match = re.search(r'<img[^>]+src="(https?://[^"]+\.(?:jpg|jpeg|png|webp))', context_html)
        if img_match:
            photo = img_match.group(1)

        prop = {
            "source": "rea_alert",
            "source_id": f"rea_{listing_id}",
            "address": "",
            "suburb": suburb,
            "postcode": "",
            "state": "NSW",
            "price": price,
            "display_price": f"${price:,.0f}" if price else "",
            "land_sqm": (land_acres * 4046.86) if land_acres else None,
            "land_ha": (land_acres * 0.404686) if land_acres else None,
            "land_acres": land_acres,
            "bedrooms": None,
            "bathrooms": None,
            "headline": "",
            "description": "",
            "listing_url": url,
            "photo_url": photo,
            "lat": None,
            "lng": None,
            "agent": None,
            "raw": {"alert_source": "rea"},
        }
        properties.append(prop)

    return properties


def _parse_listing_loop_alert(html, criteria):
    """Parse Listing Loop off-market alert email HTML into normalized properties.

    Listing Loop emails contain off-market property cards with address, price guide,
    and links to listingloop.com.au/property/<id>.
    """
    properties = []

    # Listing Loop property links
    listing_links = re.findall(
        r'href="(https?://(?:www\.)?listingloop\.com\.au/property/[^"]+)"',
        html
    )
    seen_urls = set()
    unique_links = []
    for link in listing_links:
        clean = link.split("?")[0]
        if clean not in seen_urls:
            seen_urls.add(clean)
            unique_links.append(clean)

    for url in unique_links:
        # Extract ID from URL
        id_match = re.search(r'/property/(\d+)', url)
        listing_id = id_match.group(1) if id_match else url.split("/")[-1]

        idx = html.find(url)
        context_html = html[max(0, idx-500):idx+1000] if idx > -1 else ""
        context_text = re.sub(r'<[^>]+>', ' ', context_html)

        # Try to extract address from context
        address = ""
        addr_match = re.search(r'(\d+\s+\w[\w\s]+(?:Road|Street|Lane|Drive|Court|Place|Avenue|Way|Close))',
                               context_text, re.IGNORECASE)
        if addr_match:
            address = addr_match.group(1).strip()

        suburb = ""
        # Look for NSW suburb patterns (word followed by NSW or postcode)
        suburb_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*,?\s*NSW', context_text)
        if suburb_match:
            suburb = suburb_match.group(1).strip()

        postcode = ""
        pc_match = re.search(r'\b(2\d{3})\b', context_text)
        if pc_match:
            postcode = pc_match.group(1)

        price = _extract_price_from_text(context_text)
        land_acres = _extract_land_from_text(context_text)

        photo = None
        img_match = re.search(r'<img[^>]+src="(https?://[^"]+\.(?:jpg|jpeg|png|webp))', context_html)
        if img_match:
            photo = img_match.group(1)

        prop = {
            "source": "listing_loop",
            "source_id": f"ll_{listing_id}",
            "address": address,
            "suburb": suburb,
            "postcode": postcode,
            "state": "NSW",
            "price": price,
            "display_price": f"${price:,.0f}" if price else "",
            "land_sqm": (land_acres * 4046.86) if land_acres else None,
            "land_ha": (land_acres * 0.404686) if land_acres else None,
            "land_acres": land_acres,
            "bedrooms": None,
            "bathrooms": None,
            "headline": "",
            "description": "",
            "listing_url": url,
            "photo_url": photo,
            "lat": None,
            "lng": None,
            "agent": None,
            "raw": {"alert_source": "listing_loop"},
        }
        properties.append(prop)

    return properties


def _parse_property_whispers_alert(html, criteria):
    """Parse Property Whispers off-market alert email HTML into normalized properties.

    Property Whispers sends off-market rural listings via email with links
    to propertywhispers.com.au.
    """
    properties = []

    listing_links = re.findall(
        r'href="(https?://(?:www\.)?propertywhispers\.com\.au/[^"]*property[^"]*)"',
        html
    )
    seen_urls = set()
    unique_links = []
    for link in listing_links:
        clean = link.split("?")[0]
        if clean not in seen_urls:
            seen_urls.add(clean)
            unique_links.append(clean)

    for url in unique_links:
        id_match = re.search(r'/(\d+)', url)
        listing_id = id_match.group(1) if id_match else url.split("/")[-1]

        idx = html.find(url)
        context_html = html[max(0, idx-500):idx+1000] if idx > -1 else ""
        context_text = re.sub(r'<[^>]+>', ' ', context_html)

        address = ""
        addr_match = re.search(r'(\d+\s+\w[\w\s]+(?:Road|Street|Lane|Drive|Court|Place|Avenue|Way|Close))',
                               context_text, re.IGNORECASE)
        if addr_match:
            address = addr_match.group(1).strip()

        suburb = ""
        suburb_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*,?\s*NSW', context_text)
        if suburb_match:
            suburb = suburb_match.group(1).strip()

        postcode = ""
        pc_match = re.search(r'\b(2\d{3})\b', context_text)
        if pc_match:
            postcode = pc_match.group(1)

        price = _extract_price_from_text(context_text)
        land_acres = _extract_land_from_text(context_text)

        photo = None
        img_match = re.search(r'<img[^>]+src="(https?://[^"]+\.(?:jpg|jpeg|png|webp))', context_html)
        if img_match:
            photo = img_match.group(1)

        prop = {
            "source": "property_whispers",
            "source_id": f"pw_{listing_id}",
            "address": address,
            "suburb": suburb,
            "postcode": postcode,
            "state": "NSW",
            "price": price,
            "display_price": f"${price:,.0f}" if price else "",
            "land_sqm": (land_acres * 4046.86) if land_acres else None,
            "land_ha": (land_acres * 0.404686) if land_acres else None,
            "land_acres": land_acres,
            "bedrooms": None,
            "bathrooms": None,
            "headline": "",
            "description": "",
            "listing_url": url,
            "photo_url": photo,
            "lat": None,
            "lng": None,
            "agent": None,
            "raw": {"alert_source": "property_whispers"},
        }
        properties.append(prop)

    return properties


def _extract_price_from_text(text):
    """Extract price from surrounding text. Returns int or None."""
    # $1,650,000 or $1.65M or $1,650k
    m = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*[Mm](?:illion)?', text)
    if m:
        return int(float(m.group(1).replace(",", "")) * 1_000_000)
    m = re.search(r'\$\s*([\d,]+)\s*[Kk]', text)
    if m:
        return int(float(m.group(1).replace(",", "")) * 1_000)
    m = re.search(r'\$\s*([\d,]+)', text)
    if m:
        val = int(m.group(1).replace(",", ""))
        if val > 50000:  # ignore small numbers
            return val
    return None


def _extract_land_from_text(text):
    """Extract land size in acres from surrounding text. Returns float or None."""
    # "100 acres" or "100ac"
    m = re.search(r'([\d,.]+)\s*(?:acres?|ac)\b', text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", ""))
    # "40.5 hectares" or "40.5ha"
    m = re.search(r'([\d,.]+)\s*(?:hectares?|ha)\b', text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "")) * 2.47105
    # "40,000 sqm" or "40000m2"
    m = re.search(r'([\d,.]+)\s*(?:sqm|m2|m²)\b', text, re.IGNORECASE)
    if m:
        sqm = float(m.group(1).replace(",", ""))
        if sqm > 10000:
            return sqm / 4046.86
    return None


def fetch_email_alerts(criteria):
    """Fetch property listings from email alerts (REA, Listing Loop, Property Whispers).

    Connects to Gmail IMAP, reads recent alert emails, parses property data,
    and returns normalized listings. Gracefully returns empty if Gmail not configured.
    """
    mail = _connect_gmail_imap()
    if not mail:
        print("Email alerts: skipped (Gmail credentials not configured)")
        return []

    all_properties = []
    parsers = {
        "rea": _parse_rea_alert,
        "listing_loop": _parse_listing_loop_alert,
        "property_whispers": _parse_property_whispers_alert,
    }

    try:
        for source_key, senders in _ALERT_SENDERS.items():
            emails = _search_emails_from(mail, senders, days_back=8)
            if not emails:
                continue

            parser = parsers.get(source_key)
            if not parser:
                continue

            source_count = 0
            for email_data in emails:
                props = parser(email_data["html"], criteria)
                all_properties.extend(props)
                source_count += len(props)

            if source_count:
                print(f"Email alerts ({source_key}): {source_count} listings from {len(emails)} emails")

    except Exception as e:
        print(f"Email alert fetch error: {e}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    print(f"Email alerts total: {len(all_properties)} listings")
    return all_properties


# ── Source: Elders Real Estate (JSON API) ─────────────────────────────────
#
# Elders Regional NSW has an open JSON API that returns all rural sale
# listings. No auth required. We fetch all pages and filter by postcode.

ELDERS_API_URL = "https://regionalnsw.eldersrealestate.com.au/wp-admin/admin-ajax.php"


def fetch_elders(criteria):
    """Fetch rural sale listings from Elders Real Estate via their JSON API."""
    gates = criteria["gates"]
    target_postcodes = set(
        gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]
    )

    all_listings = []
    page = 1

    print("Elders: fetching via JSON API...")
    session = _retry_session(retries=2, backoff=1.0)

    while True:
        try:
            resp = session.get(
                ELDERS_API_URL,
                params={
                    "action": "search",
                    "listing_type": "rural",
                    "sale_type": "sale",
                    "page": str(page),
                },
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (property search tool)"},
            )
            if resp.status_code != 200:
                print(f"  Elders: HTTP {resp.status_code} on page {page}")
                break
        except requests.RequestException as e:
            print(f"  Elders: request failed on page {page}: {e}")
            break

        try:
            raw_data = resp.json()
        except (json.JSONDecodeError, ValueError):
            print(f"  Elders: invalid JSON on page {page}")
            break

        # API wraps results in {"data": [...], "last_page": N, ...}
        if isinstance(raw_data, dict):
            items = raw_data.get("data", [])
            last_page = raw_data.get("last_page", page)
        elif isinstance(raw_data, list):
            items = raw_data
            last_page = page
        else:
            break

        if not items:
            break

        for item in items:
            listing = item.get("listing", {})
            postcode = str(listing.get("postcode", "")).strip()

            if postcode not in target_postcodes:
                continue

            # Parse price
            price = None
            price_str = listing.get("price", "")
            if price_str:
                try:
                    price = int(float(price_str))
                except (ValueError, TypeError):
                    pass

            price_view = listing.get("price_view", "")

            # Parse land area (always in sqm from API)
            land_sqm = None
            land_ha = None
            land_acres = None
            land_str = str(item.get("land_area", "")).strip()
            if land_str:
                try:
                    land_sqm = float(land_str)
                    land_ha = round(land_sqm / 10000, 2)
                    land_acres = round(land_ha * 2.47105, 1)
                except (ValueError, TypeError):
                    pass

            # Build address
            street_num = (listing.get("street_number") or "").strip()
            street_name = (listing.get("street_name") or "").strip()
            suburb = (listing.get("suburb") or "").strip()
            state = (listing.get("state") or "NSW").strip()
            address = f"{street_num} {street_name}, {suburb} {state} {postcode}".strip()
            if address.startswith(","):
                address = address[1:].strip()

            unique_id = listing.get("unique_id", str(listing.get("id", "")))
            listing_url = f"https://regionalnsw.eldersrealestate.com.au/rural/sale/{unique_id}/"

            # Images
            photo_url = None
            image_count = listing.get("image_count", 0)
            if image_count and int(image_count) > 0:
                # Elders uses imgix CDN — construct URL from unique_id
                photo_url = f"https://elders-re-vre-cdn.imgix.net/{unique_id}/image-1.jpg?w=660&h=440&fit=crop"

            all_listings.append({
                "source": "elders",
                "source_id": f"elders-{unique_id}",
                "address": address,
                "suburb": suburb,
                "postcode": postcode,
                "state": state,
                "price": price,
                "display_price": price_view or f"${price:,}" if price else "Price on application",
                "land_sqm": land_sqm,
                "land_ha": land_ha,
                "land_acres": land_acres,
                "bedrooms": item.get("bedrooms"),
                "bathrooms": item.get("bathrooms"),
                "headline": listing.get("heading", ""),
                "description": "",  # Need detail page fetch for description
                "listing_url": listing_url,
                "photo_url": photo_url,
                "lat": _safe_float(listing.get("geo_latitude")),
                "lng": _safe_float(listing.get("geo_longitude")),
                "date_listed": listing.get("created_at"),
                "agent": listing.get("agent_name"),
                "raw": item,
            })

        if page >= last_page:
            break
        page += 1
        time.sleep(1.0)

    print(f"Elders: {len(all_listings)} listings in target postcodes")
    return all_listings


def _safe_float(val):
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── Source: Southern Tablelands Realty (yourstr.com.au) ────────────────────
#
# Small independent agency covering Tarago, Windellama, Tallong, Bungonia.
# Server-rendered HTML, Rex Software platform. ~11-15 listings total.

STR_BASE_URL = "https://yourstr.com.au"


def fetch_str(criteria):
    """Fetch listings from Southern Tablelands Realty (yourstr.com.au)."""
    gates = criteria["gates"]
    target_postcodes = set(
        gates["geography"]["postcodes_west"] + gates["geography"]["postcodes_south"]
    )

    url = f"{STR_BASE_URL}/listings?saleOrRental=Sale&status=available&sortby=dateListed-desc"

    all_listings = []
    page = 1

    print("Southern Tablelands Realty: fetching listings...")
    session = _retry_session(retries=2, backoff=1.0)

    while True:
        page_url = f"{url}&page={page}" if page > 1 else url

        try:
            resp = session.get(
                page_url,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (property search tool)"},
            )
            if resp.status_code != 200:
                print(f"  STR: HTTP {resp.status_code} on page {page}")
                break
        except requests.RequestException as e:
            print(f"  STR: request failed: {e}")
            break

        html = resp.text

        # Extract listing links — pattern: /listings/{type}_sale-R2-{id}-{suburb}
        listing_links = re.findall(
            r'href="(/listings/[^"]*_sale-R2-(\d+)-[^"]*)"', html
        )

        if not listing_links:
            break

        # Deduplicate links (each card has 2 links — image + title)
        seen_ids = set()
        unique_links = []
        for link, rex_id in listing_links:
            if rex_id not in seen_ids:
                seen_ids.add(rex_id)
                unique_links.append((link, rex_id))

        for link, rex_id in unique_links:
            listing_url = f"{STR_BASE_URL}{link}"
            prop = _parse_str_detail(listing_url, rex_id, target_postcodes)
            if prop:
                all_listings.append(prop)
            time.sleep(1.0)

        # Check for next page
        if f'page={page + 1}' not in html:
            break
        page += 1

    print(f"Southern Tablelands Realty: {len(all_listings)} listings in target postcodes")
    return all_listings


def _parse_str_detail(url, rex_id, target_postcodes):
    """Fetch and parse a single Southern Tablelands Realty listing page."""
    try:
        session = _retry_session(retries=2, backoff=0.5)
        resp = session.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (property search tool)"},
        )
        if resp.status_code != 200:
            return None
    except requests.RequestException:
        return None

    html = resp.text

    # Extract address from h1
    address_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    address = address_match.group(1).strip() if address_match else ""
    address = re.sub(r'<[^>]+>', '', address).strip()

    # Extract postcode from address
    pc_match = re.search(r'\b(\d{4})\s*$', address)
    postcode = pc_match.group(1) if pc_match else ""

    if postcode and postcode not in target_postcodes:
        return None

    # Extract suburb from address (pattern: "..., SUBURB STATE POSTCODE")
    suburb = ""
    suburb_match = re.search(r',\s*([A-Za-z\s]+?)\s+(?:NSW|ACT)\s+\d{4}', address)
    if suburb_match:
        suburb = suburb_match.group(1).strip()

    # Extract price
    price = None
    display_price = "Price on application"
    price_match = re.search(r'<strong[^>]*>\s*\$([0-9,]+)', html)
    if price_match:
        try:
            price = int(price_match.group(1).replace(",", ""))
            display_price = f"${price:,}"
        except ValueError:
            pass

    # Extract land area
    land_sqm = None
    land_ha = None
    land_acres = None
    land_match = re.search(r'title="Land Area".*?(\d+(?:\.\d+)?)\s*(ha|hect|m2|sqm|ac|acre)', html, re.DOTALL | re.IGNORECASE)
    if land_match:
        val = float(land_match.group(1))
        unit = land_match.group(2).lower()
        if unit in ("ha", "hect"):
            land_ha = val
            land_sqm = val * 10000
            land_acres = round(val * 2.47105, 1)
        elif unit in ("m2", "sqm"):
            land_sqm = val
            land_ha = round(val / 10000, 2)
            land_acres = round(land_ha * 2.47105, 1)
        elif unit in ("ac", "acre"):
            land_acres = val
            land_ha = round(val / 2.47105, 2)
            land_sqm = land_ha * 10000

    # Extract beds/baths
    beds_match = re.search(r'(\d+)\s*Bed', html)
    baths_match = re.search(r'(\d+)\s*Bath', html)
    bedrooms = int(beds_match.group(1)) if beds_match else None
    bathrooms = int(baths_match.group(1)) if baths_match else None

    # Extract description
    description = ""
    desc_blocks = re.findall(r'<p[^>]*class="[^"]*v2-text[^"]*"[^>]*>(.*?)</p>', html, re.DOTALL)
    if desc_blocks:
        description = " ".join(re.sub(r'<[^>]+>', '', b).strip() for b in desc_blocks[:5])[:1000]

    # Extract lat/lng from Leaflet marker
    lat = None
    lng = None
    marker_match = re.search(r'L\.marker\(\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]', html)
    if marker_match:
        lat = float(marker_match.group(1))
        lng = float(marker_match.group(2))

    # Extract first image
    photo_url = None
    img_match = re.search(r'(https://au-crm\.cdns\.rexsoftware\.com/[^"\']+\.jpg)', html)
    if img_match:
        photo_url = img_match.group(1)

    return {
        "source": "str",
        "source_id": f"str-{rex_id}",
        "address": address,
        "suburb": suburb,
        "postcode": postcode,
        "state": "NSW",
        "price": price,
        "display_price": display_price,
        "land_sqm": land_sqm,
        "land_ha": land_ha,
        "land_acres": land_acres,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "headline": "",
        "description": description,
        "listing_url": url,
        "photo_url": photo_url,
        "lat": lat,
        "lng": lng,
        "date_listed": None,
        "agent": "Southern Tablelands Realty",
        "raw": {},
    }


# ── Fetch all sources ──────────────────────────────────────────────────────

def fetch_all(criteria=None):
    """Fetch from all sources, normalize, and deduplicate.

    Each source is wrapped in try/except so one failure doesn't kill the pipeline.
    Returns (deduped_properties, source_report) — report is a dict of
    {source_name: {"count": N, "error": str|None}} for downstream health checks.
    """
    if criteria is None:
        criteria = load_criteria()

    all_properties = []
    source_report = {}

    sources = [
        ("Domain API", fetch_domain),
        ("Domain Web", fetch_domain_web),
        ("Farmbuy", fetch_farmbuy),
        ("Elders", fetch_elders),
        ("Southern Tablelands Realty", fetch_str),
        ("REA Web", fetch_rea_web),
        ("REA Manual", fetch_rea_manual),
        ("Email Alerts", fetch_email_alerts),
    ]

    for name, fetcher in sources:
        try:
            props = fetcher(criteria)
            all_properties.extend(props)
            source_report[name] = {"count": len(props), "error": None}
        except Exception as e:
            print(f"ERROR: {name} failed: {e}")
            source_report[name] = {"count": 0, "error": str(e)}

    print(f"\nTotal before dedup: {len(all_properties)}")
    for name, report in source_report.items():
        status = f"{report['count']}" if not report["error"] else f"FAILED ({report['error'][:80]})"
        print(f"  {name}: {status}")

    deduped = deduplicate(all_properties)
    print(f"After dedup: {len(deduped)}")

    return deduped, source_report


if __name__ == "__main__":
    criteria = load_criteria()
    props, report = fetch_all(criteria)
    print(f"\n{len(props)} unique properties across all sources")
    for p in props[:5]:
        print(f"  [{p['source']}] {p['suburb']} — {p['display_price']} — {p.get('land_acres', '?')} acres")
