#!/usr/bin/env python3
"""
Domain API property search — pull listings matching criteria.json gates,
score them, and output a ranked shortlist.

Usage:
    python3 search.py              # search and print results
    python3 search.py --email      # search, score, and send email digest
    python3 search.py --json       # output raw JSON for debugging
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
CRITERIA_PATH = BASE_DIR / "criteria.json"
RESULTS_DIR = BASE_DIR / "data" / "listings"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

DOMAIN_API_BASE = "https://api.domain.com.au"
DOMAIN_AUTH_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_CLIENT_ID = os.getenv("DOMAIN_CLIENT_ID")  # client_... value
DOMAIN_CLIENT_SECRET = os.getenv("DOMAIN_CLIENT_SECRET")  # secret_... value

_cached_token = None

# ── Load criteria ──────────────────────────────────────────────────────────

def load_criteria():
    with open(CRITERIA_PATH) as f:
        return json.load(f)


# ── Domain API auth ────────────────────────────────────────────────────────

def get_access_token():
    """Get OAuth2 bearer token via client_credentials grant."""
    global _cached_token
    if _cached_token:
        return _cached_token

    if not DOMAIN_CLIENT_ID or not DOMAIN_CLIENT_SECRET:
        print("ERROR: DOMAIN_CLIENT_ID and DOMAIN_CLIENT_SECRET must be set in .env")
        sys.exit(1)

    resp = requests.post(DOMAIN_AUTH_URL, data={
        "client_id": DOMAIN_CLIENT_ID,
        "client_secret": DOMAIN_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "api_listings_read",
    })

    if resp.status_code != 200:
        print(f"Auth error {resp.status_code}: {resp.text[:500]}")
        sys.exit(1)

    _cached_token = resp.json()["access_token"]
    print("Authenticated with Domain API")
    return _cached_token


def get_auth_headers():
    """Get headers with OAuth2 bearer token."""
    token = get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ── Drive time via OSRM ────────────────────────────────────────────────────

# South Bondi origin (George's reference point)
ORIGIN_LNG, ORIGIN_LAT = 151.2653, -33.8688
_drive_time_cache = {}


_osrm_session = None
_osrm_failures = 0


def _get_osrm_session():
    """Get or create a persistent session for OSRM requests."""
    global _osrm_session
    if _osrm_session is None:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        _osrm_session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503))
        _osrm_session.mount("https://", HTTPAdapter(max_retries=retry))
    return _osrm_session


def calc_drive_time(lat, lng):
    """Calculate drive time in minutes from origin using OSRM public API."""
    global _osrm_failures
    if not lat or not lng:
        return None

    # If OSRM has failed too many times this session, skip to avoid hanging
    if _osrm_failures >= 5:
        return None

    cache_key = f"{round(lat, 4)},{round(lng, 4)}"
    if cache_key in _drive_time_cache:
        return _drive_time_cache[cache_key]

    try:
        url = (
            f"https://router.project-osrm.org/route/v1/driving/"
            f"{ORIGIN_LNG},{ORIGIN_LAT};{lng},{lat}"
            f"?overview=false"
        )
        session = _get_osrm_session()
        resp = session.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                mins = round(data["routes"][0]["duration"] / 60)
                _drive_time_cache[cache_key] = mins
                _osrm_failures = 0  # reset on success
                return mins
        _osrm_failures += 1
    except requests.RequestException:
        _osrm_failures += 1
        if _osrm_failures >= 5:
            print("WARNING: OSRM unreachable after 5 attempts — skipping drive time calculations")

    return None


# ── Search Domain API ─────────────────────────────────────────────────────

def search_domain(criteria):
    """Search Domain API for listings matching our gate criteria."""
    gates = criteria["gates"]

    # Build postcode list from criteria
    postcodes = (
        gates["geography"]["postcodes_west"]
        + gates["geography"]["postcodes_south"]
    )

    # Domain API search payload
    payload = {
        "listingType": "Sale",
        "propertyTypes": ["AcreageSemiRural", "Farm", "Rural"],
        "minPrice": gates["budget"]["min_price"],
        "maxPrice": gates["budget"]["max_price"],
        "minLandArea": gates["land_size"]["min_hectares"] * 10000,  # API uses sqm
        "maxLandArea": gates["land_size"]["max_hectares"] * 10000,
        "locations": [
            {"postCode": pc, "state": "NSW"} for pc in postcodes
        ],
        "pageSize": 200,
        "sort": {"sortKey": "DateListed", "direction": "Descending"},
    }

    headers = get_auth_headers()
    url = f"{DOMAIN_API_BASE}/v1/listings/residential/_search"

    print(f"Searching Domain API across {len(postcodes)} postcodes...")
    resp = requests.post(url, json=payload, headers=headers)

    if resp.status_code != 200:
        print(f"API error {resp.status_code}: {resp.text[:500]}")
        return []

    results = resp.json()
    print(f"Domain returned {len(results)} listings")
    return results


# ── Gate filter ────────────────────────────────────────────────────────────

def passes_gates(listing, criteria):
    """Check if a listing passes all hard gate criteria."""
    gates = criteria["gates"]
    details = listing.get("listing", {})

    # Price gate
    price = details.get("priceDetails", {}).get("price")
    display_price = details.get("priceDetails", {}).get("displayPrice", "")
    if price is not None:
        if price > gates["budget"]["max_price"] or price < gates["budget"]["min_price"]:
            return False, "price_out_of_range"

    # Land size gate (Domain returns sqm)
    land_area = details.get("propertyDetails", {}).get("landArea")
    if land_area is not None:
        land_ha = land_area / 10000
        if land_ha < gates["land_size"]["min_hectares"] or land_ha > gates["land_size"]["max_hectares"]:
            return False, "land_size_out_of_range"

    return True, "pass"


# ── Scoring ────────────────────────────────────────────────────────────────

# Keywords to detect features from listing descriptions
WATER_KEYWORDS = [
    r'\briver\b', r'\bcreek\b', r'\bstream\b', r'\bdam\b', r'\bdams\b',
    r'\bbore\b', r'\bspring\b', r'\bwater\s*front', r'\bfrontage\b',
    r'\bwater\s*hole', r'\bwaterway\b', r'\bpermanent\s*water\b',
]
TERRAIN_KEYWORDS = [
    r'\bundulat', r'\bhilly\b', r'\bhill\b', r'\brolling\b', r'\belevat',
    r'\bviews?\b', r'\bpanoram', r'\boutlook\b', r'\bridgeline\b',
    r'\bescarpment\b', r'\bvalley\b', r'\bmountain\b',
]
SECLUSION_KEYWORDS = [
    r'\bseclud', r'\bprivat', r'\bisolat', r'\bremote\b', r'\bhidden\b',
    r'\bend\s*of\s*road', r'\bno\s*neighbou?rs?\b', r'\btranquil',
    r'\bpeaceful\b', r'\bretreat\b', r'\bgetaway\b', r'\bhideaway\b',
]
HOUSE_KEYWORDS = [
    r'\bbedroom', r'\bbath', r'\bkitchen\b', r'\brenovated\b',
    r'\bcottage\b', r'\bhomestead\b', r'\bfarmhouse\b', r'\bdwelling\b',
    r'\bresidence\b', r'\bhome\b',
]
PARK_KEYWORDS = [
    r'\bnational\s*park\b', r'\bstate\s*forest\b', r'\bnature\s*reserve\b',
    r'\bconservation\b', r'\bbacking\s*onto\s*bush\b',
]
CONVENIENCE_KEYWORDS = [
    r'\bmobile\s*(reception|coverage|service)\b', r'\binternet\b',
    r'\bclose\s*to\s*town\b', r'\bminutes?\s*(to|from)\s*town\b',
    r'\bschool\b', r'\bshop', r'\bcafe\b',
]


def keyword_score(text, patterns):
    """Count how many keyword patterns match in text. Returns 0-1 normalised."""
    if not text:
        return 0.0
    text_lower = text.lower()
    hits = sum(1 for p in patterns if re.search(p, text_lower))
    # Normalise: 1 hit = 0.5, 2+ hits = 0.8, 3+ = 1.0
    if hits == 0:
        return 0.0
    if hits == 1:
        return 0.5
    if hits == 2:
        return 0.8
    return 1.0


def score_listing(listing, criteria):
    """Score a listing against criteria.json weights. Returns dict with breakdown."""
    weights = criteria["scoring"]
    details = listing.get("listing", {})
    description = details.get("description", "") or ""
    headline = details.get("headline", "") or ""
    full_text = f"{headline} {description}"

    scores = {}

    # Water (weight 25) — keyword detection from description
    scores["water"] = keyword_score(full_text, WATER_KEYWORDS) * weights["water"]["weight"]

    # Terrain (weight 20)
    scores["terrain"] = keyword_score(full_text, TERRAIN_KEYWORDS) * weights["terrain"]["weight"]

    # Seclusion (weight 15)
    scores["seclusion"] = keyword_score(full_text, SECLUSION_KEYWORDS) * weights["seclusion"]["weight"]

    # House quality (weight 15) — bedrooms as proxy + keywords
    bedrooms = details.get("propertyDetails", {}).get("bedrooms")
    house_kw = keyword_score(full_text, HOUSE_KEYWORDS)
    if bedrooms and bedrooms >= 2:
        house_kw = max(house_kw, 0.6)
    scores["house_quality"] = house_kw * weights["house_quality"]["weight"]

    # Drive time bonus (weight 10) — OSRM routing
    lat = details.get("propertyDetails", {}).get("latitude")
    lng = details.get("propertyDetails", {}).get("longitude")
    drive_mins = calc_drive_time(lat, lng) if lat and lng else None
    if drive_mins is not None:
        if drive_mins <= 180:  # under 3 hours
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"]
        elif drive_mins <= 210:  # under 3.5 hours
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"] * 0.7
        elif drive_mins <= 240:  # under 4 hours (acceptable)
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"] * 0.3
        else:  # over 4 hours — gate fail but may still be in results
            scores["drive_time_bonus"] = 0
    else:
        scores["drive_time_bonus"] = 0

    # National park adjacent (weight 5)
    scores["national_park_adjacent"] = keyword_score(full_text, PARK_KEYWORDS) * weights["national_park_adjacent"]["weight"]

    # Carbon eligible (weight 5) — land size proxy
    land_area = details.get("propertyDetails", {}).get("landArea")
    if land_area and land_area >= 404700:  # 100+ acres in sqm
        scores["carbon_eligible"] = weights["carbon_eligible"]["weight"] * 0.5
    else:
        scores["carbon_eligible"] = 0

    # Convenience (weight 5)
    scores["convenience"] = keyword_score(full_text, CONVENIENCE_KEYWORDS) * weights["convenience"]["weight"]

    total = sum(scores.values())
    max_possible = sum(w["weight"] for w in weights.values())

    return {
        "total": round(total, 1),
        "max_possible": max_possible,
        "pct": round(100 * total / max_possible, 1) if max_possible > 0 else 0,
        "breakdown": {k: round(v, 1) for k, v in scores.items()},
    }


# ── Extract clean property record ──────────────────────────────────────────

def extract_property(listing, score_result, drive_mins=None):
    """Extract a clean property dict from a Domain API listing."""
    details = listing.get("listing", {})
    prop = details.get("propertyDetails", {})
    price_details = details.get("priceDetails", {})
    media = details.get("media", [])

    # Get first image
    photo_url = None
    for m in media:
        if m.get("category") == "Image" or m.get("type") == "photo":
            photo_url = m.get("url")
            break

    # Land area conversion
    land_sqm = prop.get("landArea")
    land_ha = round(land_sqm / 10000, 1) if land_sqm else None
    land_acres = round(land_ha * 2.471, 0) if land_ha else None

    # Address
    addr_parts = [
        prop.get("streetNumber", ""),
        prop.get("street", ""),
        prop.get("suburb", ""),
        prop.get("state", ""),
        prop.get("postcode", ""),
    ]
    address = " ".join(p for p in addr_parts if p).strip()

    return {
        "id": details.get("id"),
        "address": address,
        "suburb": prop.get("suburb", ""),
        "postcode": prop.get("postcode", ""),
        "state": prop.get("state", ""),
        "price": price_details.get("price"),
        "display_price": price_details.get("displayPrice", "Price on application"),
        "land_ha": land_ha,
        "land_acres": land_acres,
        "bedrooms": prop.get("bedrooms"),
        "bathrooms": prop.get("bathrooms"),
        "headline": details.get("headline", ""),
        "description": (details.get("description", "") or "")[:500],
        "listing_url": f"https://www.domain.com.au/{details.get('listingSlug', '')}",
        "photo_url": photo_url,
        "date_listed": details.get("dateListed"),
        "date_updated": details.get("dateUpdated"),
        "property_type": prop.get("propertyType", ""),
        "score": score_result,
        "lat": prop.get("latitude"),
        "lng": prop.get("longitude"),
        "drive_time_minutes": drive_mins,
    }


# ── Tag extraction ─────────────────────────────────────────────────────────

def extract_tags(listing, criteria):
    """Extract observable tags from listing description."""
    details = listing.get("listing", {})
    text = f"{details.get('headline', '')} {details.get('description', '')}".lower()
    available_tags = criteria.get("tags", {}).get("available", [])

    # Map tag names to detection patterns
    tag_patterns = {
        "river_frontage": [r'\briver\b', r'\briver\s*front'],
        "creek_frontage": [r'\bcreek\b', r'\bcreek\s*front'],
        "dam": [r'\bdam\b', r'\bdams\b'],
        "bore": [r'\bbore\b'],
        "hilltop_position": [r'\bhilltop\b', r'\bridgetop\b', r'\belevated\s*position'],
        "valley_floor": [r'\bvalley\s*floor\b', r'\bvalley\b'],
        "north_facing": [r'\bnorth\s*facing\b', r'\bnorthern\s*aspect\b'],
        "views": [r'\bviews?\b', r'\bpanoram', r'\boutlook\b'],
        "end_of_road": [r'\bend\s*of\s*(the\s*)?road\b', r'\bno\s*through\s*road\b'],
        "sealed_road_access": [r'\bsealed\s*road\b', r'\bbitumen\b', r'\btarred\b'],
        "backs_onto_bush": [r'\bbacks?\s*(onto|on\s*to)\s*bush\b', r'\bbush\s*backdrop\b'],
        "national_park_adjacent": [r'\bnational\s*park\b'],
        "state_forest_adjacent": [r'\bstate\s*forest\b'],
        "existing_house": [r'\bbedroom', r'\bhomestead\b', r'\bcottage\b', r'\bfarmhouse\b'],
        "existing_shed": [r'\bshed\b', r'\bbarn\b', r'\bmachinery\s*shed\b'],
        "fruit_trees": [r'\bfruit\s*tree', r'\borchard\b'],
        "established_gardens": [r'\bgarden', r'\bmature\s*tree'],
        "old_growth_trees": [r'\bold\s*growth\b', r'\bcentury\s*old\b'],
        "cleared_pastoral": [r'\bpastur', r'\bgrazing\b', r'\bcleared\b'],
        "mixed_bush_and_cleared": [r'\bmix\w*\s*(bush|timber|native)'],
        "mobile_reception": [r'\bmobile\s*(reception|coverage|service)\b'],
        "near_town": [r'\bclose\s*to\s*town\b', r'\bminutes?\s*(to|from)\s*\w+\s*town\b'],
    }

    detected = []
    for tag in available_tags:
        patterns = tag_patterns.get(tag, [])
        for p in patterns:
            if re.search(p, text):
                detected.append(tag)
                break

    return detected


# ── Gate filter for normalized properties ──────────────────────────────────

def passes_gates_normalized(prop, criteria):
    """Check if a normalized property (from any source) passes gate criteria."""
    gates = criteria["gates"]

    # Price gate
    price = prop.get("price")
    if price is not None:
        if price > gates["budget"]["max_price"] or price < gates["budget"]["min_price"]:
            return False, "price_out_of_range"

    # Land size gate
    land_ha = prop.get("land_ha")
    if land_ha is not None:
        if land_ha < gates["land_size"]["min_hectares"] or land_ha > gates["land_size"]["max_hectares"]:
            return False, "land_size_out_of_range"

    # Drive time gate (if we have coordinates)
    lat, lng = prop.get("lat"), prop.get("lng")
    if lat and lng:
        drive_mins = calc_drive_time(lat, lng)
        if drive_mins and drive_mins > gates["drive_time"]["max_minutes"]:
            return False, "drive_time_exceeded"
        prop["_drive_mins"] = drive_mins  # stash for scoring

    return True, "pass"


def score_normalized(prop, criteria):
    """Score a normalized property from any source."""
    weights = criteria["scoring"]
    full_text = f"{prop.get('headline', '')} {prop.get('description', '')}"

    scores = {}

    # Water (weight 25)
    scores["water"] = keyword_score(full_text, WATER_KEYWORDS) * weights["water"]["weight"]

    # Terrain (weight 20)
    scores["terrain"] = keyword_score(full_text, TERRAIN_KEYWORDS) * weights["terrain"]["weight"]

    # Seclusion (weight 15)
    scores["seclusion"] = keyword_score(full_text, SECLUSION_KEYWORDS) * weights["seclusion"]["weight"]

    # House quality (weight 15)
    house_kw = keyword_score(full_text, HOUSE_KEYWORDS)
    try:
        beds = int(prop.get("bedrooms") or 0)
    except (ValueError, TypeError):
        beds = 0
    if beds >= 2:
        house_kw = max(house_kw, 0.6)
    scores["house_quality"] = house_kw * weights["house_quality"]["weight"]

    # Drive time bonus (weight 10)
    drive_mins = prop.get("_drive_mins") or (
        calc_drive_time(prop["lat"], prop["lng"]) if prop.get("lat") and prop.get("lng") else None
    )
    prop["drive_time_minutes"] = drive_mins
    if drive_mins is not None:
        if drive_mins <= 180:
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"]
        elif drive_mins <= 210:
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"] * 0.7
        elif drive_mins <= 240:
            scores["drive_time_bonus"] = weights["drive_time_bonus"]["weight"] * 0.3
        else:
            scores["drive_time_bonus"] = 0
    else:
        scores["drive_time_bonus"] = 0

    # National park adjacent (weight 5)
    scores["national_park_adjacent"] = keyword_score(full_text, PARK_KEYWORDS) * weights["national_park_adjacent"]["weight"]

    # Carbon eligible (weight 5)
    land_sqm = prop.get("land_sqm")
    if land_sqm and land_sqm >= 404700:
        scores["carbon_eligible"] = weights["carbon_eligible"]["weight"] * 0.5
    else:
        scores["carbon_eligible"] = 0

    # Convenience (weight 5)
    scores["convenience"] = keyword_score(full_text, CONVENIENCE_KEYWORDS) * weights["convenience"]["weight"]

    total = sum(scores.values())
    max_possible = sum(w["weight"] for k, w in weights.items() if isinstance(w, dict) and "weight" in w)

    return {
        "total": round(total, 1),
        "max_possible": max_possible,
        "pct": round(100 * total / max_possible, 1) if max_possible > 0 else 0,
        "breakdown": {k: round(v, 1) for k, v in scores.items()},
    }


def extract_tags_normalized(prop, criteria):
    """Extract tags from a normalized property."""
    text = f"{prop.get('headline', '')} {prop.get('description', '')}".lower()
    available_tags = criteria.get("tags", {}).get("available", [])

    tag_patterns = {
        "river_frontage": [r'\briver\b', r'\briver\s*front'],
        "creek_frontage": [r'\bcreek\b', r'\bcreek\s*front'],
        "dam": [r'\bdam\b', r'\bdams\b'],
        "bore": [r'\bbore\b'],
        "hilltop_position": [r'\bhilltop\b', r'\bridgetop\b', r'\belevated\s*position'],
        "valley_floor": [r'\bvalley\s*floor\b', r'\bvalley\b'],
        "north_facing": [r'\bnorth\s*facing\b', r'\bnorthern\s*aspect\b'],
        "views": [r'\bviews?\b', r'\bpanoram', r'\boutlook\b'],
        "end_of_road": [r'\bend\s*of\s*(the\s*)?road\b', r'\bno\s*through\s*road\b'],
        "sealed_road_access": [r'\bsealed\s*road\b', r'\bbitumen\b', r'\btarred\b'],
        "backs_onto_bush": [r'\bbacks?\s*(onto|on\s*to)\s*bush\b', r'\bbush\s*backdrop\b'],
        "national_park_adjacent": [r'\bnational\s*park\b'],
        "state_forest_adjacent": [r'\bstate\s*forest\b'],
        "existing_house": [r'\bbedroom', r'\bhomestead\b', r'\bcottage\b', r'\bfarmhouse\b'],
        "existing_shed": [r'\bshed\b', r'\bbarn\b', r'\bmachinery\s*shed\b'],
        "fruit_trees": [r'\bfruit\s*tree', r'\borchard\b'],
        "established_gardens": [r'\bgarden', r'\bmature\s*tree'],
        "old_growth_trees": [r'\bold\s*growth\b', r'\bcentury\s*old\b'],
        "cleared_pastoral": [r'\bpastur', r'\bgrazing\b', r'\bcleared\b'],
        "mixed_bush_and_cleared": [r'\bmix\w*\s*(bush|timber|native)'],
        "mobile_reception": [r'\bmobile\s*(reception|coverage|service)\b'],
        "near_town": [r'\bclose\s*to\s*town\b', r'\bminutes?\s*(to|from)\s*\w+\s*town\b'],
    }

    detected = []
    for tag in available_tags:
        patterns = tag_patterns.get(tag, [])
        for p in patterns:
            if re.search(p, text):
                detected.append(tag)
                break

    return detected


# ── Main ───────────────────────────────────────────────────────────────────

def run_search(domain_only=False):
    """Run the full search pipeline: fetch all sources → gate → score → output."""
    criteria = load_criteria()

    source_report = {}

    if domain_only:
        # Legacy mode — Domain API only (uses Domain-specific listing format)
        raw_listings = search_domain(criteria)
        if not raw_listings:
            print("No listings returned. Check API key and criteria.")
            return [], {}

        properties = []
        rejected = {"price_out_of_range": 0, "land_size_out_of_range": 0}

        for listing in raw_listings:
            passed, reason = passes_gates(listing, criteria)
            if not passed:
                rejected[reason] = rejected.get(reason, 0) + 1
                continue
            score_result = score_listing(listing, criteria)
            tags = extract_tags(listing, criteria)
            details = listing.get("listing", {})
            plat = details.get("propertyDetails", {}).get("latitude")
            plng = details.get("propertyDetails", {}).get("longitude")
            drive_mins = calc_drive_time(plat, plng) if plat and plng else None
            prop = extract_property(listing, score_result, drive_mins)
            prop["tags"] = tags
            properties.append(prop)
    else:
        # Multi-source mode — fetch from all sources
        from sources import fetch_all, enrich_with_descriptions
        all_normalized, source_report = fetch_all(criteria)

        if not all_normalized:
            print("No listings from any source.")
            return [], source_report

        properties = []
        rejected = {}
        gate_passed = []

        for prop in all_normalized:
            passed, reason = passes_gates_normalized(prop, criteria)
            if not passed:
                rejected[reason] = rejected.get(reason, 0) + 1
                continue
            gate_passed.append(prop)

        # Fetch descriptions for gate-passed listings before scoring
        enrich_with_descriptions(gate_passed)

        for prop in gate_passed:
            score_result = score_normalized(prop, criteria)
            tags = extract_tags_normalized(prop, criteria)

            # Build final property record
            prop["score"] = score_result
            prop["tags"] = tags
            if "drive_time_minutes" not in prop:
                prop["drive_time_minutes"] = prop.get("_drive_mins")
            # Clean up internal fields
            prop.pop("_drive_mins", None)
            prop.pop("raw", None)
            properties.append(prop)

    # Sort by score descending
    properties.sort(key=lambda p: p["score"]["total"], reverse=True)

    # Summary
    print(f"\nResults: {len(properties)} passed gates, {sum(rejected.values())} rejected")
    for reason, count in rejected.items():
        if count > 0:
            print(f"  - {reason}: {count}")

    print(f"\nTop properties:")
    print(f"{'Score':>6} {'Price':>12} {'Acres':>7} {'Drive':>6} {'Src':<8} {'Suburb':<20} {'Headline'}")
    print("-" * 105)
    for p in properties[:20]:
        price_str = f"${p['price']:,.0f}" if p.get('price') else (p.get('display_price') or '?')[:12]
        acres_str = f"{p['land_acres']:.0f}" if p.get('land_acres') else "?"
        drive_str = f"{p['drive_time_minutes']}m" if p.get('drive_time_minutes') else "?"
        src = p.get("source", "domain")[:7]
        headline = (p.get('headline') or "")[:25]
        print(f"{p['score']['pct']:>5.0f}% {price_str:>12} {acres_str:>5}ac {drive_str:>5} {src:<8} {p.get('suburb', ''):<20} {headline}")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = RESULTS_DIR / f"search_{timestamp}.json"
    with open(outfile, "w") as f:
        json.dump({
            "search_date": datetime.now().isoformat(),
            "criteria_version": criteria.get("version"),
            "passed_gates": len(properties),
            "rejected": rejected,
            "source_report": source_report if not domain_only else {},
            "properties": properties,
        }, f, indent=2, default=str)
    print(f"\nSaved to {outfile}")

    return properties, source_report if not domain_only else {}


# ── Resilience: sanity gate + cache fallback ─────────────────────────────

def _load_previous_results():
    """Load the most recent previous search results for comparison."""
    results_files = sorted(RESULTS_DIR.glob("search_*.json"), reverse=True)
    if len(results_files) < 2:
        # No previous run to compare against — first run or only one file
        return None
    # Skip the first one (that's the one we just saved), take the second
    # But if called before saving, take the first
    for f in results_files:
        try:
            with open(f) as fh:
                return json.load(fh)
        except (json.JSONDecodeError, IOError):
            continue
    return None


def sanity_check(properties, source_report, previous=None):
    """Compare this run against previous results. Returns (ok, warnings).

    Checks:
    - Listing count didn't drop by >50%
    - Average score didn't drop by >15 points (suggests descriptions failed)
    - Domain Web (primary source) didn't return 0 when it previously returned >0
    """
    warnings = []

    if previous is None:
        previous = _load_previous_results()

    if previous is None:
        # No baseline — first run, everything is fine
        return True, []

    prev_count = previous.get("passed_gates", 0)
    curr_count = len(properties)

    # Count drop check
    if prev_count > 0 and curr_count < prev_count * 0.5:
        warnings.append(
            f"Listing count dropped sharply: {curr_count} vs {prev_count} last run. "
            f"A source may be broken."
        )

    # Score drop check — compare average of top 20
    prev_props = previous.get("properties", [])
    if prev_props and properties:
        prev_avg = sum(p["score"]["pct"] for p in prev_props[:20]) / min(len(prev_props), 20)
        curr_avg = sum(p["score"]["pct"] for p in properties[:20]) / min(len(properties), 20)
        if curr_avg < prev_avg - 15:
            warnings.append(
                f"Average score dropped: {curr_avg:.0f}% vs {prev_avg:.0f}% last run. "
                f"Description enrichment may have failed."
            )

    # Domain Web source check (it's our primary source)
    prev_source = previous.get("source_report", {})
    prev_domain_web = prev_source.get("Domain Web", {}).get("count", 0)
    curr_domain_web = source_report.get("Domain Web", {}).get("count", 0)
    if prev_domain_web > 50 and curr_domain_web == 0:
        warnings.append(
            f"Domain Web returned 0 listings (was {prev_domain_web}). "
            f"Domain may have changed their page structure."
        )

    # Any source errors?
    for name, report in source_report.items():
        if report.get("error"):
            warnings.append(f"{name} failed: {report['error'][:100]}")

    ok = len([w for w in warnings if "dropped sharply" in w or "returned 0" in w]) == 0
    return ok, warnings


def load_cached_results():
    """Load last good search results as fallback."""
    results_files = sorted(RESULTS_DIR.glob("search_*.json"), reverse=True)
    for f in results_files:
        try:
            with open(f) as fh:
                data = json.load(fh)
            props = data.get("properties", [])
            if len(props) > 10:  # only use if it had a decent number
                search_date = data.get("search_date", "unknown")
                print(f"Cache fallback: loaded {len(props)} properties from {f.name}")
                return props, search_date
        except (json.JSONDecodeError, IOError):
            continue
    return [], None


if __name__ == "__main__":
    domain_only = "--domain-only" in sys.argv
    send_email = "--email" in sys.argv
    dry_run = "--dry-run" in sys.argv
    force = "--force" in sys.argv

    if "--json" in sys.argv:
        props, _ = run_search(domain_only=domain_only)
        if props:
            print(json.dumps(props, indent=2, default=str))
    else:
        props, source_report = run_search(domain_only=domain_only)

        if send_email:
            from email_sender import send_digest
            from shortlist import generate_shortlist

            if not props:
                # Pipeline returned nothing — try cache fallback
                print("\nWARNING: Pipeline returned 0 properties.")
                cached, cached_date = load_cached_results()
                if cached and not force:
                    print(f"Sending last good results from {cached_date} with warning.")
                    generate_shortlist(cached, search_date=f"Cached — {cached_date}")
                    send_digest(
                        cached,
                        search_date=f"(cached from {cached_date}) — pipeline error, Karl is investigating",
                        dry_run=dry_run,
                    )
                else:
                    print("No cached results available. No email sent.")
                sys.exit(1)

            # Sanity check before sending
            ok, warnings = sanity_check(props, source_report)
            if warnings:
                print(f"\n{'='*60}")
                print("SANITY CHECK WARNINGS:")
                for w in warnings:
                    print(f"  ⚠ {w}")
                print(f"{'='*60}")

            if not ok and not force:
                print("\nSanity check FAILED — email NOT sent.")
                print("Review warnings above. Use --force to send anyway.")
                sys.exit(1)

            if warnings and ok:
                print("\nMinor warnings detected but sending anyway.")

            # Generate shortlist page (docs/index.html for GitHub Pages)
            generate_shortlist(props)

            # Send short email with link to shortlist
            result = send_digest(props, dry_run=dry_run)
            if not result:
                sys.exit(1)
