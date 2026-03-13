#!/usr/bin/env python3
"""
Resend email sender — sends George a short email with a link to the
hosted shortlist page on GitHub Pages.

Setup:
    1. Sign up at resend.com and grab your API key
    2. Add to .env:
         RESEND_API_KEY=re_xxxxxxxxxxxxx
         GEORGE_EMAIL=george@example.com
         SHORTLIST_URL=https://your-username.github.io/bolt-hole/

Usage:
    from email_sender import send_digest
    send_digest(properties, search_date="10 March 2026")
"""

import os
from datetime import datetime

import resend
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
GEORGE_EMAIL = os.getenv("GEORGE_EMAIL")
SHORTLIST_URL = os.getenv("SHORTLIST_URL", "")

# Resend's test address — works without a custom domain
FROM_ADDRESS = os.getenv("FROM_ADDRESS", "Bolt Hole <onboarding@resend.dev>")


def _build_link_email(properties, search_date, shortlist_url):
    """Build a short, punchy email pointing George to the shortlist page."""
    count = len(properties)
    top = properties[0] if properties else None

    top_line = ""
    if top:
        top_suburb = top.get("suburb", "")
        top_pct = top["score"]["pct"]
        top_price = f"${top['price']:,.0f}" if top.get("price") else top.get("display_price", "")
        top_line = f"Top match: {top_suburb} — {top_price} ({top_pct:.0f}% fit)"

    # Top 3 preview
    preview_lines = []
    for p in properties[:3]:
        price = f"${p['price']:,.0f}" if p.get("price") else p.get("display_price", "?")
        suburb = p.get("suburb", "")
        acres = f"{p['land_acres']:.0f}ac" if p.get("land_acres") else ""
        pct = p["score"]["pct"]
        preview_lines.append(f"{suburb} — {price} — {acres} — {pct:.0f}%")

    preview_html = "".join(
        f'<div style="font-size:14px;color:#334155;padding:4px 0;">{line}</div>'
        for line in preview_lines
    )

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f0e8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:520px;margin:0 auto;padding:32px 20px;">

    <div style="text-align:center;margin-bottom:24px;">
        <div style="font-size:11px;font-weight:600;letter-spacing:2.5px;color:#94a3b8;text-transform:uppercase;">Reforged</div>
        <div style="font-size:22px;font-weight:700;color:#1e293b;margin-top:4px;">Bolt Hole — Weekly Shortlist</div>
        <div style="font-size:14px;color:#64748b;margin-top:4px;">{search_date}</div>
    </div>

    <div style="background:#fff;border-radius:10px;padding:20px 24px;margin-bottom:20px;">
        <div style="font-size:16px;font-weight:600;color:#1e293b;margin-bottom:4px;">{count} properties this week</div>
        <div style="font-size:13px;color:#64748b;margin-bottom:16px;">{top_line}</div>

        <div style="border-top:1px solid #e2e8f0;padding-top:12px;margin-bottom:16px;">
            {preview_html}
        </div>

        <div style="text-align:center;">
            <a href="{shortlist_url}" style="display:inline-block;background:#1e293b;color:#ffffff;padding:12px 32px;border-radius:8px;text-decoration:none;font-size:15px;font-weight:600;">
                View Full Shortlist
            </a>
        </div>
    </div>

    <div style="text-align:center;font-size:12px;color:#94a3b8;">
        <div>Tap Love it, Interesting, or Not for me on each property — your feedback sharpens next week's results.</div>
        <div style="margin-top:8px;">Prepared by Karl Howard &middot; Reforged</div>
    </div>

</div>
</body>
</html>"""

    plain = f"Bolt Hole — Weekly Shortlist — {search_date}\n\n"
    plain += f"{count} properties this week.\n"
    if top_line:
        plain += f"{top_line}\n"
    plain += f"\nView your shortlist: {shortlist_url}\n\n"
    for i, line in enumerate(preview_lines):
        plain += f"  {i+1}. {line}\n"
    plain += "\nTap Love it, Interesting, or Not for me — your feedback sharpens next week's results.\n"

    return plain, html


def send_digest(properties, search_date=None, recipient=None, dry_run=False):
    """Send the weekly shortlist link email via Resend.

    Args:
        properties: scored property list (already sorted by score desc)
        search_date: display date string, defaults to today
        recipient: override GEORGE_EMAIL for testing
        dry_run: if True, render HTML but don't send (saves to email_preview.html)

    Returns:
        True if sent (or dry_run succeeded), False on error.
    """
    if search_date is None:
        search_date = datetime.now().strftime("%d %B %Y")

    to_addr = recipient or GEORGE_EMAIL
    shortlist_url = SHORTLIST_URL or "(shortlist URL not configured — set SHORTLIST_URL in .env)"

    plain, html = _build_link_email(properties, search_date, shortlist_url)

    if dry_run:
        from pathlib import Path
        preview = Path(__file__).parent / "email_preview.html"
        with open(preview, "w") as f:
            f.write(html)
        print(f"Dry run: saved preview to {preview}")
        return True

    # Validate credentials
    if not RESEND_API_KEY:
        print("ERROR: RESEND_API_KEY must be set in .env")
        print("Sign up at resend.com and grab your API key.")
        return False

    if not to_addr:
        print("ERROR: No recipient. Set GEORGE_EMAIL in .env or pass recipient=")
        return False

    # Send via Resend
    count = len(properties)
    subject = f"Bolt Hole Weekly — {search_date} — {count} properties"

    try:
        resend.api_key = RESEND_API_KEY
        params = {
            "from": FROM_ADDRESS,
            "to": [to_addr],
            "subject": subject,
            "html": html,
            "text": plain,
        }
        email = resend.Emails.send(params)
        email_id = email.get("id", "ok") if isinstance(email, dict) else str(email)
        print(f"Email sent to {to_addr} ({count} properties) — id: {email_id}")
        return True
    except Exception as e:
        print(f"ERROR: Resend send failed to {to_addr}: {e}")
        print(f"  Subject: {subject}")
        print(f"  Properties: {count}")
        print(f"  FROM_ADDRESS: {FROM_ADDRESS}")
        print(f"  Check: Is RESEND_API_KEY valid? Is FROM_ADDRESS verified in Resend?")
        return False


if __name__ == "__main__":
    import sys
    # Quick test: python3 email_sender.py --dry-run
    # Real send: python3 email_sender.py --to karl@example.com
    dry = "--dry-run" in sys.argv

    to = None
    for arg in sys.argv:
        if arg.startswith("--to="):
            to = arg.split("=", 1)[1]
        elif arg == "--to" and sys.argv.index(arg) + 1 < len(sys.argv):
            to = sys.argv[sys.argv.index(arg) + 1]

    # Load most recent results
    import json
    from pathlib import Path
    results_dir = Path(__file__).parent / "data" / "listings"
    results_files = sorted(results_dir.glob("search_*.json"), reverse=True)

    if results_files:
        with open(results_files[0]) as f:
            data = json.load(f)
        properties = data.get("properties", [])
        print(f"Loaded {len(properties)} properties from {results_files[0].name}")
    else:
        print("No search results found. Run search.py first.")
        sys.exit(1)

    send_digest(properties, recipient=to, dry_run=dry)
