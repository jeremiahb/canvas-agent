"""
Canvas Cookie Exporter — Windows Chrome
Run this on your LOCAL machine AFTER logging into Canvas.

Usage:
    python export_cookies.py

Output:
    canvas_cookies.json  (upload this to your Canvas Agent dashboard)

Requirements — install once before running:
    pip install browser-cookie3
"""

import json
import os
import sys
from datetime import datetime


def export_canvas_cookies(canvas_url: str = "wilmu.instructure.com") -> None:
    # Explicit import check with a clear install instruction — never auto-install
    # via os.system() because that silently runs arbitrary shell commands and
    # can mask permission errors or install into the wrong environment.
    try:
        import browser_cookie3
    except ImportError:
        print(
            "❌ browser-cookie3 is not installed.\n"
            "   Run this first, then try again:\n\n"
            f"       {sys.executable} -m pip install browser-cookie3\n"
        )
        sys.exit(1)

    print(f"\n🔐 Exporting Canvas cookies for {canvas_url}...")
    print("   ⚠️  Close Chrome completely before running — including the system tray icon.\n")

    try:
        raw_cookies = list(browser_cookie3.chrome(domain_name=canvas_url))
    except PermissionError:
        print(
            "❌ Permission denied — Chrome must be fully closed.\n"
            "   Right-click the Chrome icon in the system tray → Exit, then try again."
        )
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error reading Chrome cookies: {e}")
        print("\nManual fallback:")
        print("  1. Open Chrome → press F12 → Application tab → Cookies → wilmu.instructure.com")
        print("  2. Export using the 'Cookie-Editor' Chrome extension (JSON format)")
        sys.exit(1)

    if not raw_cookies:
        print(
            "❌ No cookies found for this domain.\n"
            "   Make sure you are logged into Canvas in Chrome, then close Chrome and try again."
        )
        sys.exit(1)

    cookie_list = [
        {
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path,
            "secure": bool(c.secure),
            "expires": c.expires,
        }
        for c in raw_cookies
    ]

    output = {
        "canvas_url": canvas_url,
        "exported_at": datetime.now().isoformat(),
        "cookie_count": len(cookie_list),
        "cookies": cookie_list,
    }

    output_path = "canvas_cookies.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    abs_path = os.path.abspath(output_path)
    print(f"✅ Exported {len(cookie_list)} cookies successfully!")
    print(f"📁 Saved to: {abs_path}")
    print("\n📤 Next step: Upload canvas_cookies.json to your Canvas Agent dashboard.")


if __name__ == "__main__":
    export_canvas_cookies()
