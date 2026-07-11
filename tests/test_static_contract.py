import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML_FILES = [
    "index.html", "ADMINPRO.html", "wholesale.html", "invoice.html",
    "settings.html", "scanner.html", "B2Binvoice.html",
]


class StaticContractTests(unittest.TestCase):
    def test_no_google_database_or_public_cdn_runtime(self):
        forbidden = (
            "script.google.com", "docs.google.com/spreadsheets", "cdn.tailwindcss.com",
            "cdn.jsdelivr.net", "unpkg.com/react", "fonts.googleapis.com",
        )
        for filename in HTML_FILES:
            text = (ROOT / filename).read_text(encoding="utf-8")
            for value in forbidden:
                self.assertNotIn(value, text, f"{value} remains in {filename}")
            if filename != "scanner.html":
                self.assertIn("/assets/local-api.js", text)
            else:
                self.assertIn("/api/v1/scanner/", text)
            self.assertIn("/assets/app.css", text)

    def test_database_and_source_are_not_in_static_allowlist(self):
        main = (ROOT / "main.py").read_text(encoding="utf-8")
        block = main.split("STATIC_FILES =", 1)[1].split("for route_path", 1)[0]
        for private_name in ("erp.db", "main.py", ".env", "_backups"):
            self.assertNotIn(private_name, block)


if __name__ == "__main__":
    unittest.main()
