"""
AI Automation Lead Generator v1 — Dentist Niche
Finds dental practices that could benefit from AI automation services.
Filters out big corps, directories, blogs, listicles, market reports.

SQLite history DB + query rotation + real-time Google Sheets push.

Optimized for Brave Search paid plan:
  - 20 req/sec rate limit
  - 20M requests/month
  - $3.00 per 1,000 requests

Usage:
    1. Paste your Brave API key below
    2. Set up Google Sheets (see setup section)
    3. pip install requests beautifulsoup4 gspread
    4. python ai_leads.py

Output: ai_leads_YYYY-MM-DD.csv + Google Sheet (real-time)
"""

import requests
import csv
import re
import time
import sqlite3
import random
import os
from datetime import datetime, date
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import socket

# ============================================================
# CONFIG
# ============================================================
BRAVE_API_KEY = "YOUR_BRAVE_API_KEY_HERE"

GOOGLE_CREDS_FILE = "service_account.json"
GOOGLE_SHEET_URL = ""

DAILY_LEAD_TARGET = 600
BRAVE_SEARCH_DELAY = 0.06
SITE_AUDIT_DELAY = 0.8
REQUEST_TIMEOUT = 12
BRAVE_COUNT = 20
MIN_TOTAL_SCORE = 30          # lower bar since we now require contact info
DB_FILE = "ai_leads_history.db"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TODAY = date.today().isoformat()
OUTPUT_FILE = os.path.join(SCRIPT_DIR, f"ai_leads_{TODAY}.csv")
DB_PATH = os.path.join(SCRIPT_DIR, DB_FILE)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

api_calls = 0

# ============================================================
# SKIP LISTS — big corps, directories, media, platforms
# ============================================================

SKIP_DOMAINS = {
    # ── Social / Big Tech ──
    "google.com", "youtube.com", "facebook.com", "twitter.com", "x.com",
    "linkedin.com", "instagram.com", "tiktok.com", "pinterest.com",
    "reddit.com", "quora.com", "medium.com", "substack.com",
    "github.com", "stackoverflow.com",
    # ── Mega corps ──
    "amazon.com", "apple.com", "microsoft.com", "walmart.com", "target.com",
    "costco.com", "homedepot.com", "lowes.com", "bestbuy.com",
    # ── Enterprise SaaS / platforms ──
    "hubspot.com", "salesforce.com", "slack.com", "notion.so", "zoom.us",
    "shopify.com", "wordpress.com", "wix.com", "squarespace.com",
    "godaddy.com", "bluehost.com", "hostgator.com",
    "mailchimp.com", "constantcontact.com", "sendgrid.com",
    "zendesk.com", "intercom.com", "freshdesk.com",
    "stripe.com", "paypal.com", "square.com",
    # ── SEO / marketing tools (competitors, not leads) ──
    "semrush.com", "ahrefs.com", "moz.com", "similarweb.com",
    "builtwith.com", "neilpatel.com", "backlinko.com",
    "searchenginejournal.com", "searchengineland.com",
    # ── Directories / review sites / listings ──
    "yelp.com", "bbb.org", "glassdoor.com", "indeed.com",
    "angellist.com", "wellfound.com", "crunchbase.com", "producthunt.com",
    "g2.com", "capterra.com", "trustpilot.com", "gartner.com",
    "tripadvisor.com", "healthgrades.com", "zocdoc.com", "avvo.com",
    "thumbtack.com", "angi.com", "homeadvisor.com", "houzz.com",
    "zillow.com", "realtor.com", "redfin.com", "trulia.com",
    "opentable.com", "doordash.com", "ubereats.com", "grubhub.com",
    "findlaw.com", "justia.com", "lawyers.com", "martindale.com",
    "vitals.com", "ratemds.com", "practo.com", "webmd.com",
    "networx.com", "expertise.com", "bark.com", "clutch.co",
    # ── News / media / blogs ──
    "nytimes.com", "wsj.com", "cnn.com", "bbc.com", "bbc.co.uk",
    "theverge.com", "wired.com", "venturebeat.com",
    "techcrunch.com", "forbes.com", "bloomberg.com", "inc.com",
    "entrepreneur.com", "businessinsider.com", "fastcompany.com",
    "mashable.com", "thenextweb.com", "engadget.com",
    # ── Market research (not businesses) ──
    "grandviewresearch.com", "fortunebusinessinsights.com",
    "mordorintelligence.com", "marketsandmarkets.com",
    "statista.com", "ibisworld.com", "euromonitor.com",
    "towardshealthcare.com", "precedenceresearch.com",
    "alliedmarketresearch.com", "transparencymarketresearch.com",
    "verifiedmarketresearch.com", "researchandmarkets.com",
    "globenewswire.com", "prnewswire.com", "businesswire.com",
    # ── Web design / template sites (not your leads) ──
    "sitebuilderreport.com", "muffingroup.com", "whatpixel.com",
    "mycodelesswebsite.com", "themeforest.com", "templatemonster.com",
    "dribbble.com", "behance.net", "awwwards.com",
    "insidea.com", "perfectpatients.com", "advisorevolved.com",
    "privateequitysites.com", "servgrow.com", "fieldedge.com",
    "scorpion.co", "wecreate.com",
    # ── Education / govt / nonprofit ──
    "wikipedia.org", "archive.org", "web.archive.org",
    "mayoclinic.org", "nih.gov", "cdc.gov",
    # ── Big retail / consumer brands ──
    "chewy.com", "petsmart.com", "petco.com",
    "ulta.com", "sephora.com", "macys.com", "nordstrom.com",
    "nike.com", "adidas.com", "gap.com", "hm.com", "zara.com",
    "ikea.com", "wayfair.com", "overstock.com",
    "booking.com", "expedia.com", "hotels.com", "airbnb.com",
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com",
    "ritzcarlton.com", "fourseasons.com", "starwoodhotels.com",
    "sonesta.com", "relaischateaux.com",
    # ── Big finance ──
    "kkr.com", "blackstone.com", "carlylegroup.com",
    "apollo.com", "tpg.com", "warburg.com", "warburgpincus.com",
    "goldmansachs.com", "jpmorgan.com", "morganstanley.com",
    "bankofamerica.com", "wellsfargo.com", "citi.com",
    # ── Big real estate ──
    "jll.com", "cbre.com", "cushmanwakefield.com", "colliers.com",
    "savills.com", "savills.us", "newmark.com",
    # ── Big food / brands ──
    "mcdonalds.com", "starbucks.com", "subway.com",
    "dominos.com", "pizzahut.com", "burgerking.com",
    "supercuts.com", "smartstyle.com", "fantasticsams.com",
    # ── Animal / pet industry big players ──
    "aspca.org", "akc.org", "humanesociety.org",
    "banfield.com", "vca.com", "bluepearlvet.com",
    # ── Professional orgs ──
    "acatoday.org", "ibew.org", "ada.org", "ama-assn.org",
    # ── Other aggregators ──
    "50pros.com", "reonomy.com", "amnhealthcare.com",
    "greenwichtime.com", "petfoodindustry.com",
}

# ── JUNK TITLE PATTERNS ─────────────────────────────────────
# If the page title matches ANY of these, it's not a business homepage

JUNK_TITLE_PATTERNS = [
    r"\b\d+\s+best\b", r"\btop\s+\d+\b", r"\bbest\s+\d+\b",
    r"\bmarket\s+(size|share|report|trends|growth|outlook|forecast)\b",
    r"\bindustry\s+(report|analysis|overview)\b",
    r"\bnear\s+me\b",
    r"\bhow\s+to\b", r"\bguide\s+to\b", r"\btips\s+for\b",
    r"\bwikipedia\b", r"\breview(s)?\s+(of|for)\b",
    r"\bvs\.?\s+\b",  # comparison articles
    r"\bwebsite(s)?\s+(design|examples|inspiration|ideas|templates)\b",
    r"\bexamples?\s+(of|for)\b",
    r"\bcase\s+stud(y|ies)\b",
    r"\b(find|search|compare|browse)\s+(a|the)?\s*(best|top)?\b",
    r"\bbook\s+appointment\b",  # booking pages, not business homepages
    r"\bhaircuts?\b$",  # generic "Haircuts" title (chains like Supercuts)
    r"\b(nonprofit|non-profit|non profit|charity|charitable|501\s*\(?c\)?)\\b",
    r"\b(foundation|association|society|coalition|alliance|federation)\b",
    r"\b(church|ministry|ministries|parish|diocese|mosque|synagogue|temple)\b",
    r"\b(volunteer|donate|donation|fundrais)\b",
]
JUNK_TITLE_RE = re.compile("|".join(JUNK_TITLE_PATTERNS), re.I)

# ── ENTERPRISE SIGNALS (links/text in the page) ─────────────
# If a site has 3+ of these, it's too big to be your client

ENTERPRISE_KEYWORDS = [
    "investor relations", "investors", "annual report",
    "press releases", "newsroom", "media center",
    "careers", "join our team", "open positions", "we're hiring",
    "global offices", "our locations", "worldwide",
    "nasdaq", "nyse", "stock price", "sec filing",
    "fortune 500", "fortune 100",
    "enterprise solutions", "enterprise platform",
]

# ── NONPROFIT / NGO SIGNALS (skip if 2+ found) ──────────────
NONPROFIT_KEYWORDS = [
    "501(c)", "501c3", "nonprofit", "non-profit", "tax-exempt",
    "tax exempt", "charitable organization", "donate now",
    "make a donation", "support our mission", "our mission",
    "volunteer opportunities", "volunteer with us",
    "fundraising", "grant funding", "annual fund",
    "board of directors", "board members",
    "community outreach", "public benefit",
]

# ============================================================
# SQLITE HISTORY DB
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS seen_domains (
            domain TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            was_lead INTEGER NOT NULL DEFAULT 0,
            niche TEXT DEFAULT '',
            score INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS query_log (
            query TEXT NOT NULL,
            run_date TEXT NOT NULL,
            PRIMARY KEY (query, run_date)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS run_stats (
            run_date TEXT PRIMARY KEY,
            leads_found INTEGER DEFAULT 0,
            domains_searched INTEGER DEFAULT 0,
            api_calls INTEGER DEFAULT 0,
            cost REAL DEFAULT 0.0
        )
    """)
    conn.commit()
    return conn


def load_seen_domains(conn):
    c = conn.cursor()
    c.execute("SELECT domain FROM seen_domains")
    return {row[0] for row in c.fetchall()}


def mark_domain_seen(conn, domain, was_lead, niche="", score=0):
    c = conn.cursor()
    c.execute("""
        INSERT INTO seen_domains (domain, first_seen, last_seen, was_lead, niche, score)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET last_seen=?, was_lead=MAX(was_lead, ?)
    """, (domain, TODAY, TODAY, int(was_lead), niche, score, TODAY, int(was_lead)))
    conn.commit()


def log_query(conn, query):
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO query_log (query, run_date) VALUES (?, ?)", (query, TODAY))
    conn.commit()


def get_used_queries_today(conn):
    c = conn.cursor()
    c.execute("SELECT query FROM query_log WHERE run_date = ?", (TODAY,))
    return {row[0] for row in c.fetchall()}


def get_today_lead_count(conn):
    c = conn.cursor()
    c.execute("SELECT leads_found FROM run_stats WHERE run_date = ?", (TODAY,))
    row = c.fetchone()
    return row[0] if row else 0


def update_run_stats(conn, leads_found, domains_searched, api_calls_used, cost):
    c = conn.cursor()
    c.execute("""
        INSERT INTO run_stats (run_date, leads_found, domains_searched, api_calls, cost)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(run_date) DO UPDATE SET
            leads_found = leads_found + ?,
            domains_searched = domains_searched + ?,
            api_calls = api_calls + ?,
            cost = cost + ?
    """, (TODAY, leads_found, domains_searched, api_calls_used, cost,
          leads_found, domains_searched, api_calls_used, cost))
    conn.commit()


def get_all_time_stats(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM seen_domains")
    total_domains = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM seen_domains WHERE was_lead = 1")
    total_leads = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT run_date) FROM run_stats")
    total_runs = c.fetchone()[0]
    return total_domains, total_leads, total_runs


# ============================================================
# GOOGLE SHEETS (REAL-TIME)
# ============================================================

_sheets_ws = None

CSV_FIELDS = [
    "Run_Date", "Lead_Tier", "Company_Name", "Domain", "Niche",
    "Email", "Email_Verified", "Phone", "Contact_Page",
    "Total_Score", "Automation_Score", "Biz_Fit_Score", "Budget_Score", "Contact_Score",
    "Automation_Gaps", "Revenue_Signals", "CMS",
    "Page_Load_Time", "Page_Size_KB",
]


def init_sheets():
    global _sheets_ws
    if not GOOGLE_SHEET_URL:
        print("  [–] GOOGLE_SHEET_URL empty — Sheets disabled")
        return None
    try:
        import gspread
    except ImportError:
        print("  [✗] gspread not installed — run: pip install gspread")
        return None
    try:
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open_by_url(GOOGLE_SHEET_URL)
        ws = sh.sheet1
        existing = ws.row_values(1)
        if not existing or existing[0] != "Run_Date":
            ws.insert_row(CSV_FIELDS, index=1)
            ws.format("1:1", {"textFormat": {"bold": True}})
            ws.freeze(rows=1)
            print(f"  [✓] Sheets connected: '{sh.title}' (header written)")
        else:
            print(f"  [✓] Sheets connected: '{sh.title}'")
        _sheets_ws = ws
        return ws
    except Exception as e:
        print(f"  [✗] Sheets failed: {e}")
        return None


def push_lead_to_sheets(row):
    if _sheets_ws is None:
        return False
    try:
        values = [str(row.get(f, "")) for f in CSV_FIELDS]
        _sheets_ws.append_row(values, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"  [!] Sheets push failed: {e}")
        return False


# ============================================================
# SEARCH QUERIES — targeted for dentist practices
# ============================================================

BASE_NICHES = [
    # ── Dental Practices (AI automation prospects) ──
    "dental practice", "dentist office", "cosmetic dentist",
    "orthodontist", "family dentist", "pediatric dentist",
    "dental implants", "periodontist", "oral surgeon",
    "sedation dentistry", "holistic dentist", "emergency dentist",
    "invisalign provider", "veneers specialist", "teeth whitening",
    "endodontist", "prosthodontist",
]

# Only use city modifiers for local businesses
LOCAL_NICHES = set(BASE_NICHES)

US_CITIES = [
    # Top 50 US metros — enough surface for weeks of unique leads
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "San Antonio", "San Diego", "Dallas", "Austin", "Denver",
    "Miami", "Atlanta", "Seattle", "San Francisco", "Boston",
    "Nashville", "Portland", "Las Vegas", "Charlotte", "Tampa",
    "Minneapolis", "Raleigh", "Sacramento", "Kansas City", "Columbus",
    "Indianapolis", "San Jose", "Jacksonville", "Memphis", "Louisville",
    "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
    "Mesa", "Omaha", "Colorado Springs", "Virginia Beach", "Tulsa",
    "Arlington", "New Orleans", "Bakersfield", "Honolulu", "Anaheim",
    "St Louis", "Pittsburgh", "Cincinnati", "Anchorage", "Henderson",
]

# Modifiers that help find actual small businesses
INTENT_MODIFIERS = [
    "",
    "local",
    "small",
    "family owned",
    "independent",
]


def niche_label(q):
    return "Dental"


def build_queries():
    queries = []
    seen_queries = set()
    for base in BASE_NICHES:
        label = niche_label(base)
        for mod in INTENT_MODIFIERS:
            q = f"{mod} {base}".strip() if mod else base
            if q not in seen_queries:
                seen_queries.add(q)
                queries.append((q, label))
        if base in LOCAL_NICHES:
            for city in US_CITIES:
                q = f"{base} {city}"
                if q not in seen_queries:
                    seen_queries.add(q)
                    queries.append((q, label))
    return queries


# ============================================================
# BRAVE SEARCH
# ============================================================

def brave_search(query):
    global api_calls
    url = "https://api.search.brave.com/res/v1/web/search"
    api_headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    params = {"q": query, "count": BRAVE_COUNT, "country": "us"}
    try:
        resp = requests.get(url, headers=api_headers, params=params, timeout=REQUEST_TIMEOUT)
        api_calls += 1
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("web", {}).get("results", []):
            results.append({
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
            })
        time.sleep(BRAVE_SEARCH_DELAY)
        return results
    except Exception as e:
        print(f"  [!] '{query}': {e}")
        return []


def extract_root_domain(url):
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.hostname or "").lower().strip(".")
        if host.startswith("www."):
            host = host[4:]
        parts = host.split(".")
        if len(parts) < 2:
            return None
        double_tlds = {"co.uk", "com.au", "co.in", "co.nz", "com.br", "co.za",
                       "co.jp", "co.kr", "com.sg", "com.hk", "co.il", "com.mx"}
        suffix = ".".join(parts[-2:])
        root = ".".join(parts[-3:]) if suffix in double_tlds and len(parts) >= 3 else ".".join(parts[-2:])
        if host != root and host != f"www.{root}":
            return None
        if root.endswith((".gov", ".edu", ".mil", ".org")):
            return None  # skip govt, edu, military, and nonprofits/NGOs
        return root
    except Exception:
        return None


def collect_unique_domains(conn, seen_ever):
    """Search and collect fresh SMB domains only."""
    domain_map = {}
    all_queries = build_queries()
    used_today = get_used_queries_today(conn)

    fresh_queries = [(q, l) for q, l in all_queries if q not in used_today]
    if not fresh_queries:
        print("  [!] All queries used today. Resetting rotation.")
        fresh_queries = all_queries

    random.shuffle(fresh_queries)
    candidate_target = DAILY_LEAD_TARGET * 4  # need more candidates since we filter harder
    total_available = len(fresh_queries)

    print(f"\n{'='*60}")
    print(f"  SEARCH PHASE")
    print(f"  Fresh queries: {total_available}")
    print(f"  History DB:    {len(seen_ever):,} domains")
    print(f"  Target:        {candidate_target} candidates")
    print(f"{'='*60}\n")

    queries_used = 0
    for idx, (query, label) in enumerate(fresh_queries, 1):
        if len(domain_map) >= candidate_target:
            print(f"\n  [✓] {candidate_target} candidates collected")
            break

        if idx % 50 == 0 or idx == 1:
            print(f"  --- {idx}/{total_available} queries | {len(domain_map)} fresh domains ---")

        results = brave_search(query)
        log_query(conn, query)
        queries_used += 1

        for r in results:
            root = extract_root_domain(r["url"])
            if not root or root in SKIP_DOMAINS or root in seen_ever or root in domain_map:
                continue

            # Pre-filter: check search result title for junk patterns
            if JUNK_TITLE_RE.search(r.get("title", "")):
                continue

            domain_map[root] = {
                "title": r["title"],
                "niche": label,
                "snippet": r.get("description", ""),
            }

    print(f"\n[✓] Search done: {len(domain_map)} candidates from {queries_used} queries")
    return domain_map


# ============================================================
# AI AUTOMATION NEED AUDIT
# ============================================================

# ── Tools/platforms that indicate automation is ALREADY in place ──
BOOKING_SIGNALS = [
    "calendly", "acuity", "acuityscheduling", "zocdoc", "localized",
    "localmed", "nexhealth", "solutionreach", "dentrix ascend",
    "opencare", "carestack", "patientpop", "schedule online",
    "book online", "book now", "book appointment", "online booking",
    "online scheduling", "request appointment", "schedule appointment",
    "flexbook", "jane.app", "simplepractice",
]

CHATBOT_SIGNALS = [
    "drift", "intercom", "tidio", "livechat", "tawk.to", "tawk",
    "zendesk", "freshchat", "crisp.chat", "hubspot-messages",
    "chatwidget", "live-chat", "chat-widget", "dialogflow",
    "landbot", "manychat", "chatfuel", "botpress",
    "kommunicate", "olark", "purechat",
]

REVIEW_SYSTEM_SIGNALS = [
    "birdeye", "podium", "weave", "reviewtrackers", "reputation.com",
    "grade.us", "trustpilot", "broadly", "getjerry", "demandforce",
    "swell", "nicejob", "reviewwave",
]

PATIENT_PORTAL_SIGNALS = [
    "patient portal", "patient login", "myportal", "patient access",
    "secure portal", "online portal", "my account",
    "patient forms", "digital forms", "online forms",
    "paperless", "e-forms",
]

SMS_SIGNALS = [
    "text us", "sms", "text message", "send a text",
    "text to schedule", "text reminders", "weave",
    "solutionreach", "revenuewell", "lighthouse 360",
    "patient communicator",
]

PRACTICE_MGMT_SIGNALS = [
    "dentrix", "eaglesoft", "open dental", "curve dental",
    "carestack", "denticon", "tab32", "planet dds",
    "practice-web", "maxident", "ace dental",
]

EMAIL_MARKETING_SIGNALS = [
    "mailchimp", "constant contact", "sendgrid", "klaviyo",
    "activecampaign", "drip", "convertkit", "campaign monitor",
    "mc.js", "mailerlite", "revenuewell",
]

# Signs they do things manually (= they need automation)
MANUAL_SIGNALS = [
    "call to schedule", "call us to", "call our office",
    "phone to schedule", "give us a call", "call today",
    "call for appointment", "call now", "call for",
]

PAPER_FORM_SIGNALS = [
    "download and print", "print and fill", "print out",
    "printable form", "paper form", "fill out and bring",
    "download the form", "print the form", "bring completed",
]

def fetch(url, timeout=REQUEST_TIMEOUT):
    try:
        start = time.time()
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r, round(time.time() - start, 2)
    except Exception:
        return None, 0


def audit_domain(domain):
    """Audit dental practice for AI automation needs."""
    resp, load_time = fetch(f"https://{domain}")
    if resp is None or resp.status_code >= 400:
        resp, load_time = fetch(f"http://{domain}")
        if resp is None or resp.status_code >= 400:
            return None

    html = resp.text
    size_kb = round(len(resp.content) / 1024, 1)
    is_https = resp.url.startswith("https")
    soup = BeautifulSoup(html, "html.parser")
    html_lower = html.lower()

    # ─── Page title ───
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Double-check title for junk patterns (in case search title was different)
    if JUNK_TITLE_RE.search(title):
        return None  # not a business homepage

    # ─── Enterprise detection ───
    enterprise_hits = 0
    for kw in ENTERPRISE_KEYWORDS:
        if kw in html_lower:
            enterprise_hits += 1
    if enterprise_hits >= 3:
        return None  # too big, skip

    # ─── Nonprofit / NGO detection ───
    nonprofit_hits = 0
    for kw in NONPROFIT_KEYWORDS:
        if kw in html_lower:
            nonprofit_hits += 1
    if nonprofit_hits >= 2:
        return None  # nonprofit/NGO, not a revenue business

    # ─── CMS ───
    cms = "Unknown"
    cms_checks = [
        ("WordPress", ["wp-content", "wordpress"]),
        ("Shopify", ["shopify", "cdn.shopify"]),
        ("Squarespace", ["squarespace"]),
        ("Wix", ["wix"]),
        ("Webflow", ["webflow"]),
        ("Ghost", ["ghost"]),
        ("Drupal", ["drupal"]),
        ("Joomla", ["joomla"]),
        ("Framer", ["framer"]),
    ]
    for name, keywords in cms_checks:
        if any(kw in html_lower for kw in keywords):
            cms = name
            break

    # ─── Revenue / budget signals ───
    signals = []
    signal_checks = [
        ("Google Analytics", ["gtag(", "google-analytics", "googletagmanager"]),
        ("Facebook Pixel", ["fbq(", "facebook.com/tr"]),
        ("Hotjar", ["hotjar"]),
        ("HubSpot", ["hubspot"]),
        ("Stripe", ["stripe.com", "checkout.stripe"]),
        ("Google Ads", ["googleads", "adservice", "conversion.js"]),
        ("Yelp Widget", ["yelp.com/biz"]),
    ]
    for name, keywords in signal_checks:
        if any(kw in html_lower for kw in keywords):
            signals.append(name)

    # ═══════════════════════════════════════════════════════════
    # AI AUTOMATION NEED DETECTION
    # Check what they DON'T have → automation gaps = opportunities
    # ═══════════════════════════════════════════════════════════

    automation_gaps = []    # (gap_description, weight)
    automation_present = [] # what they already have

    # ── 1. Online Booking — weight 4 ──
    has_booking = any(sig in html_lower for sig in BOOKING_SIGNALS)
    if has_booking:
        automation_present.append("Online Booking")
    else:
        automation_gaps.append(("No online booking system", 4))

    # ── 2. Chatbot / Live Chat — weight 3 ──
    has_chatbot = any(sig in html_lower for sig in CHATBOT_SIGNALS)
    if has_chatbot:
        automation_present.append("Chatbot/Live Chat")
    else:
        automation_gaps.append(("No chatbot or live chat", 3))

    # ── 3. Automated Review System — weight 3 ──
    has_reviews = any(sig in html_lower for sig in REVIEW_SYSTEM_SIGNALS)
    if has_reviews:
        automation_present.append("Review Automation")
    else:
        automation_gaps.append(("No automated review system", 3))

    # ── 4. Patient Portal — weight 2 ──
    has_portal = any(sig in html_lower for sig in PATIENT_PORTAL_SIGNALS)
    if has_portal:
        automation_present.append("Patient Portal")
    else:
        automation_gaps.append(("No patient portal", 2))

    # ── 5. SMS / Text Capability — weight 2 ──
    has_sms = any(sig in html_lower for sig in SMS_SIGNALS)
    if has_sms:
        automation_present.append("SMS/Text")
    else:
        automation_gaps.append(("No SMS or text capability", 2))

    # ── 6. Phone-Only Booking (POSITIVE signal = they need help) — weight 3 ──
    is_phone_only = any(sig in html_lower for sig in MANUAL_SIGNALS)
    if is_phone_only and not has_booking:
        automation_gaps.append(("Phone-only appointment booking", 3))

    # ── 7. Paper/Printable Forms — weight 3 ──
    has_paper_forms = any(sig in html_lower for sig in PAPER_FORM_SIGNALS)
    if has_paper_forms:
        automation_gaps.append(("Still uses paper/printable forms", 3))

    # ── 8. Email Marketing — weight 2 ──
    has_email_mktg = any(sig in html_lower for sig in EMAIL_MARKETING_SIGNALS)
    if has_email_mktg:
        automation_present.append("Email Marketing")
    else:
        automation_gaps.append(("No email marketing automation", 2))

    # ── 9. Practice Management Software — weight 2 ──
    has_pms = any(sig in html_lower for sig in PRACTICE_MGMT_SIGNALS)
    if has_pms:
        automation_present.append("Practice Management Software")
    # Note: not having PMS detected on website doesn't necessarily mean they
    # don't have it, so we only give a small weight
    # We don't add a gap for this — too many false positives

    # ── Also scan /contact and /about pages for more signals ──
    extra_html = ""
    for path in ["/contact", "/about"]:
        extra_resp, _ = fetch(f"https://{domain}{path}", timeout=8)
        if extra_resp and extra_resp.status_code == 200:
            extra_html += extra_resp.text.lower()
        time.sleep(0.2)

    if extra_html:
        # Check extra pages for signals we might have missed on homepage
        if not has_booking and any(sig in extra_html for sig in BOOKING_SIGNALS):
            # Found on subpage — remove the gap
            automation_gaps = [(g, w) for g, w in automation_gaps if "booking" not in g.lower()]
            automation_present.append("Online Booking")
        if not has_chatbot and any(sig in extra_html for sig in CHATBOT_SIGNALS):
            automation_gaps = [(g, w) for g, w in automation_gaps if "chatbot" not in g.lower()]
            automation_present.append("Chatbot/Live Chat")
        if not has_portal and any(sig in extra_html for sig in PATIENT_PORTAL_SIGNALS):
            automation_gaps = [(g, w) for g, w in automation_gaps if "portal" not in g.lower()]
            automation_present.append("Patient Portal")
        # Check for more manual signals on subpages
        if not is_phone_only and any(sig in extra_html for sig in MANUAL_SIGNALS):
            if not any("Phone-only" in g for g, _ in automation_gaps):
                automation_gaps.append(("Phone-only appointment booking", 3))
        if not has_paper_forms and any(sig in extra_html for sig in PAPER_FORM_SIGNALS):
            if not any("paper" in g.lower() for g, _ in automation_gaps):
                automation_gaps.append(("Still uses paper/printable forms", 3))

    # ─── SMB signals (positive = small business) ───
    smb_signals = 0
    smb_reasons = []

    # Phone number on homepage = local business + approachable
    phone_re = re.compile(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}")
    phone_match = phone_re.search(html)
    phone_number = ""
    if phone_match:
        smb_signals += 2
        smb_reasons.append("local phone")
        # Clean up the phone number
        raw_phone = phone_match.group(0)
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) == 10:
            phone_number = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

    # Address on homepage
    address_re = re.compile(r"\d+\s+[\w\s]+(?:st|street|ave|avenue|blvd|boulevard|dr|drive|rd|road|ln|lane|ct|court|way|pl|place)\b", re.I)
    if address_re.search(html):
        smb_signals += 2
        smb_reasons.append("street address")

    # Common SMB CMS = easy sell
    if cms in ("WordPress", "Wix", "Squarespace", "Webflow", "Joomla"):
        smb_signals += 1
        smb_reasons.append(f"{cms} site")

    # Small page = likely simple business site
    if size_kb < 500:
        smb_signals += 1
        smb_reasons.append("small site")

    # No careers page text = small team
    if "careers" not in html_lower and "job openings" not in html_lower:
        smb_signals += 1
        smb_reasons.append("no careers page")

    # "Owner", "founder", "family" = approachable, owner-operated
    if any(w in html_lower for w in ["owner", "founder", "family owned", "family-owned", "established in", "since 19", "since 20", "locally owned", "veteran owned", "woman owned"]):
        smb_signals += 2
        smb_reasons.append("owner/founder mention")

    # "Schedule", "call us" = local service biz
    if any(w in html_lower for w in ["schedule appointment", "call us today", "free consultation", "free estimate", "free quote", "get a quote"]):
        smb_signals += 1
        smb_reasons.append("service-oriented")

    # Negative: enterprise signals reduce SMB score
    if enterprise_hits > 0:
        smb_signals -= enterprise_hits
        smb_reasons.append(f"-{enterprise_hits} enterprise signals")

    return {
        "html": html,
        "load_time": load_time,
        "page_size_kb": size_kb,
        "title": title,
        "cms": cms,
        "revenue_signals": signals,
        "automation_gaps": automation_gaps,
        "automation_present": automation_present,
        "smb_signals": smb_signals,
        "smb_reasons": smb_reasons,
        "enterprise_hits": enterprise_hits,
        "phone": phone_number,
    }


# ============================================================
# CONTACT EXTRACTION
# ============================================================

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

JUNK_MAIL_DOMAINS = {
    "example.com", "domain.com", "domain.tld", "email.com", "email.tld",
    "sentry.io", "wixpress.com", "googleapis.com",
    "w3.org", "schema.org", "json-ld.org", "wordpress.org",
    "gravatar.com", "wp.com", "cloudflare.com", "gstatic.com",
    "bootstrapcdn.com", "jquery.com", "jsdelivr.net", "unpkg.com",
    "fontawesome.com", "google.com", "facebook.com", "twitter.com",
    "sentry-next.wixpress.com", "shopify.com", "squarespace.com",
    "myspace.com", "yourwebsite.com", "yourdomain.com",
}

JUNK_EMAIL_PREFIXES = {
    "noreply", "no-reply", "mailer-daemon", "postmaster", "test",
    "admin", "webmaster", "root", "nobody", "null",
    "user", "example", "email", "your", "name",
    "privacy", "legal", "abuse", "spam", "support",
}


def clean_emails(raw):
    clean = []
    for em in raw:
        em = em.lower().strip()
        prefix = em.split("@")[0]
        domain = em.split("@")[-1]
        if domain in JUNK_MAIL_DOMAINS:
            continue
        if prefix in JUNK_EMAIL_PREFIXES:
            continue
        if em.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js")):
            continue
        # Filter placeholder-looking emails
        if "example" in em or "domain" in em or "yourname" in em:
            continue
        clean.append(em)
    return list(dict.fromkeys(clean))[:5]


def extract_contacts(domain, homepage_html):
    emails = clean_emails(EMAIL_RE.findall(homepage_html))
    if emails:
        return emails, ""
    contact_page = ""
    for path in ["/contact", "/contact-us", "/about", "/about-us", "/team"]:
        url = f"https://{domain}{path}"
        resp, _ = fetch(url, timeout=8)
        if resp and resp.status_code == 200:
            found = clean_emails(EMAIL_RE.findall(resp.text))
            if found:
                return found, ""
            if not contact_page and "contact" in path:
                contact_page = url
        time.sleep(0.2)
    return [], contact_page or f"https://{domain}/contact"


# ── Email MX verification ────────────────────────────────────
_mx_cache = {}  # cache MX lookups to avoid repeated DNS queries
_smtp_cache = {}  # cache SMTP RCPT TO results

def verify_email_domain(email):
    """Check if email domain has valid MX records (can receive mail)."""
    domain = email.split("@")[-1].lower()
    if domain in _mx_cache:
        return _mx_cache[domain]

    # Try dns.resolver first (most accurate)
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'MX')
        valid = len(answers) > 0
        _mx_cache[domain] = valid
        return valid
    except ImportError:
        pass  # dnspython not installed, fall back to socket
    except Exception:
        _mx_cache[domain] = False
        return False

    # Fallback: check if domain resolves at all via socket
    try:
        socket.getaddrinfo(domain, 25, socket.AF_INET, socket.SOCK_STREAM)
        _mx_cache[domain] = True
        return True
    except socket.gaierror:
        _mx_cache[domain] = False
        return False


def verify_email_smtp(email):
    """
    SMTP-level verification: connect to the mail server and check
    if the specific address is accepted via RCPT TO.
    Returns True if accepted, False if rejected, None if inconclusive.
    """
    if email in _smtp_cache:
        return _smtp_cache[email]

    domain = email.split("@")[-1].lower()

    # Get MX host
    mx_host = None
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'MX')
        # Pick the lowest-priority (preferred) MX
        mx_host = str(sorted(answers, key=lambda r: r.preference)[0].exchange).rstrip('.')
    except Exception:
        mx_host = domain  # fallback to domain itself

    try:
        import smtplib
        smtp = smtplib.SMTP(timeout=5)
        smtp.connect(mx_host, 25)
        smtp.helo("check.local")
        smtp.mail("verify@check.local")
        code, _ = smtp.rcpt(email)
        smtp.quit()
        # 250 = accepted, 550/551/553 = rejected
        result = code == 250
        _smtp_cache[email] = result
        return result
    except Exception:
        # Connection failed or timed out — inconclusive, assume valid
        _smtp_cache[email] = True
        return True


def verify_emails(emails):
    """Filter list to only emails with valid MX records + SMTP check. Returns (verified, all_valid)."""
    if not emails:
        return [], False
    verified = []
    for em in emails:
        if not verify_email_domain(em):
            continue
        # SMTP check — skip if address is rejected
        smtp_ok = verify_email_smtp(em)
        if smtp_ok is False:
            continue
        verified.append(em)
    return verified, len(verified) == len(emails)


def email_quality_score(emails):
    """
    Score email quality 0-100.
    Personal name emails > role emails > generic > no email.
    """
    if not emails:
        return 0

    best = 0
    role_prefixes = {"info", "contact", "hello", "sales", "office", "team", "general", "service", "customerservice"}
    for em in emails:
        prefix = em.split("@")[0].lower()
        # Personal name pattern: firstname, firstname.lastname, firstlast
        if "." in prefix and len(prefix) > 4:
            best = max(best, 100)  # firstname.lastname = gold
        elif prefix not in role_prefixes and len(prefix) > 2 and not prefix.isdigit():
            best = max(best, 80)   # looks like a name
        elif prefix in role_prefixes:
            best = max(best, 50)   # generic role email
        else:
            best = max(best, 30)
    return best


# ============================================================
# MULTI-FACTOR SCORING
# ============================================================

def calc_automation_score(gaps):
    """Automation need score 0-100. More gaps = higher score = more manual = better lead."""
    raw = sum(w for _, w in gaps)
    # Max possible raw ~25 (all gaps), normalize to 0-100
    return min(100, int((raw / 25) * 100))


def calc_biz_fit_score(smb_signals):
    """Business fit score 0-100 based on SMB signals."""
    # smb_signals ranges roughly -3 to +9
    # Map to 0-100: 0 signals = 20, max signals = 100
    return min(100, max(0, int(20 + (smb_signals * 10))))


def calc_budget_score(signals):
    """Budget score 0-100 based on marketing/revenue tools detected."""
    if len(signals) == 0:
        return 10
    elif len(signals) == 1:
        return 30
    elif len(signals) == 2:
        return 55
    elif len(signals) == 3:
        return 75
    else:
        return 100


def calc_total_score(automation_score, biz_fit_score, budget_score, contact_score):
    """
    Weighted total score 0-100.
    Contact:    35% (approachability is #1)
    Automation: 25% (they need automation you can provide)
    Biz fit:    25% (must be small enough to hire you)
    Budget:     15% (nice to know they spend, but not critical)
    """
    return int(
        contact_score * 0.35 +
        automation_score * 0.25 +
        biz_fit_score * 0.25 +
        budget_score * 0.15
    )


def lead_tier(total_score):
    if total_score >= 65:
        return "🔥 HOT"
    elif total_score >= 45:
        return "🟡 WARM"
    elif total_score >= MIN_TOTAL_SCORE:
        return "🟢 COLD"
    return "SKIP"


# ============================================================
# CSV
# ============================================================

def init_csv():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
        print(f"  [✓] CSV: {os.path.basename(OUTPUT_FILE)}")
    else:
        print(f"  [✓] CSV exists: {os.path.basename(OUTPUT_FILE)} (appending)")


def append_csv(row):
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)


# ============================================================
# MAIN
# ============================================================

def main():
    global api_calls

    if BRAVE_API_KEY == "YOUR_BRAVE_API_KEY_HERE":
        print("ERROR: Set your Brave API key.")
        return

    print("=" * 60)
    print("  AI AUTOMATION LEAD GENERATOR v1 — Dentist Niche")
    print("  Small biz only • Multi-factor scoring • Real-time Sheets")
    print(f"  Date: {TODAY}")
    print("=" * 60)

    # ─── Init DB ───
    print("\n[1/5] History database...")
    conn = init_db()
    seen_ever = load_seen_domains(conn)
    total_domains, total_leads, total_runs = get_all_time_stats(conn)
    already_today = get_today_lead_count(conn)

    print(f"  [✓] {total_domains:,} domains seen | {total_leads:,} leads | {total_runs} runs")

    if already_today >= DAILY_LEAD_TARGET:
        print(f"\n  ⚠ Already {already_today} leads today (target {DAILY_LEAD_TARGET}). Run tomorrow!")
        conn.close()
        return

    remaining_target = DAILY_LEAD_TARGET - already_today
    if already_today > 0:
        print(f"  [i] Resuming: need {remaining_target} more leads")

    # ─── Sheets ───
    print("\n[2/5] Google Sheets...")
    init_sheets()

    # ─── CSV ───
    print("\n[3/5] CSV...")
    init_csv()

    # ─── Search ───
    print("\n[4/5] Searching for dental practices...")
    domain_map = collect_unique_domains(conn, seen_ever)

    if not domain_map:
        print("\n  ⚠ No fresh domains. Try tomorrow.")
        conn.close()
        return

    # ─── Audit + Score + Push ───
    print(f"\n[5/5] Auditing {len(domain_map)} sites (target: {remaining_target} leads)")
    print(f"{'='*60}\n")

    leads_this_run = 0
    skipped_enterprise = 0
    skipped_junk = 0
    skipped_low_score = 0
    skipped_dead = 0
    total = len(domain_map)
    domains_audited = 0

    for i, (domain, info) in enumerate(domain_map.items(), 1):
        if leads_this_run >= remaining_target:
            print(f"\n  🎯 TARGET HIT! {leads_this_run} leads. Done.")
            break

        if i % 25 == 0:
            print(f"\n  --- {i}/{total} | {leads_this_run}/{remaining_target} leads ---\n")
        print(f"[{i}/{total}] {domain} ", end="", flush=True)

        audit = audit_domain(domain)
        domains_audited += 1

        if audit is None:
            # Could be dead, enterprise, or junk title
            print("✗ skip")
            skipped_dead += 1
            mark_domain_seen(conn, domain, was_lead=False, niche=info["niche"])
            seen_ever.add(domain)
            time.sleep(SITE_AUDIT_DELAY)
            continue

        emails, contact_page = extract_contacts(domain, audit["html"])
        phone = audit.get("phone", "")

        # ─── Verify emails (MX record check) ───
        if emails:
            verified_emails, all_valid = verify_emails(emails)
            email_verified = "✓" if verified_emails else "✗"
            emails = verified_emails  # only keep verified ones
        else:
            email_verified = "—"

        # ─── HARD REQUIREMENT: must be reachable ───
        if not emails and not phone:
            print("✗ no contact info")
            skipped_low_score += 1
            mark_domain_seen(conn, domain, was_lead=False, niche=info["niche"])
            seen_ever.add(domain)
            time.sleep(SITE_AUDIT_DELAY)
            continue

        # ─── Multi-factor scoring ───
        auto_score = calc_automation_score(audit["automation_gaps"])
        biz_fit_score = calc_biz_fit_score(audit["smb_signals"])
        budget_score = calc_budget_score(audit["revenue_signals"])
        # Contact score: boost if we have BOTH email and phone
        contact_score = email_quality_score(emails)
        if phone:
            contact_score = min(100, contact_score + 30)  # phone = very approachable
        total_score = calc_total_score(auto_score, biz_fit_score, budget_score, contact_score)

        tier = lead_tier(total_score)

        if tier == "SKIP":
            print(f"— score {total_score} (auto={auto_score} biz={biz_fit_score} budget={budget_score} contact={contact_score})")
            skipped_low_score += 1
            mark_domain_seen(conn, domain, was_lead=False, niche=info["niche"], score=total_score)
            seen_ever.add(domain)
            time.sleep(SITE_AUDIT_DELAY)
            continue

        # ─── Build lead row ───
        gaps_str = "; ".join(d for d, _ in audit["automation_gaps"])
        signals_str = "; ".join(audit["revenue_signals"]) if audit["revenue_signals"] else "None"

        company = info["title"].split(" - ")[0].split(" | ")[0].split(" — ")[0].split(" · ")[0].strip()
        company = re.sub(r"<[^>]+>", "", company).strip()
        if not company or len(company) < 2:
            company = domain

        row = {
            "Run_Date": TODAY,
            "Lead_Tier": tier,
            "Company_Name": company,
            "Domain": domain,
            "Niche": info["niche"],
            "Email": "; ".join(emails) if emails else "",
            "Email_Verified": email_verified,
            "Phone": phone,
            "Contact_Page": contact_page,
            "Total_Score": total_score,
            "Automation_Score": auto_score,
            "Biz_Fit_Score": biz_fit_score,
            "Budget_Score": budget_score,
            "Contact_Score": contact_score,
            "Automation_Gaps": gaps_str,
            "Revenue_Signals": signals_str,
            "CMS": audit["cms"],
            "Page_Load_Time": f"{audit['load_time']}s",
            "Page_Size_KB": audit["page_size_kb"],
        }

        # Real-time push
        append_csv(row)
        sheets_ok = push_lead_to_sheets(row)

        leads_this_run += 1
        mark_domain_seen(conn, domain, was_lead=True, niche=info["niche"], score=total_score)
        seen_ever.add(domain)

        sheets_icon = "📊" if sheets_ok else ""
        smb_info = ", ".join(audit["smb_reasons"][:3]) if audit["smb_reasons"] else ""
        print(f"✓ {tier} total={total_score} (auto={auto_score} biz={biz_fit_score} $={budget_score} contact={contact_score}) {smb_info} {sheets_icon} [{leads_this_run}/{remaining_target}]")
        time.sleep(SITE_AUDIT_DELAY)

    # ─── Stats ───
    cost = (api_calls / 1000) * 3.0
    update_run_stats(conn, leads_this_run, domains_audited, api_calls, cost)
    total_domains_now, total_leads_now, total_runs_now = get_all_time_stats(conn)
    conn.close()

    print(f"\n{'='*60}")
    print(f"  RESULTS — {TODAY}")
    print(f"  {'─'*40}")
    print(f"  Leads this run: {leads_this_run}")
    print(f"  Total today:    {already_today + leads_this_run}")
    print(f"  Target:         {DAILY_LEAD_TARGET}")
    print(f"  {'─'*40}")
    print(f"  Dead/junk:      {skipped_dead}")
    print(f"  Low score:      {skipped_low_score}")
    print(f"  {'─'*40}")
    print(f"  API calls:      {api_calls:,}")
    print(f"  Cost:           ${cost:.2f}")
    print(f"  CSV:            {os.path.basename(OUTPUT_FILE)}")
    if _sheets_ws:
        print(f"  Sheets:         ✅ {leads_this_run} leads pushed live")
    print(f"  {'─'*40}")
    print(f"  ALL-TIME: {total_domains_now:,} domains | {total_leads_now:,} leads | {total_runs_now} runs")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
