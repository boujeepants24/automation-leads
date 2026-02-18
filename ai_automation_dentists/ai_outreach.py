"""
AI Automation Email Outreach v1 — Dentist Niche Campaign
Sends personalized cold emails + automated follow-ups for AI automation services.
Reads from ai_leads CSV, tracks everything in ai_leads_history.db.
Pushes outreach log to Google Sheets in real-time.

Usage:
    python ai_outreach.py                    # fresh emails + follow-ups
    python ai_outreach.py --fresh-only       # only new contacts
    python ai_outreach.py --followups-only   # only follow-ups
    python ai_outreach.py --dry-run          # preview without sending
    python ai_outreach.py --test you@email   # send 1 test to yourself
    python ai_outreach.py --replied domain.com  # mark as replied (no more follow-ups)
    python ai_outreach.py --status               # show campaign stats
    python ai_outreach.py --csv file.csv          # specific CSV
"""

import smtplib
import imaplib
import csv
import sqlite3
import os
import sys
import time
import random
import re
import socket
import email as email_lib
import gspread
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText

# ============================================================
# CONFIG
# ============================================================

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

YOUR_NAME = "YOUR_NAME_HERE"
YOUR_TITLE = "AI Automation Specialist"

# ── SENDER ACCOUNTS ──────────────────────────────────────────
# Add/remove accounts here. Each one warms up independently.
# To add a new account: create Gmail, enable 2FA, generate app password,
# then add a new dict below.
ACCOUNTS = [
    {
        "email": "YOUR_EMAIL@gmail.com",
        "password": "YOUR_APP_PASSWORD_HERE",
        "created": "2026-02-01",       # when you made this account
    },
    # Add more sender accounts as needed:
    # {
    #     "email": "your_second_email@gmail.com",
    #     "password": "YOUR_APP_PASSWORD_HERE",
    #     "created": "2026-02-14",
    # },
]

def _account_warmup_limit(created_date):
    """Auto-ramp fresh daily limit based on account age."""
    age_days = (date.today() - date.fromisoformat(created_date)).days
    if age_days < 14:
        return 15       # week 1-2: gentle start
    elif age_days < 28:
        return 25       # week 3-4: building reputation
    elif age_days < 42:
        return 35       # week 5-6: ramping up
    else:
        return 50       # week 7+: full speed

# Combined limit across all accounts (uses manual 'limit' override if set, otherwise warmup ramp)
FRESH_DAILY_LIMIT = sum(a.get("limit") or _account_warmup_limit(a["created"]) for a in ACCOUNTS)
FOLLOWUP_DAILY_LIMIT = 100      # follow-ups per day
TOTAL_DAILY_CAP = 200           # absolute max emails/day
MIN_DELAY = 20                  # random delay range (seconds)
MAX_DELAY = 90                  # keeps Gmail happy
MIN_SCORE = 40

# For backward compat — default sender
SMTP_EMAIL = ACCOUNTS[0]["email"]
SMTP_APP_PASSWORD = ACCOUNTS[0]["password"]

# Follow-up timing — fast follow-up cycle
FOLLOWUP_1_DAYS = 3             # days after first email
FOLLOWUP_2_DAYS = 7             # days after first email

# Google Sheets (same sheet as lead gen, new tab)
GOOGLE_CREDS_FILE = "service_account.json"
GOOGLE_SHEET_URL = ""

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "ai_leads_history.db")
TODAY = date.today().isoformat()


# ============================================================
# DATABASE
# ============================================================

def init_outreach_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Check if old table exists without follow-up columns
    c.execute("SELECT sql FROM sqlite_master WHERE name='sent_emails'")
    row = c.fetchone()

    if row and "followup_1_date" not in (row[0] or ""):
        # Migrate: add new columns to existing table
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN followup_1_date TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN followup_2_date TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN status TEXT DEFAULT 'sent'")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN company TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN niche TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN issues TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        conn.commit()
    elif not row:
        # Fresh table
        c.execute("""
            CREATE TABLE IF NOT EXISTS sent_emails (
                domain TEXT NOT NULL,
                email TEXT NOT NULL,
                sent_date TEXT NOT NULL,
                template_used TEXT NOT NULL,
                subject TEXT NOT NULL,
                followup_1_date TEXT,
                followup_2_date TEXT,
                status TEXT DEFAULT 'sent',
                company TEXT DEFAULT '',
                niche TEXT DEFAULT '',
                issues TEXT DEFAULT '',
                sender_account TEXT DEFAULT '',
                PRIMARY KEY (domain, email)
            )
        """)

    # Migrate: add sender_account column if missing
    if row and "sender_account" not in (row[0] or ""):
        try:
            c.execute("ALTER TABLE sent_emails ADD COLUMN sender_account TEXT DEFAULT ''")
            # Backfill existing rows with the primary account
            c.execute("UPDATE sent_emails SET sender_account = ? WHERE sender_account = ''",
                      (ACCOUNTS[0]["email"],))
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # Migrate outreach_stats if old schema (missing fresh_sent column)
    c.execute("SELECT sql FROM sqlite_master WHERE name='outreach_stats'")
    stats_row = c.fetchone()
    if stats_row and "fresh_sent" not in (stats_row[0] or ""):
        c.execute("DROP TABLE outreach_stats")
        stats_row = None  # recreate below

    if not stats_row:
        c.execute("""
            CREATE TABLE IF NOT EXISTS outreach_stats (
                run_date TEXT PRIMARY KEY,
                fresh_sent INTEGER DEFAULT 0,
                followups_sent INTEGER DEFAULT 0
            )
        """)
    conn.commit()
    return conn


def already_emailed(conn, domain):
    c = conn.cursor()
    c.execute("SELECT 1 FROM sent_emails WHERE domain = ?", (domain,))
    return c.fetchone() is not None


def log_sent(conn, domain, email, template_name, subject, company="", niche="", issues="", sender_account=""):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO sent_emails
        (domain, email, sent_date, template_used, subject, status, company, niche, issues, sender_account)
        VALUES (?, ?, ?, ?, ?, 'sent', ?, ?, ?, ?)
    """, (domain, email, TODAY, template_name, subject, company, niche, issues, sender_account))
    c.execute("""
        INSERT INTO outreach_stats (run_date, fresh_sent)
        VALUES (?, 1)
        ON CONFLICT(run_date) DO UPDATE SET fresh_sent = fresh_sent + 1
    """, (TODAY,))
    conn.commit()


def log_followup(conn, domain, email, followup_num):
    c = conn.cursor()
    col = f"followup_{followup_num}_date"
    c.execute(f"UPDATE sent_emails SET {col} = ? WHERE domain = ? AND email = ?",
              (TODAY, domain, email))
    c.execute("""
        INSERT INTO outreach_stats (run_date, followups_sent)
        VALUES (?, 1)
        ON CONFLICT(run_date) DO UPDATE SET followups_sent = followups_sent + 1
    """, (TODAY,))
    conn.commit()


def get_today_stats(conn):
    c = conn.cursor()
    c.execute("SELECT fresh_sent, followups_sent FROM outreach_stats WHERE run_date = ?", (TODAY,))
    row = c.fetchone()
    if row:
        return {"fresh": row[0] or 0, "followups": row[1] or 0}
    return {"fresh": 0, "followups": 0}


def get_total_sent(conn):
    c = conn.cursor()
    c.execute("SELECT SUM(fresh_sent), SUM(followups_sent) FROM outreach_stats")
    row = c.fetchone()
    return {"fresh": row[0] or 0, "followups": row[1] or 0}


def get_followup_queue(conn, followup_num, days_after):
    """Get leads that need follow-up N (haven't received it yet, sent X+ days ago)."""
    cutoff = (date.today() - timedelta(days=days_after)).isoformat()
    col = f"followup_{followup_num}_date"
    c = conn.cursor()
    c.execute(f"""
        SELECT domain, email, company, niche, issues, template_used, sender_account
        FROM sent_emails
        WHERE {col} IS NULL
          AND status = 'sent'
          AND sent_date <= ?
        ORDER BY sent_date ASC
    """, (cutoff,))
    return c.fetchall()


def mark_replied(conn, domain):
    """Mark a domain as replied — stops all follow-ups."""
    c = conn.cursor()
    c.execute("UPDATE sent_emails SET status = 'replied' WHERE domain = ?", (domain,))
    conn.commit()
    return c.rowcount > 0


def get_pending_followup_domains(conn):
    """Get all domains that are still in 'sent' status (not replied)."""
    c = conn.cursor()
    c.execute("SELECT DISTINCT domain FROM sent_emails WHERE status = 'sent'")
    return {row[0] for row in c.fetchall()}


# ============================================================
# REPLY DETECTION VIA IMAP
# ============================================================

def check_replies_imap(conn, dry_run=False):
    """
    Connect to Gmail via IMAP for EVERY sender account, scan inbox for
    replies from lead domains.  Auto-marks any replied domains in the DB.
    Returns set of domains that replied.
    """
    pending_domains = get_pending_followup_domains(conn)
    if not pending_domains:
        return set()

    replied = set()

    for account in ACCOUNTS:
        acct_email = account["email"]
        acct_password = account["password"]
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(acct_email, acct_password)
            mail.select("INBOX", readonly=True)

            # Search for emails received in the last 14 days
            since_date = (date.today() - timedelta(days=14)).strftime("%d-%b-%Y")
            status, messages = mail.search(None, f'(SINCE "{since_date}")')

            if status != "OK" or not messages[0]:
                mail.logout()
                continue

            msg_ids = messages[0].split()
            # Check latest 500 messages max per account to keep it fast
            for msg_id in msg_ids[-500:]:
                try:
                    status, data = mail.fetch(msg_id, "(BODY[HEADER.FIELDS (FROM)])")
                    if status != "OK":
                        continue
                    raw_from = data[0][1].decode("utf-8", errors="ignore")
                    # Extract email domain from From header
                    from_match = re.search(r'[\w.+-]+@([\w.-]+)', raw_from)
                    if not from_match:
                        continue
                    from_domain = from_match.group(1).lower()
                    # Check if this sender's domain matches any pending lead
                    if from_domain in pending_domains:
                        replied.add(from_domain)
                except Exception:
                    continue

            mail.logout()
        except imaplib.IMAP4.error as e:
            print(f"  [!] IMAP login failed for {acct_email}: {e}")
            print(f"  [i] Make sure IMAP is enabled in Gmail settings for {acct_email}")
            continue
        except Exception as e:
            print(f"  [!] Reply check error for {acct_email}: {e}")
            continue

    # Mark replied domains in DB
    for domain in replied:
        if not dry_run:
            mark_replied(conn, domain)
        print(f"  [REPLY] {domain} replied — skipping follow-ups")

    return replied


def check_bounces_imap(conn, dry_run=False):
    """
    Scan ALL sender account inboxes for bounce/delivery-failure notifications.
    Auto-marks bounced domains as 'bounced' in DB to stop follow-ups.
    Returns set of bounced domains.
    """
    pending_domains = get_pending_followup_domains(conn)
    if not pending_domains:
        return set()

    bounced = set()

    for account in ACCOUNTS:
        acct_email = account["email"]
        acct_password = account["password"]
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(acct_email, acct_password)
            mail.select("INBOX", readonly=True)

            # Search for bounce notifications in the last 14 days
            since_date = (date.today() - timedelta(days=14)).strftime("%d-%b-%Y")
            status, messages = mail.search(None, f'(SINCE "{since_date}" FROM "mailer-daemon")')

            if status != "OK" or not messages[0]:
                mail.logout()
                continue

            msg_ids = messages[0].split()
            for msg_id in msg_ids[-200:]:
                try:
                    status, data = mail.fetch(msg_id, "(RFC822)")
                    if status != "OK":
                        continue
                    raw_msg = data[0][1]
                    msg = email_lib.message_from_bytes(raw_msg)
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                body += part.get_payload(decode=True).decode("utf-8", errors="ignore")
                    else:
                        body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")

                    for addr_match in re.finditer(r'[\w.+-]+@([\w.-]+)', body):
                        found_domain = addr_match.group(1).lower()
                        if found_domain in pending_domains:
                            bounced.add(found_domain)
                except Exception:
                    continue

            mail.logout()
        except Exception as e:
            print(f"  [!] Bounce check error for {acct_email}: {e}")
            continue

    # Mark bounced domains
    for domain in bounced:
        if not dry_run:
            c = conn.cursor()
            c.execute("UPDATE sent_emails SET status = 'bounced' WHERE domain = ?", (domain,))
            conn.commit()
        print(f"  [BOUNCE] {domain} bounced — removed from follow-up queue")

    return bounced


# ============================================================
# GOOGLE SHEETS — Outreach tab
# ============================================================

_outreach_sheet = None

def init_outreach_sheets():
    global _outreach_sheet
    creds_path = os.path.join(SCRIPT_DIR, GOOGLE_CREDS_FILE)
    if not os.path.exists(creds_path):
        print("  [i] No service_account.json — Sheets disabled")
        return

    try:
        gc = gspread.service_account(filename=creds_path)
        sh = gc.open_by_url(GOOGLE_SHEET_URL)

        # Get or create "Outreach" tab
        try:
            _outreach_sheet = sh.worksheet("Outreach")
        except gspread.exceptions.WorksheetNotFound:
            _outreach_sheet = sh.add_worksheet(title="Outreach", rows=2000, cols=10)
            # Write headers
            _outreach_sheet.update("A1:H1", [[
                "Date", "Domain", "Company", "Email",
                "Template", "Type", "Subject", "Status"
            ]])
            print("  [✓] Created 'Outreach' tab in Google Sheets")

        print("  [✓] Google Sheets connected (Outreach tab)")
    except Exception as e:
        print(f"  [!] Sheets error: {e}")
        _outreach_sheet = None


def push_to_sheets(date_str, domain, company, email, template, email_type, subject, status="sent"):
    if not _outreach_sheet:
        return
    try:
        _outreach_sheet.append_row(
            [date_str, domain, company, email, template, email_type, subject, status],
            value_input_option="RAW"
        )
    except Exception:
        pass  # don't crash over Sheets errors


# ============================================================
# EMAIL TEMPLATES — AI Automation for Dentists
# ============================================================

def pick_top_issues(issues_str, max_issues=3):
    if not issues_str:
        return ["some technical issues"]
    issues = [i.strip() for i in issues_str.split(";") if i.strip()]
    return issues[:max_issues] if issues else ["some technical issues"]


GENERIC_PREFIXES = {
    "info", "contact", "hello", "office", "admin", "sales",
    "support", "help", "team", "mail", "enquiry", "inquiry",
    "service", "services", "billing", "accounts",
}

COMMON_FIRST_NAMES = {
    "james", "john", "robert", "michael", "david", "william", "richard", "joseph",
    "thomas", "charles", "chris", "daniel", "matthew", "anthony", "mark", "donald",
    "steven", "paul", "andrew", "joshua", "kenneth", "kevin", "brian", "george",
    "timothy", "ronald", "edward", "jason", "jeffrey", "ryan", "jacob", "gary",
    "nicholas", "eric", "jonathan", "stephen", "larry", "justin", "scott", "brandon",
    "benjamin", "samuel", "raymond", "gregory", "frank", "alexander", "patrick",
    "jack", "dennis", "jerry", "tyler", "aaron", "jose", "adam", "nathan", "henry",
    "peter", "zachary", "douglas", "harold", "kyle", "noah", "gerald", "ethan",
    "carl", "terry", "sean", "austin", "arthur", "lawrence", "jesse", "dylan",
    "bryan", "joe", "jordan", "billy", "bruce", "albert", "willie", "gabriel",
    "logan", "alan", "juan", "wayne", "elijah", "randy", "roy", "vincent",
    "ralph", "eugene", "russell", "bobby", "mason", "philip", "harry", "dale",
    "mary", "patricia", "jennifer", "linda", "barbara", "elizabeth", "susan",
    "jessica", "sarah", "karen", "lisa", "nancy", "betty", "margaret", "sandra",
    "ashley", "dorothy", "kimberly", "emily", "donna", "michelle", "carol",
    "amanda", "melissa", "deborah", "stephanie", "rebecca", "sharon", "laura",
    "cynthia", "kathleen", "amy", "angela", "shirley", "anna", "brenda", "pamela",
    "emma", "nicole", "helen", "samantha", "katherine", "christine", "debra",
    "rachel", "carolyn", "janet", "catherine", "maria", "heather", "diane",
    "ruth", "julie", "olivia", "joyce", "virginia", "victoria", "kelly", "lauren",
    "christina", "joan", "evelyn", "judith", "megan", "andrea", "cheryl", "hannah",
    "jacqueline", "martha", "gloria", "teresa", "ann", "sara", "madison", "frances",
    "kathryn", "janice", "jean", "abigail", "alice", "judy", "sophia", "grace",
    "denise", "amber", "doris", "marilyn", "danielle", "beverly", "isabella",
    "theresa", "diana", "natalie", "brittany", "charlotte", "marie", "kayla",
    "alexis", "lori", "mike", "matt", "dan", "tom", "bob", "jim", "tim", "ben",
    "sam", "max", "alex", "nick", "luke", "jake", "cole", "drew", "chad", "brad",
    "todd", "kurt", "troy", "seth", "wade", "brent", "derek", "lance", "neil",
    "tony", "dave", "steve", "phil", "rick", "jeff", "greg", "doug", "ted", "ray",
    "jen", "kate", "beth", "anne", "jill", "dana", "tara", "erin", "meg", "lynn",
}

def guess_first_name(email):
    prefix = email.split("@")[0].lower()
    if prefix in GENERIC_PREFIXES:
        return ""
    parts = re.split(r"[._\-]+", prefix)
    name = parts[0]
    if name in COMMON_FIRST_NAMES:
        return name.capitalize()
    return ""


def format_issues_list(issues):
    """Turn automation gaps into natural, conversational prose."""
    if not issues:
        return "a few areas where things could run more smoothly"
    
    cleaned = []
    for issue in issues:
        i_lower = issue.lower()
        if "booking" in i_lower or "appointment" in i_lower:
            cleaned.append("how patients book appointments")
        elif "chatbot" in i_lower or "chat" in i_lower:
            cleaned.append("after-hours patient communication")
        elif "review" in i_lower:
            cleaned.append("how you're collecting patient reviews")
        elif "portal" in i_lower:
            cleaned.append("patient access to their records")
        elif "sms" in i_lower or "text" in i_lower:
            cleaned.append("text-based communication with patients")
        elif "phone-only" in i_lower or "call" in i_lower:
            cleaned.append("the booking flow relying entirely on phone calls")
        elif "paper" in i_lower or "print" in i_lower:
            cleaned.append("intake forms that still need to be printed")
        elif "email marketing" in i_lower:
            cleaned.append("staying in touch with patients between visits")
        else:
            cleaned.append("some workflow gaps")

    cleaned = list(dict.fromkeys(cleaned))
    
    if len(cleaned) == 1:
        return cleaned[0]
    elif len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    else:
        return f"{cleaned[0]}, {cleaned[1]}, and a couple of other things"



def template_quick_audit(lead, issues):
    company = lead.get("Company_Name", lead.get("company", ""))
    domain = lead.get("Domain", lead.get("domain", ""))
    niche_lower = lead.get("Niche", lead.get("niche", "dental")).lower()
    issues_text = format_issues_list(issues)
    name = guess_first_name(lead.get("Email", lead.get("email", "")).split(";")[0].strip())
    greeting = f"Hi {name}" if name else "Hi there"

    subjects = [
        f"Thought about {company}",
        f"Quick note about {company}",
        f"Something I noticed at {domain}",
        f"Re: {company}",
    ]
    subject = random.choice(subjects)

    bodies = [
        f"""{greeting},

I was looking into a few dental practices recently and came across {domain}. It caught my attention because it looks like a lot of the day-to-day — reminders, follow-ups, intake — is still being handled manually.

That's not unusual, most practices are in the same boat. But the ones I've worked with that started automating those pieces ended up freeing up a surprising amount of time for their team.

I took some notes on what could apply to {company} specifically. Happy to share them if that'd be useful — no strings attached.

{YOUR_NAME}""",
        f"""{greeting},

Came across {domain} earlier and spent a few minutes clicking around. Nice practice.

One thing I noticed is that {issues_text} — it's the kind of thing that adds up quietly over time but is also pretty straightforward to improve with the right setup.

I do this kind of work for dental practices regularly, so I have a decent sense of what moves the needle. If you're open to it, I'd be glad to share what I found.

{YOUR_NAME}""",
        f"""{greeting},

I hope this isn't out of the blue — I was doing some research on dental practices in your area and {domain} came up.

It seems like there's an opportunity to take some of the repetitive stuff off your team's plate — things like appointment confirmations, patient follow-ups, review collection. The kind of work that's important but eats hours.

I've been helping practices set up systems for exactly this. Would it be worth a quick conversation?

{YOUR_NAME}""",
    ]

    return subject, random.choice(bodies), "quick_audit"


def template_competitor_angle(lead, issues):
    company = lead.get("Company_Name", lead.get("company", ""))
    domain = lead.get("Domain", lead.get("domain", ""))
    niche_lower = lead.get("Niche", lead.get("niche", "dental")).lower()
    issues_text = format_issues_list(issues)
    name = guess_first_name(lead.get("Email", lead.get("email", "")).split(";")[0].strip())
    greeting = f"Hi {name}" if name else "Hi"

    subjects = [
        f"Something I've been seeing with practices like {company}",
        f"Trend I keep noticing",
        f"Quick thought for {company}",
    ]
    subject = random.choice(subjects)

    bodies = [
        f"""{greeting},

I've been working with a number of dental practices lately, and there's a pattern I keep seeing — the ones that automate their patient communication tend to run noticeably smoother than the ones that don't.

I'm talking about things like reminders going out automatically, reviews being requested without anyone having to think about it, and patients who haven't come in for a while getting a thoughtful nudge to rebook.

I took a look at {domain} and it seems like {company} could benefit from some of that. Not trying to sell you anything right now — just thought it was worth flagging.

Let me know if you'd ever want to talk through it.

{YOUR_NAME}""",
        f"""{greeting},

I spend a lot of time looking at how dental practices operate online, and one thing that keeps standing out is how much time gets lost on tasks that could easily run on their own.

{company} looks like a solid practice, but from what I can see, there are a few things — {issues_text} — that other practices in your area have already started automating.

Not saying that to be alarmist — just figured it's the kind of thing that's easier to act on sooner than later.

If you're curious, I'm happy to share more. If not, no worries at all.

{YOUR_NAME}""",
        f"""{greeting},

Not sure if this is on your radar, but I've noticed more and more dental practices moving toward automating their patient workflows — rebooking, reminders, review requests, that sort of thing.

I looked at {domain} and I think {company} is well positioned to get a lot out of it. It wouldn't take much to set up, and the practices I've helped with this usually see the impact pretty quickly.

Worth a conversation? Totally fine if the timing's not right.

{YOUR_NAME}""",
    ]

    return subject, random.choice(bodies), "competitor"


def template_helpful_tip(lead, issues):
    company = lead.get("Company_Name", lead.get("company", ""))
    domain = lead.get("Domain", lead.get("domain", ""))
    niche_lower = lead.get("Niche", lead.get("niche", "dental")).lower()
    first_issue = issues[0] if issues else "some workflow gaps"
    issues_text = format_issues_list(issues)
    name = guess_first_name(lead.get("Email", lead.get("email", "")).split(";")[0].strip())
    greeting = f"Hi {name}" if name else "Hi"

    subjects = [
        f"Idea for {company}",
        f"Something that might help at {company}",
        f"Quick thought for {domain}",
    ]
    subject = random.choice(subjects)

    bodies = [
        f"""{greeting},

I was looking at {domain} and a couple of things jumped out at me that I thought were worth mentioning.

Most of the dental practices I work with deal with the same challenges — no-shows, keeping their Google reviews strong, and re-engaging patients who haven't been in for a while. The ones that automate those three things almost always see a meaningful difference within a few weeks.

It's not complicated to set up, and it runs quietly in the background once it's going. I think it could work well for {company}.

If that sounds interesting, I'd be glad to walk you through it. And if not, no hard feelings — just wanted to pass it along.

{YOUR_NAME}""",
        f"""{greeting},

I came across {company} and wanted to reach out because I noticed {issues_text}.

In my experience working with dental practices, those are usually the highest-leverage things to address — not because they're broken, but because fixing them tends to have an outsized impact on how the practice operates day to day.

I've got some specific thoughts on how this could work for you. Happy to share if you're interested, or feel free to ignore this entirely.

{YOUR_NAME}""",
        f"""{greeting},

I know you're probably busy running {company}, so I'll keep this brief.

I took a look at your online setup and noticed a few areas where some simple automation could save your team real time — particularly around {issues_text}. These are common gaps, nothing unusual, but they tend to add up.

I've helped other practices sort this out and it's usually a pretty painless process. If you'd want to hear more, just let me know.

{YOUR_NAME}""",
    ]

    return subject, random.choice(bodies), "helpful_tip"


def get_actionable_tip(issue):
    """Return a conversational, non-technical tip for a specific automation gap."""
    issue_lower = issue.lower()
    tips = {
        "no online booking": "Right now it looks like patients have to call in to schedule. Most practices that add online booking see a noticeable bump in new patient appointments, especially from people searching after hours.",
        "no chatbot": "There's no way for patients to get quick answers when your office is closed. A simple automated chat can handle the most common questions and capture leads overnight.",
        "no automated review": "It looks like you're not automatically asking patients for reviews after visits. The practices that do this consistently tend to build their Google rating much faster.",
        "no patient portal": "Patients don't seem to have a way to access their info online. A simple portal for forms, records, and appointment history tends to reduce front desk calls significantly.",
        "no sms": "Text messaging is becoming the default way patients want to communicate. Automated appointment reminders via text alone can cut no-shows by a third or more.",
        "phone-only": "Right now everything goes through the phone, which means your front desk is the bottleneck. Giving patients other ways to book and communicate takes a lot of pressure off your team.",
        "paper": "It looks like intake forms still need to be printed and filled out. Digital forms that patients complete on their phone before they arrive save everyone time and reduce errors.",
        "no email marketing": "There doesn't appear to be any automated patient communication between visits. Even basic things like recall reminders and birthday messages help with retention.",
    }
    for key, tip in tips.items():
        if key in issue_lower:
            return tip
    return f"I noticed {issue} at your practice. It's a common gap that's usually straightforward to address."


TEMPLATES = [template_quick_audit, template_competitor_angle, template_helpful_tip]


# ── Follow-up templates ──────────────────────────────────────

def followup_1_template(name, domain, company, niche):
    """Follow-up #1 — gentle bump, 3 days later."""
    greeting = f"Hi {name}" if name else "Hi"
    subjects = [
        f"Re: {company}",
        f"Following up",
        f"Re: {domain}",
    ]

    bodies = [
        f"""{greeting},

Just wanted to make sure my last email didn't get lost — I sent a note about {company} a few days ago.

No rush on my end. Just figured I'd check in since inboxes can be brutal.

{YOUR_NAME}""",
        f"""{greeting},

Circling back on my earlier message about {company}. Completely understand if the timing isn't right — just didn't want it to slip through the cracks.

{YOUR_NAME}""",
        f"""{greeting},

Quick follow-up — I reached out recently about some ideas for {company}. If it landed in spam or just got buried, wanted to surface it one more time.

Happy to chat whenever works, or feel free to disregard.

{YOUR_NAME}""",
    ]

    return random.choice(subjects), random.choice(bodies), "followup_1"


def followup_2_template(name, domain, company, niche):
    """Follow-up #2 — final note, 7 days later."""
    greeting = f"Hi {name}" if name else "Hi"
    subjects = [
        f"Last note — {company}",
        f"One last thing",
        f"{company}",
    ]

    bodies = [
        f"""{greeting},

Last email from me on this — I know how it feels to have a stranger in your inbox.

I genuinely think there are a few things that could make life easier at {company}, but I also respect your time. If you ever want to revisit it, my door's open.

Wishing you all the best.

{YOUR_NAME}""",
        f"""{greeting},

I'll leave this here and won't follow up again. I reached out about {company} because I thought there were some things worth looking at, but I understand if now isn't the time.

If anything changes, feel free to reply to this whenever.

Take care,
{YOUR_NAME}""",
        f"""{greeting},

Final note from me. I still think {company} has some real opportunities to streamline things, but I don't want to be that person who keeps showing up in your inbox.

If you're ever curious, just reply — I'll be around.

All the best,
{YOUR_NAME}""",
    ]

    return random.choice(subjects), random.choice(bodies), "followup_2"


# ============================================================
# EMAIL SENDING — multi-sender round-robin
# ============================================================

# Connection pool: one persistent connection per account
_smtp_pool = {}       # email -> smtplib.SMTP
_sender_idx = 0       # round-robin index

def _get_smtp_for(account):
    """Get or create a persistent SMTP connection for a specific account."""
    email = account["email"]
    conn = _smtp_pool.get(email)
    try:
        if conn is not None:
            conn.noop()  # check if still alive
            return conn
    except Exception:
        _smtp_pool.pop(email, None)

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(email, account["password"])
    _smtp_pool[email] = server
    return server

def _next_sender():
    """Round-robin: pick the next sender account."""
    global _sender_idx
    account = ACCOUNTS[_sender_idx % len(ACCOUNTS)]
    _sender_idx += 1
    return account

def close_smtp():
    """Close all persistent SMTP connections."""
    for email, conn in list(_smtp_pool.items()):
        try:
            conn.quit()
        except Exception:
            pass
    _smtp_pool.clear()


def _find_account_by_email(sender_email):
    """Look up an account dict by email. Falls back to primary account."""
    for acct in ACCOUNTS:
        if acct["email"] == sender_email:
            return acct
    return ACCOUNTS[0]


def _do_send(account, to_email, subject, body, dry_run=False):
    """Core send logic — sends via a specific account."""
    sender_email = account["email"]

    if dry_run:
        tag = f" [{sender_email.split('@')[0]}]" if len(ACCOUNTS) > 1 else ""
        print(f"\n{'─'*50}")
        print(f"  FROM:{tag}    {sender_email}")
        print(f"  TO:      {to_email}")
        print(f"  SUBJECT: {subject}")
        print(f"{'─'*50}")
        print(body)
        print(f"{'─'*50}\n")
        return True

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = f"{YOUR_NAME} <{sender_email}>"
        msg["To"] = to_email
        msg["Subject"] = subject

        server = _get_smtp_for(account)
        server.sendmail(sender_email, to_email, msg.as_string())
        return True
    except smtplib.SMTPRecipientsRefused:
        print(f"  [✗] Recipient refused: {to_email}")
        return False
    except smtplib.SMTPAuthenticationError:
        print(f"  [✗] SMTP auth failed for {sender_email} — check app password")
        return False
    except (smtplib.SMTPServerDisconnected, smtplib.SMTPSenderRefused) as e:
        # Connection died — reconnect and retry once
        _smtp_pool.pop(sender_email, None)
        try:
            server = _get_smtp_for(account)
            msg = MIMEText(body, "plain", "utf-8")
            msg["From"] = f"{YOUR_NAME} <{sender_email}>"
            msg["To"] = to_email
            msg["Subject"] = subject
            server.sendmail(sender_email, to_email, msg.as_string())
            return True
        except Exception as e2:
            print(f"  [✗] Send failed after reconnect: {e2}")
            return False
    except Exception as e:
        print(f"  [✗] Send failed: {e}")
        return False


def send_email(to_email, subject, body, dry_run=False):
    """Send a fresh email — rotates sender via round-robin. Returns (success, sender_email)."""
    account = _next_sender()
    ok = _do_send(account, to_email, subject, body, dry_run)
    return ok, account["email"]


def send_email_from(sender_email, to_email, subject, body, dry_run=False):
    """Send a follow-up from a specific account (the one that sent the original)."""
    account = _find_account_by_email(sender_email)
    return _do_send(account, to_email, subject, body, dry_run)


def wait_between_emails(dry_run):
    """Random delay between sends — variance keeps Gmail from flagging us."""
    if dry_run:
        return
    # Base random delay
    base = random.randint(MIN_DELAY, MAX_DELAY)
    # Add extra jitter: ±15s so the pattern is never predictable
    jitter = random.randint(-15, 15)
    delay = max(20, base + jitter)
    print(f"  ⏳ Waiting {delay}s...", end="", flush=True)
    time.sleep(delay)
    print(" go")


# ============================================================
# LOAD LEADS FROM CSV
# ============================================================

# ── Pre-send MX verification ────────────────────────────────
_mx_cache = {}

def _check_mx(email_addr):
    """Quick MX check — returns True if the email domain can receive mail."""
    domain = email_addr.split("@")[-1].lower()
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
        pass
    except Exception:
        _mx_cache[domain] = False
        return False

    # Fallback: check if domain resolves at all on port 25
    try:
        socket.getaddrinfo(domain, 25, socket.AF_INET, socket.SOCK_STREAM)
        _mx_cache[domain] = True
        return True
    except socket.gaierror:
        _mx_cache[domain] = False
        return False

def find_all_csvs():
    """Find all ai_leads CSVs, newest first."""
    csvs = []
    for f in os.listdir(SCRIPT_DIR):
        if f.startswith("ai_leads_") and f.endswith(".csv"):
            csvs.append(os.path.join(SCRIPT_DIR, f))
    csvs.sort(reverse=True)
    return csvs


def load_leads(csv_paths, conn):
    """Load fresh leads from all CSVs."""
    leads = []
    seen_domains = set()

    for csv_path in csv_paths:
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row.get("Email"):
                        continue
                    if "Email_Verified" in row and row["Email_Verified"] == "✗":
                        continue

                    try:
                        score = int(row.get("Total_Score") or row.get("Automation_Score") or 0)
                    except (ValueError, TypeError):
                        score = 0
                    if score < MIN_SCORE:
                        continue

                    domain = row.get("Domain", "")
                    if not domain or domain in seen_domains:
                        continue
                    
                    # SAFETY: Only email dental leads
                    niche_val = row.get("Niche", "").lower()
                    if "dental" not in niche_val and "dentist" not in niche_val:
                        continue
                    if already_emailed(conn, domain):
                        continue  # already in DB — never double-send

                    seen_domains.add(domain)
                    leads.append(row)
        except FileNotFoundError:
            continue

    leads.sort(key=lambda x: int(x.get("Total_Score") or x.get("Automation_Score") or 0), reverse=True)
    return leads


# ============================================================
# MAIN
# ============================================================

def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    test_email = None
    csv_override = None
    fresh_only = "--fresh-only" in args
    followups_only = "--followups-only" in args
    replied_domain = None
    show_status = "--status" in args

    for i, arg in enumerate(args):
        if arg == "--test" and i + 1 < len(args):
            test_email = args[i + 1]
        if arg == "--csv" and i + 1 < len(args):
            csv_override = args[i + 1]
        if arg == "--replied" and i + 1 < len(args):
            replied_domain = args[i + 1]

    # Handle --replied command
    if replied_domain:
        conn = init_outreach_db()
        if mark_replied(conn, replied_domain):
            print(f"  ✓ Marked {replied_domain} as replied — no more follow-ups")
        else:
            print(f"  [!] Domain not found in sent emails: {replied_domain}")
        conn.close()
        return

    # Handle --status command
    if show_status:
        conn = init_outreach_db()
        totals = get_total_sent(conn)
        stats = get_today_stats(conn)
        fu1 = get_followup_queue(conn, 1, FOLLOWUP_1_DAYS)
        fu2 = get_followup_queue(conn, 2, FOLLOWUP_2_DAYS)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM sent_emails WHERE status = 'replied'")
        replied_count = c.fetchone()[0]
        conn.close()
        print(f"\n  Campaign Stats")
        print(f"  {'─'*30}")
        print(f"  All-time fresh:    {totals['fresh']}")
        print(f"  All-time followups: {totals['followups']}")
        print(f"  Replied:           {replied_count}")
        print(f"  Today:             {stats['fresh']} fresh + {stats['followups']} FU")
        print(f"  FU #1 queue:       {len(fu1)} leads ready")
        print(f"  FU #2 queue:       {len(fu2)} leads ready\n")
        return

    print("=" * 60)
    print("  AI AUTOMATION OUTREACH v1 — Dentist Campaign Engine")
    flags = []
    if dry_run: flags.append("DRY RUN")
    if test_email: flags.append(f"TEST → {test_email}")
    if fresh_only: flags.append("FRESH ONLY")
    if followups_only: flags.append("FOLLOW-UPS ONLY")
    if flags:
        print(f"  Mode: {' | '.join(flags)}")
    print(f"  Date: {TODAY}")
    print("=" * 60)

    # Validate
    if SMTP_EMAIL == "YOUR_EMAIL@gmail.com" and not dry_run:
        print("\n  [✗] Set your SMTP_EMAIL and SMTP_APP_PASSWORD first!")
        return

    # Init
    print("\n[1/4] Database...")
    conn = init_outreach_db()
    stats = get_today_stats(conn)
    totals = get_total_sent(conn)
    print(f"  [✓] Today: {stats['fresh']} fresh + {stats['followups']} follow-ups")
    print(f"  [✓] All-time: {totals['fresh']} fresh + {totals['followups']} follow-ups")

    total_today = stats['fresh'] + stats['followups']
    if total_today >= TOTAL_DAILY_CAP and not test_email:
        print(f"\n  ⚠ Already sent {total_today} today (cap {TOTAL_DAILY_CAP}). Run tomorrow!")
        conn.close()
        return

    print("\n[2/4] Google Sheets...")
    init_outreach_sheets()

    # ─── CHECK REPLIES & BOUNCES (before ANY sending) ───
    if not test_email:
        print("\n  [i] Checking ALL inboxes for replies and bounces...")
        replied_domains = check_replies_imap(conn, dry_run=dry_run)
        bounced_domains = check_bounces_imap(conn, dry_run=dry_run)
        if replied_domains:
            print(f"  [✓] {len(replied_domains)} lead(s) replied — removed from queues")
        if bounced_domains:
            print(f"  [✓] {len(bounced_domains)} lead(s) bounced — removed from queues")
        if not replied_domains and not bounced_domains:
            print(f"  [✓] No replies or bounces detected")

    # ─── FOLLOW-UPS ───
    followups_sent = 0
    if not fresh_only and not test_email:
        print("\n[3/4] Follow-ups...")

        fu_remaining = min(FOLLOWUP_DAILY_LIMIT - stats['followups'], TOTAL_DAILY_CAP - total_today)

        if fu_remaining > 0:
            # Follow-up #1 (day 3)
            fu1_queue = get_followup_queue(conn, 1, FOLLOWUP_1_DAYS)
            # Follow-up #2 (day 7)
            fu2_queue = get_followup_queue(conn, 2, FOLLOWUP_2_DAYS)
            # Only send FU2 to leads that already got FU1
            fu2_queue = [r for r in fu2_queue if r[0] not in {r2[0] for r2 in fu1_queue}]

            print(f"  [i] Follow-up #1 queue: {len(fu1_queue)} leads (day {FOLLOWUP_1_DAYS})")
            print(f"  [i] Follow-up #2 queue: {len(fu2_queue)} leads (day {FOLLOWUP_2_DAYS})")

            # Send follow-up #2 first (older leads, more urgent)
            for domain, email, company, niche, issues_str, orig_template, orig_sender in fu2_queue:
                if followups_sent >= fu_remaining:
                    break
                name = guess_first_name(email)
                subject, body, tmpl = followup_2_template(name, domain, company, niche)

                print(f"  [FU2] {company or domain} → {email} (via {orig_sender.split('@')[0] if orig_sender else 'primary'})")
                ok = send_email_from(orig_sender or ACCOUNTS[0]['email'], email, subject, body, dry_run=dry_run)
                if ok:
                    followups_sent += 1
                    if not dry_run:
                        log_followup(conn, domain, email, 2)
                    push_to_sheets(TODAY, domain, company, email, tmpl, "follow-up #2", subject)
                    print(f"    ✓ {'Previewed' if dry_run else 'Sent'}")
                    if followups_sent < fu_remaining:
                        wait_between_emails(dry_run)

            # Send follow-up #1
            for domain, email, company, niche, issues_str, orig_template, orig_sender in fu1_queue:
                if followups_sent >= fu_remaining:
                    break
                name = guess_first_name(email)
                subject, body, tmpl = followup_1_template(name, domain, company, niche)

                print(f"  [FU1] {company or domain} → {email} (via {orig_sender.split('@')[0] if orig_sender else 'primary'})")
                ok = send_email_from(orig_sender or ACCOUNTS[0]['email'], email, subject, body, dry_run=dry_run)
                if ok:
                    followups_sent += 1
                    if not dry_run:
                        log_followup(conn, domain, email, 1)
                    push_to_sheets(TODAY, domain, company, email, tmpl, "follow-up #1", subject)
                    print(f"    ✓ {'Previewed' if dry_run else 'Sent'}")
                    if followups_sent < fu_remaining:
                        wait_between_emails(dry_run)

        print(f"  [✓] {followups_sent} follow-ups {'previewed' if dry_run else 'sent'}")
    else:
        print("\n[3/4] Follow-ups... skipped")

    # ─── FRESH EMAILS ───
    fresh_sent = 0
    if not followups_only:
        print("\n[4/4] Fresh emails...")
        fresh_remaining = min(FRESH_DAILY_LIMIT - stats['fresh'],
                             TOTAL_DAILY_CAP - total_today - followups_sent)

        if fresh_remaining <= 0:
            print(f"  [i] Fresh limit reached for today")
        else:
            # Find CSVs
            if csv_override:
                csv_paths = [csv_override if os.path.isabs(csv_override) else os.path.join(SCRIPT_DIR, csv_override)]
            else:
                csv_paths = find_all_csvs()

            if not csv_paths:
                print("  [✗] No CSV found. Run ai_leads.py first!")
            else:
                print(f"  [i] Reading {len(csv_paths)} CSV(s)")
                leads = load_leads(csv_paths, conn)

                if not leads:
                    print("  [✗] No eligible leads (all emailed, unverified, or low score)")
                    print("  [i] Run ai_leads.py to generate more!")
                else:
                    print(f"  [✓] {len(leads)} fresh leads available")

                    # Test mode
                    if test_email:
                        lead = leads[0]
                        issues = pick_top_issues(lead.get("Automation_Gaps", ""))
                        template_fn = random.choice(TEMPLATES)
                        subject, body, template_name = template_fn(lead, issues)
                        print(f"\n  [TEST] Template: {template_name}")
                        print(f"  [TEST] Subject:  {subject}")
                        print(f"  [TEST] Lead:     {lead.get('Company_Name', '')} ({lead.get('Domain', '')})")
                        ok, _ = send_email(test_email, subject, body, dry_run=dry_run)
                        if ok:
                            print(f"\n  ✓ Test email {'previewed' if dry_run else 'sent'} to {test_email}")
                        close_smtp()
                        conn.close()
                        return

                    # Send fresh emails
                    print(f"\n  Sending {min(fresh_remaining, len(leads))} fresh emails...\n")
                    template_idx = 0

                    for lead in leads:
                        if fresh_sent >= fresh_remaining:
                            break

                        domain = lead.get("Domain", "")
                        company = lead.get("Company_Name", domain)
                        email_to = lead.get("Email", "").split(";")[0].strip()
                        score = lead.get("Total_Score") or lead.get("Automation_Score") or "?"
                        niche = lead.get("Niche", "")
                        issues_str = lead.get("Automation_Gaps", "")

                        if not email_to or "@" not in email_to:
                            continue

                        # Pre-send MX check — skip bad domains before wasting a send
                        if not _check_mx(email_to):
                            print(f"  [skip] {domain} — bad MX for {email_to.split('@')[-1]}")
                            continue

                        issues = pick_top_issues(issues_str)
                        template_fn = TEMPLATES[template_idx % len(TEMPLATES)]
                        template_idx += 1
                        subject, body, template_name = template_fn(lead, issues)

                        print(f"  [{fresh_sent + 1}/{fresh_remaining}] {company} ({domain}) → {email_to}")
                        print(f"    Template: {template_name} | Score: {score}")

                        ok, sender_used = send_email(email_to, subject, body, dry_run=dry_run)
                        if ok:
                            fresh_sent += 1
                            if not dry_run:
                                log_sent(conn, domain, email_to, template_name, subject,
                                         company=company, niche=niche, issues=issues_str,
                                         sender_account=sender_used)
                            push_to_sheets(TODAY, domain, company, email_to, template_name, "fresh", subject)
                            print(f"    ✓ {'Previewed' if dry_run else 'Sent'}")
                            if fresh_sent < fresh_remaining:
                                wait_between_emails(dry_run)
                        else:
                            print(f"    ✗ Failed")
    else:
        print("\n[4/4] Fresh emails... skipped")

    # Cleanup
    close_smtp()
    conn.close()

    total_sent = fresh_sent + followups_sent
    print(f"\n{'='*60}")
    print(f"  OUTREACH RESULTS — {TODAY}")
    print(f"  {'─'*40}")
    label = "Previewed" if dry_run else "Sent"
    print(f"  Fresh {label.lower()}:     {fresh_sent}")
    print(f"  Follow-ups {label.lower()}: {followups_sent}")
    print(f"  Total:          {total_sent}")
    print(f"  Today total:    {total_today + total_sent}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
