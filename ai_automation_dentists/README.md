# AI Automation Lead Generator for Dental Practices

Find dental practices that still rely on manual workflows — phone-only booking, paper forms, no review automation — and reach out with personalized emails offering AI automation services.

## How It Works

1. **`ai_leads.py`** — Searches for dental practices via Brave Search, audits their websites for automation gaps (no online booking, no chatbot, no review system, etc.), scores them, and exports qualified leads to CSV + Google Sheets.

2. **`ai_outreach.py`** — Loads leads from CSV, sends personalized cold emails with automatic follow-ups, tracks everything in a local SQLite database, and detects replies/bounces via IMAP.

## Setup

### 1. Install Dependencies

```bash
pip install requests beautifulsoup4 gspread dnspython
```

### 2. Configure `ai_leads.py`

Open `ai_leads.py` and fill in these values at the top of the file:

| Variable | Where to Get It |
|---|---|
| `BRAVE_API_KEY` | [Brave Search API](https://brave.com/search/api/) — paid plan required |
| `GOOGLE_CREDS_FILE` | Path to your Google service account JSON (for Sheets sync) |
| `GOOGLE_SHEET_URL` | URL of your Google Sheet (share it with the service account email) |

> **Google Sheets is optional.** If you leave `GOOGLE_SHEET_URL` empty, leads will still export to CSV — Sheets sync is just skipped.

### 3. Configure `ai_outreach.py`

Open `ai_outreach.py` and fill in these values:

| Variable | What to Enter |
|---|---|
| `YOUR_NAME` | Your name (used in email signature) |
| `ACCOUNTS[0]["email"]` | Your Gmail address |
| `ACCOUNTS[0]["password"]` | Gmail App Password (not your regular password) |
| `GOOGLE_SHEET_URL` | Same Sheet URL as above (optional, for outreach tracking) |

#### Getting a Gmail App Password

1. Go to [Google Account Security](https://myaccount.google.com/security)
2. Enable **2-Step Verification** if not already on
3. Go to **App Passwords** → Generate one for "Mail"
4. Paste the 16-character password into the `ACCOUNTS` config

#### Adding More Sender Accounts

To scale outreach, add more Gmail accounts to the `ACCOUNTS` list:

```python
ACCOUNTS = [
    {"email": "you@gmail.com", "password": "xxxx xxxx xxxx xxxx", "created": "2026-01-01"},
    {"email": "you2@gmail.com", "password": "yyyy yyyy yyyy yyyy", "created": "2026-02-01"},
]
```

Emails are sent round-robin across all accounts. New accounts auto-warmup (start with fewer sends per day).

## Usage

### Generate Leads

```bash
python ai_leads.py
```

Searches for dental practices, audits each site, and saves qualified leads to `ai_leads_YYYY-MM-DD.csv`.

### Send Emails

```bash
# Dry run first (prints emails, doesn't send)
python ai_outreach.py --dry-run

# Send for real
python ai_outreach.py
```

### Test Email Templates

```bash
python ai_outreach.py --test your@email.com
```

Sends one test email to yourself so you can preview how it looks.

## What the Audit Detects

The lead scorer looks for **automation gaps** — things the practice is missing:

| Gap | Weight | What It Means |
|---|---|---|
| No online booking | 4 | Patients have to call to schedule |
| No chatbot / live chat | 3 | No after-hours patient communication |
| No automated review system | 3 | Not collecting Google reviews automatically |
| Phone-only booking | 3 | "Call to schedule" with no online option |
| Paper intake forms | 3 | "Download and print" forms |
| No patient portal | 2 | No online access for patients |
| No SMS / text messaging | 2 | No text reminders or communication |
| No email marketing | 2 | No automated patient follow-ups |

Higher score = more manual processes = better lead for AI automation services.

## Output

Each lead in the CSV includes:

- Company name, domain, niche
- Email (MX-verified), phone number, contact page URL
- **Automation Score** — how much they need automation (0-100)
- **Automation Gaps** — specific things they're missing
- Business fit score, budget signals, contact quality score
- Overall lead tier (A / B / C)

## File Structure

```
ai_automation_dentists/
├── ai_leads.py          # Lead generation + website auditing
├── ai_outreach.py       # Email outreach + follow-ups
├── test_templates.py    # Preview email templates
└── README.md
```

## Notes

- The scripts create local SQLite databases (`ai_leads.db`, `ai_outreach.db`) to track seen domains and sent emails — this prevents duplicates across runs.
- Daily lead targets and sending limits are configurable at the top of each file.
- Follow-up emails are sent from the **same account** that sent the original (for thread consistency).
- Reply and bounce detection runs automatically before each outreach batch.
