
import sys
import os

# Add script dir to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ai_outreach import template_quick_audit, template_competitor_angle, template_helpful_tip

# Mock lead data
lead = {
    "Company_Name": "Smile Bright Dental",
    "Domain": "smilebright.com",
    "Niche": "Dental",
    "Email": "info@smilebright.com",
    "City": "Austin"
}

issues = ["Missing title tag", "Slow load time"]

print("--- Quick Audit (AI Automation) ---")
subj, body, _ = template_quick_audit(lead, issues)
print(f"Subject: {subj}")
print(body)
print("\n")

print("--- Competitor Angle (AI Automation) ---")
subj, body, _ = template_competitor_angle(lead, issues)
print(f"Subject: {subj}")
print(body)
print("\n")

print("--- Helpful Tip (AI Automation) ---")
subj, body, _ = template_helpful_tip(lead, issues)
print(f"Subject: {subj}")
print(body)
print("\n")
