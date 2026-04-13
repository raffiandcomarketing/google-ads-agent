"""
Google Ads Overnight Optimization Agent
========================================
Pulls campaign data from Google Ads, analyzes with Claude,
stores recommendations in Supabase.

Deploy on Railway with a cron schedule.
"""

import os
import json
import sys
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

import anthropic
from google.ads.googleads.client import GoogleAdsClient
from supabase import create_client, Client

# ─── Config ───────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Google Ads credentials (all from env vars)
GOOGLE_ADS_CONFIG = {
    "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
    "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
    "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
    "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
    "login_customer_id": os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"),
    "use_proto_plus": True,
}

CUSTOMER_ID = os.environ["GOOGLE_ADS_CUSTOMER_ID"]

# Email config (SMTP)
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER)
EMAIL_TO = os.environ.get("EMAIL_TO", "al@raffiandco.com")

# Spend guardrail — total daily budget must not exceed this
MAX_DAILY_SPEND = float(os.environ.get("MAX_DAILY_SPEND", "3000"))

# How many days of data to analyze
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

# Limit data sent to Claude to keep prompt size manageable
MAX_CAMPAIGNS = int(os.environ.get("MAX_CAMPAIGNS", "25"))
MAX_KEYWORDS = int(os.environ.get("MAX_KEYWORDS", "50"))


# ─── Debug Logging to Supabase ─────────────────────────────────
_debug_supabase = None

def _get_debug_client():
    """Lazy-init a Supabase client for debug logging."""
    global _debug_supabase
    if _debug_supabase is None:
        _debug_supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _debug_supabase

def log_step(step: str, message: str = None, error: str = None):
    """Write a debug log row to Supabase so we can trace execution."""
    try:
        client = _get_debug_client()
        record = {"step": step}
        if message:
            record["message"] = message[:2000]  # truncate long messages
        if error:
            record["error"] = error[:2000]
        client.table("debug_logs").insert(record).execute()
        print(f"  [LOG] {step}: {message or ''} {('ERROR: ' + error) if error else ''}", flush=True)
    except Exception as e:
        # If debug logging itself fails, just print — don't crash the main flow
        print(f"  [LOG FAILED] {step}: {e}", flush=True)


# ─── Google Ads: Pull Campaign Data ──────────────────────────────
def fetch_campaign_data(client: GoogleAdsClient) -> list[dict]:
    """Fetch campaign performance for the last N days."""
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign_budget.amount_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_per_conversion
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """

    campaigns = []
    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    for row in response:
        cost = row.metrics.cost_micros / 1_000_000
        budget = row.campaign_budget.amount_micros / 1_000_000
        avg_cpc = row.metrics.average_cpc / 1_000_000
        conv_cost = row.metrics.cost_per_conversion / 1_000_000 if row.metrics.cost_per_conversion else None

        campaigns.append({
            "id": str(row.campaign.id),
            "name": row.campaign.name,
            "status": row.campaign.status.name,
            "channel": row.campaign.advertising_channel_type.name,
            "daily_budget": round(budget, 2),
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": round(cost, 2),
            "conversions": round(row.metrics.conversions, 2),
            "conversion_value": round(row.metrics.conversions_value, 2),
            "ctr": round(row.metrics.ctr * 100, 2),
            "avg_cpc": round(avg_cpc, 2),
            "cost_per_conversion": round(conv_cost, 2) if conv_cost else None,
        })

    return campaigns


def fetch_keyword_data(client: GoogleAdsClient) -> list[dict]:
    """Fetch keyword-level performance for deeper analysis."""
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr,
            metrics.average_cpc
        FROM keyword_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND metrics.impressions > 0
        ORDER BY metrics.cost_micros DESC
        LIMIT 100
    """

    keywords = []
    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    for row in response:
        cost = row.metrics.cost_micros / 1_000_000
        avg_cpc = row.metrics.average_cpc / 1_000_000

        keywords.append({
            "campaign": row.campaign.name,
            "ad_group": row.ad_group.name,
            "keyword": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "quality_score": row.ad_group_criterion.quality_info.quality_score or None,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": round(cost, 2),
            "conversions": round(row.metrics.conversions, 2),
            "ctr": round(row.metrics.ctr * 100, 2),
            "avg_cpc": round(avg_cpc, 2),
        })

    return keywords


# ─── Claude: Analyze & Recommend ─────────────────────────────────
def analyze_with_claude(campaigns: list[dict], keywords: list[dict]) -> dict:
    """Send performance data to Claude for optimization analysis."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Limit data size to keep prompt manageable
    top_campaigns = campaigns[:MAX_CAMPAIGNS]
    top_keywords = keywords[:MAX_KEYWORDS]

    total_spend = sum(c["cost"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    total_value = sum(c["conversion_value"] for c in campaigns)
    roas_str = f"{(total_value / total_spend):.2f}x" if total_spend > 0 else "N/A"

    prompt = f"""You are an expert Google Ads optimization consultant. Analyze the following
campaign and keyword performance data from the last {LOOKBACK_DAYS} days and provide
specific, actionable optimization recommendations.

## Account Summary
- Total Spend: ${total_spend:,.2f}
- Total Conversions: {total_conversions:,.1f}
- Total Conversion Value: ${total_value:,.2f}
- Overall ROAS: {roas_str}
- Period: Last {LOOKBACK_DAYS} days
- Showing top {len(top_campaigns)} of {len(campaigns)} campaigns, top {len(top_keywords)} of {len(keywords)} keywords

## Campaign Performance (Top {len(top_campaigns)} by Spend)
{json.dumps(top_campaigns, indent=2)}

## Top Keywords by Spend
{json.dumps(top_keywords, indent=2)}

## HARD CONSTRAINT — DAILY SPEND CAP
The TOTAL daily budget across ALL campaigns must NEVER exceed ${MAX_DAILY_SPEND:,.0f}.
If your budget recommendations would push total daily spend above this limit, scale them
down proportionally. Flag this in your summary if the cap is binding.
Current total daily budgets: ${sum(c['daily_budget'] for c in top_campaigns):,.2f}

## Your Analysis Should Include:

1. **URGENT ACTIONS** (do today)
   - Campaigns or keywords bleeding money with no conversions
   - Budget pacing issues (overspending or underspending)
   - Any anomalies or sudden performance drops

2. **BID OPTIMIZATIONS**
   - Keywords where CPC is too high relative to conversion value
   - Keywords with high CTR but low conversions (landing page issue?)
   - Keywords with low quality scores that need attention

3. **BUDGET REALLOCATION**
   - Which campaigns deserve more budget (high ROAS, capped by budget)
   - Which campaigns should be scaled back
   - Specific dollar amounts to shift

4. **KEYWORD RECOMMENDATIONS**
   - Keywords to pause (high spend, no conversions)
   - Keywords to increase bids on (converting well, could scale)
   - Negative keyword suggestions based on patterns
   - Match type changes to consider

5. **AD COPY & TESTING IDEAS**
   - Which ad groups might benefit from new ad copy tests
   - Messaging angles based on what's converting

Respond in JSON with this structure:
{{
    "summary": "2-3 sentence executive summary",
    "health_score": 1-10,
    "urgent_actions": [
        {{"action": "description", "impact": "high/medium/low", "campaign": "name", "details": "specifics"}}
    ],
    "bid_optimizations": [
        {{"keyword": "text", "campaign": "name", "current_cpc": 0.00, "suggested_action": "description"}}
    ],
    "budget_changes": [
        {{"campaign": "name", "current_budget": 0.00, "suggested_budget": 0.00, "reason": "why"}}
    ],
    "keyword_actions": [
        {{"keyword": "text", "campaign": "name", "action": "pause/increase_bid/add_negative/change_match", "reason": "why"}}
    ],
    "testing_ideas": [
        {{"ad_group": "name", "idea": "description"}}
    ],
    "estimated_monthly_savings": 0.00,
    "estimated_monthly_revenue_gain": 0.00
}}
"""

    print(f"  Prompt length: ~{len(prompt)} chars")
    print(f"  Sending to Claude (model: claude-sonnet-4-20250514)...")
    sys.stdout.flush()

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        print(f"\nClaude API error: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

    # Parse the JSON response
    response_text = message.content[0].text
    print(f"  Response length: {len(response_text)} chars", flush=True)

    # Extract JSON from the response (handle markdown code blocks)
    if "```json" in response_text:
        response_text = response_text.split("```json")[1].split("```")[0]
    elif "```" in response_text:
        response_text = response_text.split("```")[1].split("```")[0]

    try:
        return json.loads(response_text.strip())
    except json.JSONDecodeError as e:
        print(f"\nJSON PARSE ERROR: {e}", file=sys.stderr, flush=True)
        print(f"Response text (first 500 chars): {response_text[:500]}", file=sys.stderr, flush=True)
        raise


# ─── Guardrail: Enforce Spend Cap ────────────────────────────────
def enforce_spend_guardrail(analysis: dict, campaigns: list[dict]) -> dict:
    """Validate and cap budget recommendations so total daily spend never exceeds MAX_DAILY_SPEND."""
    budget_changes = analysis.get("budget_changes", [])
    if not budget_changes:
        return analysis

    # Build a map of current budgets
    current_budgets = {c["name"]: c["daily_budget"] for c in campaigns}
    total_current = sum(current_budgets.values())

    # Calculate what the total would be after applying suggested changes
    suggested_budgets = dict(current_budgets)
    for change in budget_changes:
        name = change.get("campaign", "")
        suggested = change.get("suggested_budget", 0)
        if name in suggested_budgets:
            suggested_budgets[name] = suggested

    total_suggested = sum(suggested_budgets.values())

    if total_suggested > MAX_DAILY_SPEND:
        overage = total_suggested - MAX_DAILY_SPEND
        print(f"  GUARDRAIL: Suggested budgets total ${total_suggested:,.2f}/day — exceeds cap of ${MAX_DAILY_SPEND:,.0f} by ${overage:,.2f}", flush=True)
        log_step("guardrail_triggered", f"Budget suggestions totaled ${total_suggested:,.2f}/day, cap is ${MAX_DAILY_SPEND:,.0f}. Scaling down.")

        # Scale down suggested budgets proportionally to fit within the cap
        scale_factor = MAX_DAILY_SPEND / total_suggested
        for change in budget_changes:
            name = change.get("campaign", "")
            if name in suggested_budgets:
                original_suggestion = change["suggested_budget"]
                change["suggested_budget"] = round(original_suggestion * scale_factor, 2)
                change["reason"] = f"[CAPPED — scaled to stay within ${MAX_DAILY_SPEND:,.0f}/day limit] {change.get('reason', '')}"

        # Add a warning to summary
        analysis["summary"] = f"⚠️ Budget recommendations were scaled down to stay within the ${MAX_DAILY_SPEND:,.0f}/day cap. " + analysis.get("summary", "")
        analysis["budget_changes"] = budget_changes

        new_total = sum(
            next((ch["suggested_budget"] for ch in budget_changes if ch.get("campaign") == name), budget)
            for name, budget in current_budgets.items()
        )
        print(f"  GUARDRAIL: Adjusted total daily budget: ${new_total:,.2f}", flush=True)
    else:
        print(f"  Guardrail OK: suggested total ${total_suggested:,.2f}/day within ${MAX_DAILY_SPEND:,.0f} cap", flush=True)
        log_step("guardrail_ok", f"Suggested total ${total_suggested:,.2f}/day within ${MAX_DAILY_SPEND:,.0f} cap")

    return analysis


# ─── Supabase: Store Results ─────────────────────────────────────
def store_results(supabase: Client, analysis: dict, campaigns: list[dict], keywords: list[dict]):
    """Save the analysis and raw data to Supabase."""

    total_spend = sum(c["cost"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    total_value = sum(c["conversion_value"] for c in campaigns)

    record = {
        "run_date": datetime.now().isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "total_spend": total_spend,
        "total_conversions": total_conversions,
        "total_conversion_value": total_value,
        "roas": round(total_value / total_spend, 2) if total_spend > 0 else 0,
        "health_score": analysis.get("health_score"),
        "summary": analysis.get("summary"),
        "urgent_actions": json.dumps(analysis.get("urgent_actions", [])),
        "bid_optimizations": json.dumps(analysis.get("bid_optimizations", [])),
        "budget_changes": json.dumps(analysis.get("budget_changes", [])),
        "keyword_actions": json.dumps(analysis.get("keyword_actions", [])),
        "testing_ideas": json.dumps(analysis.get("testing_ideas", [])),
        "estimated_monthly_savings": analysis.get("estimated_monthly_savings", 0),
        "estimated_monthly_revenue_gain": analysis.get("estimated_monthly_revenue_gain", 0),
        "raw_campaign_data": json.dumps(campaigns),
        "raw_keyword_data": json.dumps(keywords),
    }

    try:
        result = supabase.table("optimization_runs").insert(record).execute()
        print(f"  Stored analysis in Supabase (health score: {analysis.get('health_score')}/10)")
    except Exception as e:
        print(f"\nSUPABASE INSERT ERROR: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        print(f"Record keys: {list(record.keys())}", file=sys.stderr, flush=True)
        raise


# ─── Email Report ────────────────────────────────────────────────
def send_email_report(analysis: dict, campaigns: list[dict], keywords: list[dict]):
    """Send a formatted HTML email report of the optimization results."""
    if not SMTP_USER or not SMTP_PASSWORD:
        print("  Skipping email: SMTP_USER or SMTP_PASSWORD not configured", flush=True)
        log_step("email_skip", "SMTP credentials not configured, skipping email")
        return

    total_spend = sum(c["cost"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    total_value = sum(c["conversion_value"] for c in campaigns)
    roas = round(total_value / total_spend, 2) if total_spend > 0 else 0
    health = analysis.get("health_score", "?")
    savings = analysis.get("estimated_monthly_savings", 0)
    revenue_gain = analysis.get("estimated_monthly_revenue_gain", 0)

    # Build urgent actions HTML
    urgent_html = ""
    for a in analysis.get("urgent_actions", []):
        impact_color = {"high": "#e74c3c", "medium": "#f39c12", "low": "#27ae60"}.get(
            a.get("impact", "").lower(), "#888"
        )
        urgent_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #eee;"><span style="color:{impact_color};font-weight:bold;">{a.get('impact','').upper()}</span></td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{a.get('campaign','')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{a.get('action','')}</td>
        </tr>"""

    # Build budget changes HTML
    budget_html = ""
    for b in analysis.get("budget_changes", []):
        budget_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #eee;">{b.get('campaign','')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">${b.get('current_budget',0):,.2f}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">${b.get('suggested_budget',0):,.2f}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{b.get('reason','')}</td>
        </tr>"""

    # Build keyword actions HTML
    keyword_html = ""
    for k in analysis.get("keyword_actions", []):
        keyword_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #eee;">{k.get('keyword','')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{k.get('campaign','')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{k.get('action','')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;">{k.get('reason','')}</td>
        </tr>"""

    # Health score color
    if health >= 8:
        health_color = "#27ae60"
    elif health >= 5:
        health_color = "#f39c12"
    else:
        health_color = "#e74c3c"

    today = datetime.now().strftime("%B %d, %Y")

    html = f"""
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;max-width:700px;margin:0 auto;padding:20px;">
        <div style="background:#1a1a2e;color:#fff;padding:20px 30px;border-radius:8px 8px 0 0;">
            <h1 style="margin:0;font-size:22px;">Google Ads Daily Report</h1>
            <p style="margin:5px 0 0;opacity:0.8;">{today} &mdash; Last {LOOKBACK_DAYS} days</p>
        </div>

        <div style="background:#f8f9fa;padding:20px 30px;border:1px solid #e0e0e0;">
            <h2 style="margin-top:0;font-size:16px;color:#555;">Summary</h2>
            <p style="font-size:15px;line-height:1.5;">{analysis.get('summary','No summary available.')}</p>

            <div style="display:flex;gap:15px;flex-wrap:wrap;margin:15px 0;">
                <div style="background:#fff;padding:12px 18px;border-radius:6px;border:1px solid #ddd;flex:1;min-width:120px;text-align:center;">
                    <div style="font-size:12px;color:#888;">Health Score</div>
                    <div style="font-size:28px;font-weight:bold;color:{health_color};">{health}/10</div>
                </div>
                <div style="background:#fff;padding:12px 18px;border-radius:6px;border:1px solid #ddd;flex:1;min-width:120px;text-align:center;">
                    <div style="font-size:12px;color:#888;">Total Spend</div>
                    <div style="font-size:28px;font-weight:bold;">${total_spend:,.0f}</div>
                </div>
                <div style="background:#fff;padding:12px 18px;border-radius:6px;border:1px solid #ddd;flex:1;min-width:120px;text-align:center;">
                    <div style="font-size:12px;color:#888;">ROAS</div>
                    <div style="font-size:28px;font-weight:bold;">{roas}x</div>
                </div>
                <div style="background:#fff;padding:12px 18px;border-radius:6px;border:1px solid #ddd;flex:1;min-width:120px;text-align:center;">
                    <div style="font-size:12px;color:#888;">Conversions</div>
                    <div style="font-size:28px;font-weight:bold;">{total_conversions:,.0f}</div>
                </div>
            </div>

            <div style="display:flex;gap:15px;margin:10px 0 20px;">
                <div style="background:#e8f5e9;padding:10px 16px;border-radius:6px;flex:1;text-align:center;">
                    <div style="font-size:11px;color:#2e7d32;">Est. Monthly Savings</div>
                    <div style="font-size:20px;font-weight:bold;color:#2e7d32;">${savings:,.2f}</div>
                </div>
                <div style="background:#e3f2fd;padding:10px 16px;border-radius:6px;flex:1;text-align:center;">
                    <div style="font-size:11px;color:#1565c0;">Est. Revenue Gain</div>
                    <div style="font-size:20px;font-weight:bold;color:#1565c0;">${revenue_gain:,.2f}</div>
                </div>
            </div>
        </div>

        {"<div style='background:#fff;padding:20px 30px;border:1px solid #e0e0e0;border-top:none;'><h2 style='margin-top:0;font-size:16px;color:#e74c3c;'>Urgent Actions</h2><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr style=\"background:#fafafa;\"><th style=\"padding:8px;text-align:left;\">Impact</th><th style=\"padding:8px;text-align:left;\">Campaign</th><th style=\"padding:8px;text-align:left;\">Action</th></tr>" + urgent_html + "</table></div>" if urgent_html else ""}

        {"<div style='background:#fff;padding:20px 30px;border:1px solid #e0e0e0;border-top:none;'><h2 style='margin-top:0;font-size:16px;color:#1a1a2e;'>Budget Changes</h2><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr style=\"background:#fafafa;\"><th style=\"padding:8px;text-align:left;\">Campaign</th><th style=\"padding:8px;text-align:left;\">Current</th><th style=\"padding:8px;text-align:left;\">Suggested</th><th style=\"padding:8px;text-align:left;\">Reason</th></tr>" + budget_html + "</table></div>" if budget_html else ""}

        {"<div style='background:#fff;padding:20px 30px;border:1px solid #e0e0e0;border-top:none;'><h2 style='margin-top:0;font-size:16px;color:#1a1a2e;'>Keyword Actions</h2><table style=\"width:100%;border-collapse:collapse;font-size:14px;\"><tr style=\"background:#fafafa;\"><th style=\"padding:8px;text-align:left;\">Keyword</th><th style=\"padding:8px;text-align:left;\">Campaign</th><th style=\"padding:8px;text-align:left;\">Action</th><th style=\"padding:8px;text-align:left;\">Reason</th></tr>" + keyword_html + "</table></div>" if keyword_html else ""}

        <div style="background:#f8f9fa;padding:15px 30px;border:1px solid #e0e0e0;border-top:none;border-radius:0 0 8px 8px;text-align:center;">
            <p style="font-size:12px;color:#999;margin:0;">Generated by Google Ads Optimization Agent &bull; {len(campaigns)} campaigns &bull; {len(keywords)} keywords analyzed</p>
        </div>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Google Ads Report — {today} — Health: {health}/10"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    # Plain text fallback
    plain = f"""Google Ads Daily Report — {today}

Health Score: {health}/10
Total Spend: ${total_spend:,.2f} | ROAS: {roas}x | Conversions: {total_conversions:,.0f}

Summary: {analysis.get('summary','')}

Est. Monthly Savings: ${savings:,.2f}
Est. Revenue Gain: ${revenue_gain:,.2f}

Urgent Actions: {len(analysis.get('urgent_actions', []))}
Budget Changes: {len(analysis.get('budget_changes', []))}
Keyword Actions: {len(analysis.get('keyword_actions', []))}

— Google Ads Optimization Agent
"""

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())

    print(f"  Email sent to {EMAIL_TO}", flush=True)


# ─── Main ─────────────────────────────────────────────────────────
def main():
    print(f"Google Ads Optimization Agent - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   Analyzing last {LOOKBACK_DAYS} days of data\n")

    log_step("start", f"Agent starting, lookback={LOOKBACK_DAYS} days")

    # 1. Initialize clients
    try:
        print("-> Connecting to Google Ads...")
        google_client = GoogleAdsClient.load_from_dict(GOOGLE_ADS_CONFIG)
        log_step("google_ads_connect", "Connected to Google Ads API")
    except Exception as e:
        log_step("google_ads_connect", error=f"{type(e).__name__}: {e}")
        raise

    try:
        print("-> Connecting to Supabase...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        log_step("supabase_connect", "Connected to Supabase")
    except Exception as e:
        log_step("supabase_connect", error=f"{type(e).__name__}: {e}")
        raise

    # 2. Fetch data
    try:
        print("-> Fetching campaign data...")
        campaigns = fetch_campaign_data(google_client)
        print(f"  Found {len(campaigns)} active campaigns")
        log_step("fetch_campaigns", f"Found {len(campaigns)} campaigns")
    except Exception as e:
        log_step("fetch_campaigns", error=f"{type(e).__name__}: {e}")
        raise

    try:
        print("-> Fetching keyword data...")
        keywords = fetch_keyword_data(google_client)
        print(f"  Found {len(keywords)} keywords with impressions")
        log_step("fetch_keywords", f"Found {len(keywords)} keywords")
    except Exception as e:
        log_step("fetch_keywords", error=f"{type(e).__name__}: {e}")
        raise

    if not campaigns:
        log_step("no_data", error="No campaign data found")
        print("WARNING: No campaign data found. Check your customer ID and date range.")
        sys.exit(1)

    # 3. Analyze with Claude
    try:
        print("-> Analyzing with Claude...")
        log_step("claude_start", f"Sending {len(campaigns)} campaigns, {len(keywords)} keywords to Claude")
        analysis = analyze_with_claude(campaigns, keywords)
        print(f"  Health score: {analysis.get('health_score')}/10")
        print(f"  Urgent actions: {len(analysis.get('urgent_actions', []))}")
        print(f"  Summary: {analysis.get('summary')}")
        log_step("claude_done", f"Health score: {analysis.get('health_score')}/10, {len(analysis.get('urgent_actions', []))} urgent actions")
    except Exception as e:
        log_step("claude_analysis", error=f"{type(e).__name__}: {e}")
        raise

    # 4. Enforce spend guardrail
    try:
        print(f"-> Enforcing ${MAX_DAILY_SPEND:,.0f}/day spend guardrail...")
        analysis = enforce_spend_guardrail(analysis, campaigns)
    except Exception as e:
        log_step("guardrail", error=f"{type(e).__name__}: {e}")
        print(f"  WARNING: Guardrail check failed: {e}", flush=True)
        # Don't raise — still store results even if guardrail check errors

    # 5. Store results
    try:
        print("-> Storing results...")
        store_results(supabase, analysis, campaigns, keywords)
        log_step("store_results", "Successfully stored in optimization_runs")
    except Exception as e:
        log_step("store_results", error=f"{type(e).__name__}: {e}")
        raise

    # 6. Send email report
    try:
        print("-> Sending email report...")
        send_email_report(analysis, campaigns, keywords)
        log_step("email_sent", f"Report emailed to {EMAIL_TO}")
    except Exception as e:
        log_step("email_send", error=f"{type(e).__name__}: {e}")
        print(f"  WARNING: Email failed but analysis was stored successfully: {e}", flush=True)
        # Don't raise — email failure shouldn't crash the pipeline

    # 7. Done
    log_step("complete", f"Done! Savings: ${analysis.get('estimated_monthly_savings', 0):,.2f}, Revenue gain: ${analysis.get('estimated_monthly_revenue_gain', 0):,.2f}")
    print(f"\nAnalysis complete! {len(analysis.get('urgent_actions', []))} urgent actions found.")
    print(f"   Est. monthly savings: ${analysis.get('estimated_monthly_savings', 0):,.2f}")
    print(f"   Est. monthly revenue gain: ${analysis.get('estimated_monthly_revenue_gain', 0):,.2f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n{'='*60}", file=sys.stderr, flush=True)
        print(f"FATAL ERROR: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr, flush=True)
        sys.exit(1)
