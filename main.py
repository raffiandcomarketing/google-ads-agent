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
from datetime import datetime, timedelta

import anthropic
from google.ads.googleads.client import GoogleAdsClient
from supabase import create_client, Client

# Config
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

GOOGLE_ADS_CONFIG = {
    "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
    "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
    "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
    "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
    "login_customer_id": os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"),
    "use_proto_plus": True,
}

CUSTOMER_ID = os.environ["GOOGLE_ADS_CUSTOMER_ID"]
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))


def fetch_campaign_data(client):
    ga_service = client.get_service("GoogleAdsService")
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            campaign.id, campaign.name, campaign.status,
            campaign.advertising_channel_type, campaign_budget.amount_micros,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value,
            metrics.ctr, metrics.average_cpc, metrics.cost_per_conversion
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
            "id": str(row.campaign.id), "name": row.campaign.name,
            "status": row.campaign.status.name, "channel": row.campaign.advertising_channel_type.name,
            "daily_budget": round(budget, 2), "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks, "cost": round(cost, 2),
            "conversions": round(row.metrics.conversions, 2),
            "conversion_value": round(row.metrics.conversions_value, 2),
            "ctr": round(row.metrics.ctr * 100, 2), "avg_cpc": round(avg_cpc, 2),
            "cost_per_conversion": round(conv_cost, 2) if conv_cost else None,
        })
    return campaigns


def fetch_keyword_data(client):
    ga_service = client.get_service("GoogleAdsService")
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            campaign.name, ad_group.name, ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.ctr, metrics.average_cpc
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
            "campaign": row.campaign.name, "ad_group": row.ad_group.name,
            "keyword": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "quality_score": row.ad_group_criterion.quality_info.quality_score or None,
            "impressions": row.metrics.impressions, "clicks": row.metrics.clicks,
            "cost": round(cost, 2), "conversions": round(row.metrics.conversions, 2),
            "ctr": round(row.metrics.ctr * 100, 2), "avg_cpc": round(avg_cpc, 2),
        })
    return keywords


def analyze_with_claude(campaigns, keywords):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    total_spend = sum(c["cost"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    total_value = sum(c["conversion_value"] for c in campaigns)

    prompt = f"""You are an expert Google Ads optimization consultant. Analyze the following
campaign and keyword performance data from the last {LOOKBACK_DAYS} days and provide
specific, actionable optimization recommendations.

## Account Summary
- Total Spend: ${total_spend:,.2f}
- Total Conversions: {total_conversions:,.1f}
- Total Conversion Value: ${total_value:,.2f}
- Overall ROAS: {(total_value / total_spend):.2f}x if total_spend > 0 else 'N/A'
- Period: Last {LOOKBACK_DAYS} days

## Campaign Performance
{json.dumps(campaigns, indent=2)}

## Top 100 Keywords by Spend
{json.dumps(keywords, indent=2)}

Respond in JSON with this structure:
{{
    "summary": "2-3 sentence executive summary",
    "health_score": 1-10,
    "urgent_actions": [{{"action": "desc", "impact": "high/medium/low", "campaign": "name", "details": "specifics"}}],
    "bid_optimizations": [{{"keyword": "text", "campaign": "name", "current_cpc": 0.00, "suggested_action": "desc"}}],
    "budget_changes": [{{"campaign": "name", "current_budget": 0.00, "suggested_budget": 0.00, "reason": "why"}}],
    "keyword_actions": [{{"keyword": "text", "campaign": "name", "action": "pause/increase_bid/add_negative/change_match", "reason": "why"}}],
    "testing_ideas": [{{"ad_group": "name", "idea": "desc"}}],
    "estimated_monthly_savings": 0.00,
    "estimated_monthly_revenue_gain": 0.00
}}
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text
    if "```json" in response_text:
        response_text = response_text.split("```json")[1].split("```")[0]
    elif "```" in response_text:
        response_text = response_text.split("```")[1].split("```")[0]
    return json.loads(response_text.strip())


def store_results(supabase, analysis, campaigns, keywords):
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
    supabase.table("optimization_runs").insert(record).execute()
    print(f"Stored analysis in Supabase (health score: {analysis.get('health_score')}/10)")


def main():
    print(f"Google Ads Optimization Agent - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Analyzing last {LOOKBACK_DAYS} days of data")

    print("Connecting to Google Ads...")
    google_client = GoogleAdsClient.load_from_dict(GOOGLE_ADS_CONFIG)

    print("Connecting to Supabase...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Fetching campaign data...")
    campaigns = fetch_campaign_data(google_client)
    print(f"Found {len(campaigns)} active campaigns")

    print("Fetching keyword data...")
    keywords = fetch_keyword_data(google_client)
    print(f"Found {len(keywords)} keywords with impressions")

    if not campaigns:
        print("No campaign data found. Check your customer ID and date range.")
        sys.exit(1)

    print("Analyzing with Claude...")
    analysis = analyze_with_claude(campaigns, keywords)
    print(f"Health score: {analysis.get('health_score')}/10")
    print(f"Summary: {analysis.get('summary')}")

    print("Storing results...")
    store_results(supabase, analysis, campaigns, keywords)

    print(f"Analysis complete! {len(analysis.get('urgent_actions', []))} urgent actions found.")


if __name__ == "__main__":
    main()
