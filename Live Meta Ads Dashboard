"""
Meta Ads Data Fetcher - October 1, 2025 to NOW + Continuous Updates
Strategy: 
1. Fetch historical data: Oct 1, 2025 to Yesterday (one-time or when needed)
2. Fetch today's data: Updates every 15 minutes
3. Combine for complete dataset
"""

import os
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import logging
import json

# ============================================
# CONFIGURATION
# ============================================
class Config:
    ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "")
    
    # UPDATE WITH YOUR AD ACCOUNT IDS
    AD_ACCOUNT_IDS = [
        "act_1820431671907314",
        "act_24539675529051798"
    ]
    
    # IMPORTANT: Your historical start date
    HISTORICAL_START_DATE = "2025-10-01"  # October 1, 2025
    
    API_VERSION = "v21.0"
    BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
    IST = timezone(timedelta(hours=5, minutes=30))
    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s IST] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# ENHANCED META API CLIENT
# ============================================
class MetaAdsAPI:
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = Config.BASE_URL
    
    def _make_request(self, url, params):
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY)
                else:
                    raise
        return None
    
    def fetch_custom_date_range(self, account_id, since_date, until_date, level='campaign'):
        """
        Fetch data for CUSTOM date range (YOUR SPECIFIC DATES)
        
        Args:
            account_id: Meta ad account ID
            since_date: Start date 'YYYY-MM-DD' (e.g., '2024-10-01')
            until_date: End date 'YYYY-MM-DD' (e.g., '2026-01-10')
            level: 'ad', 'adset', or 'campaign'
        
        This uses time_range with since/until parameters
        """
        url = f"{self.base_url}/{account_id}/insights"
        
        fields = (
            'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,'
            'date_start,date_stop,impressions,clicks,spend,'
            'actions,action_values,cpm,cpc,ctr,frequency,reach'
        )
        
        # THIS IS THE KEY: time_range with since/until
        params = {
            'access_token': self.access_token,
            'fields': fields,
            'time_range': json.dumps({
                'since': since_date,   # FROM date
                'until': until_date     # TO date
            }),
            'level': level,
            'time_increment': 1,  # Daily breakdown (each day = 1 row)
            'limit': 500
        }
        
        all_data = []
        next_url = url
        
        while next_url:
            data = self._make_request(next_url, params if next_url == url else {})
            if data and 'data' in data:
                all_data.extend(data['data'])
                next_url = data.get('paging', {}).get('next')
                params = {}  # Clear params for pagination
            else:
                break
        
        logger.info(f"    Fetched {len(all_data)} records from {since_date} to {until_date}")
        return all_data
    
    def fetch_today(self, account_id, level='campaign'):
        """Fetch TODAY's data (real-time updates)"""
        url = f"{self.base_url}/{account_id}/insights"
        
        fields = (
            'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,'
            'date_start,date_stop,impressions,clicks,spend,'
            'actions,action_values,cpm,cpc,ctr,frequency,reach'
        )
        
        params = {
            'access_token': self.access_token,
            'fields': fields,
            'date_preset': 'today',
            'level': level,
            'limit': 500
        }
        
        all_data = []
        next_url = url
        
        while next_url:
            data = self._make_request(next_url, params if next_url == url else {})
            if data and 'data' in data:
                all_data.extend(data['data'])
                next_url = data.get('paging', {}).get('next')
                params = {}
            else:
                break
        
        return all_data
    
    def fetch_historical_to_yesterday(self, account_id, start_date, level='campaign'):
        """
        Fetch from start_date to YESTERDAY
        (Everything except today)
        """
        yesterday = (datetime.now(Config.IST) - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"  📅 Fetching HISTORICAL: {start_date} to {yesterday}")
        return self.fetch_custom_date_range(account_id, start_date, yesterday, level)

# ============================================
# DATA PROCESSOR
# ============================================
class DataProcessor:
    
    @staticmethod
    def extract_action_value(actions, action_type):
        if not actions:
            return 0
        for action in actions:
            if action.get('action_type') == action_type:
                try:
                    return int(float(action.get('value', 0)))
                except:
                    return 0
        return 0
    
    @staticmethod
    def extract_action_revenue(action_values, action_type):
        if not action_values:
            return 0.0
        for action in action_values:
            if action.get('action_type') == action_type:
                try:
                    return float(action.get('value', 0))
                except:
                    return 0.0
        return 0.0
    
    @staticmethod
    def process_insights(raw_data, include_ad_details=True):
        records = []
        for item in raw_data:
            spend = float(item.get('spend', 0))
            impressions = int(item.get('impressions', 0))
            clicks = int(item.get('clicks', 0))
            
            actions = item.get('actions', [])
            action_values = item.get('action_values', [])
            
            link_clicks = DataProcessor.extract_action_value(actions, 'link_click')
            landing_page_views = DataProcessor.extract_action_value(actions, 'landing_page_view')
            add_to_cart = DataProcessor.extract_action_value(actions, 'add_to_cart')
            initiate_checkout = DataProcessor.extract_action_value(actions, 'initiate_checkout')
            purchases = DataProcessor.extract_action_value(actions, 'offsite_conversion.fb_pixel_purchase')
            revenue = DataProcessor.extract_action_revenue(action_values, 'offsite_conversion.fb_pixel_purchase')
            
            roas = revenue / spend if spend > 0 else 0
            cvr = (purchases / link_clicks * 100) if link_clicks > 0 else 0
            cost_per_purchase = spend / purchases if purchases > 0 else 0
            
            record = {
                'Date': item.get('date_start', ''),
                'Timestamp': datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S'),
                'Campaign_ID': item.get('campaign_id', ''),
                'Campaign_Name': item.get('campaign_name', ''),
                'Spend': round(spend, 2),
                'Revenue': round(revenue, 2),
                'Impressions': impressions,
                'Clicks': clicks,
                'Link_Clicks': link_clicks,
                'Landing_Page_Views': landing_page_views,
                'Add_to_Cart': add_to_cart,
                'Initiate_Checkout': initiate_checkout,
                'Purchases': purchases,
                'ROAS': round(roas, 2),
                'CPM': round(float(item.get('cpm', 0)), 2),
                'CPC': round(float(item.get('cpc', 0)), 2),
                'CTR': round(float(item.get('ctr', 0)), 2),
                'CVR': round(cvr, 2),
                'Cost_Per_Purchase': round(cost_per_purchase, 2),
                'Frequency': round(float(item.get('frequency', 0)), 2),
                'Reach': int(item.get('reach', 0))
            }
            
            if include_ad_details:
                record.update({
                    'AdSet_ID': item.get('adset_id', ''),
                    'AdSet_Name': item.get('adset_name', ''),
                    'Ad_ID': item.get('ad_id', ''),
                    'Ad_Name': item.get('ad_name', '')
                })
            
            records.append(record)
        
        df = pd.DataFrame(records)
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        return df

# ============================================
# MAIN EXPORTER - HISTORICAL + LIVE
# ============================================
def main():
    logger.info("="*70)
    logger.info("🚀 META ADS DATA FETCH - OCTOBER 1, 2024 TO NOW")
    logger.info("="*70)
    
    access_token = Config.ACCESS_TOKEN
    if not access_token:
        logger.error("❌ META_ACCESS_TOKEN not set!")
        return False
    
    api = MetaAdsAPI(access_token)
    processor = DataProcessor()
    
    # Calculate dates
    start_date = Config.HISTORICAL_START_DATE  # Oct 1, 2024
    today = datetime.now(Config.IST).strftime('%Y-%m-%d')
    yesterday = (datetime.now(Config.IST) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"\n📅 Date Range Configuration:")
    logger.info(f"  Historical Start: {start_date}")
    logger.info(f"  Yesterday: {yesterday}")
    logger.info(f"  Today: {today}")
    
    # ========================================
    # STRATEGY:
    # 1. Fetch Oct 1, 2024 to Yesterday (Historical - stable data)
    # 2. Fetch Today (Live - updates every 15 min)
    # 3. Combine both for complete dataset
    # ========================================
    
    all_historical_campaigns = []
    all_historical_ads = []
    all_today_campaigns = []
    all_today_ads = []
    
    for account_id in Config.AD_ACCOUNT_IDS:
        logger.info(f"\n📌 Processing Account: {account_id}")
        
        # ========================================
        # 1. HISTORICAL DATA (Oct 1, 2024 to Yesterday)
        # ========================================
        logger.info("  📚 Fetching HISTORICAL data...")
        
        # Campaign level - Historical
        historical_campaigns = api.fetch_historical_to_yesterday(
            account_id, 
            start_date, 
            level='campaign'
        )
        all_historical_campaigns.extend(historical_campaigns)
        logger.info(f"    ✓ Historical Campaigns: {len(historical_campaigns)} records")
        
        # Ad level - Historical
        historical_ads = api.fetch_historical_to_yesterday(
            account_id, 
            start_date, 
            level='ad'
        )
        all_historical_ads.extend(historical_ads)
        logger.info(f"    ✓ Historical Ads: {len(historical_ads)} records")
        
        # ========================================
        # 2. TODAY'S DATA (Real-time, updates every 15 min)
        # ========================================
        logger.info("  📊 Fetching TODAY'S data (live)...")
        
        # Campaign level - Today
        today_campaigns = api.fetch_today(account_id, level='campaign')
        all_today_campaigns.extend(today_campaigns)
        logger.info(f"    ✓ Today Campaigns: {len(today_campaigns)} records")
        
        # Ad level - Today
        today_ads = api.fetch_today(account_id, level='ad')
        all_today_ads.extend(today_ads)
        logger.info(f"    ✓ Today Ads: {len(today_ads)} records")
    
    # ========================================
    # PROCESS AND EXPORT DATA
    # ========================================
    logger.info("\n" + "="*70)
    logger.info("💾 PROCESSING AND EXPORTING CSV FILES")
    logger.info("="*70)
    
    # ========================================
    # 1. COMPLETE CAMPAIGN DATA (Oct 1, 2024 to TODAY)
    # ========================================
    all_campaigns_combined = all_historical_campaigns + all_today_campaigns
    
    if all_campaigns_combined:
        df_campaigns_all = processor.process_insights(all_campaigns_combined, include_ad_details=False)
        
        # Remove duplicates (in case of overlapping data)
        df_campaigns_all = df_campaigns_all.drop_duplicates(
            subset=['Date', 'Campaign_ID'], 
            keep='last'
        ).sort_values('Date')
        
        df_campaigns_all.to_csv('complete_campaign_data.csv', index=False)
        logger.info(f"✅ complete_campaign_data.csv ({len(df_campaigns_all)} rows)")
        logger.info(f"   Date range: {df_campaigns_all['Date'].min()} to {df_campaigns_all['Date'].max()}")
    
    # ========================================
    # 2. COMPLETE AD DATA (Oct 1, 2024 to TODAY)
    # ========================================
    all_ads_combined = all_historical_ads + all_today_ads
    
    if all_ads_combined:
        df_ads_all = processor.process_insights(all_ads_combined, include_ad_details=True)
        
        # Remove duplicates
        df_ads_all = df_ads_all.drop_duplicates(
            subset=['Date', 'Ad_ID'], 
            keep='last'
        ).sort_values('Date')
        
        df_ads_all.to_csv('complete_ad_data.csv', index=False)
        logger.info(f"✅ complete_ad_data.csv ({len(df_ads_all)} rows)")
        logger.info(f"   Date range: {df_ads_all['Date'].min()} to {df_ads_all['Date'].max()}")
    
    # ========================================
    # 3. TODAY'S PERFORMANCE (Separate for real-time dashboard)
    # ========================================
    if all_today_campaigns:
        df_today = processor.process_insights(all_today_campaigns, include_ad_details=False)
        df_today.to_csv('live_today_performance.csv', index=False)
        logger.info(f"✅ live_today_performance.csv ({len(df_today)} rows)")
    
    # ========================================
    # 4. DAILY SUMMARY (Aggregated by date)
    # ========================================
    if all_campaigns_combined:
        df_daily_summary = df_campaigns_all.groupby('Date').agg({
            'Spend': 'sum',
            'Revenue': 'sum',
            'Impressions': 'sum',
            'Clicks': 'sum',
            'Link_Clicks': 'sum',
            'Landing_Page_Views': 'sum',
            'Add_to_Cart': 'sum',
            'Initiate_Checkout': 'sum',
            'Purchases': 'sum',
            'Reach': 'sum'
        }).reset_index()
        
        # Calculate metrics
        df_daily_summary['ROAS'] = (df_daily_summary['Revenue'] / df_daily_summary['Spend']).round(2)
        df_daily_summary['CPM'] = ((df_daily_summary['Spend'] / df_daily_summary['Impressions']) * 1000).round(2)
        df_daily_summary['CPC'] = (df_daily_summary['Spend'] / df_daily_summary['Clicks']).round(2)
        df_daily_summary['CTR'] = ((df_daily_summary['Clicks'] / df_daily_summary['Impressions']) * 100).round(2)
        df_daily_summary['CVR'] = ((df_daily_summary['Purchases'] / df_daily_summary['Link_Clicks']) * 100).round(2)
        
        # Replace inf and NaN
        df_daily_summary = df_daily_summary.replace([np.inf, -np.inf], 0).fillna(0)
        
        df_daily_summary.to_csv('daily_summary.csv', index=False)
        logger.info(f"✅ daily_summary.csv ({len(df_daily_summary)} rows)")
    
    # ========================================
    # 5. SUMMARY STATS (Latest snapshot)
    # ========================================
    if all_today_campaigns:
        df_today = processor.process_insights(all_today_campaigns, include_ad_details=False)
        summary = {
            'Last_Updated': datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S'),
            'Data_Start_Date': start_date,
            'Data_End_Date': today,
            'Total_Days': (datetime.now(Config.IST) - datetime.strptime(start_date, '%Y-%m-%d')).days + 1,
            'Today_Spend': round(df_today['Spend'].sum(), 2),
            'Today_Revenue': round(df_today['Revenue'].sum(), 2),
            'Today_Purchases': int(df_today['Purchases'].sum()),
            'Today_ROAS': round(df_today['Revenue'].sum() / df_today['Spend'].sum() if df_today['Spend'].sum() > 0 else 0, 2),
            'Total_Records': len(all_campaigns_combined)
        }
        df_summary = pd.DataFrame([summary])
        df_summary.to_csv('live_summary.csv', index=False)
        logger.info(f"✅ live_summary.csv (1 row)")
    
    logger.info("\n" + "="*70)
    logger.info("✅ DATA FETCH COMPLETED!")
    logger.info("="*70)
    logger.info(f"\n📊 Summary:")
    logger.info(f"  • Start Date: {start_date}")
    logger.info(f"  • End Date: {today}")
    logger.info(f"  • Total Days: {(datetime.now(Config.IST) - datetime.strptime(start_date, '%Y-%m-%d')).days + 1}")
    logger.info(f"  • Campaign Records: {len(all_campaigns_combined)}")
    logger.info(f"  • Ad Records: {len(all_ads_combined)}")
    logger.info(f"\n⏰ Next update in 15 minutes (today's data will refresh)")
    logger.info("="*70)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
