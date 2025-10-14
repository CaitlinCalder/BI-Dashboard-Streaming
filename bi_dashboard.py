"""
Power BI Configuration Helper
Helps you find your Workspace ID, Dataset ID, and get Access Token

Run this script to get the values you need for ClearVueConfig.py

Author: ClearVue Team
Date: October 2025
"""

import webbrowser
import time


def print_header(text):
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70)


def main():
    print("\n" + "="*70)
    print("   POWER BI REFRESH CONFIGURATION HELPER")
    print("="*70)
    
    print("\n📋 This script will help you get 3 required values:")
    print("   1. Workspace ID")
    print("   2. Dataset ID")
    print("   3. Access Token")
    
    # ========================================================================
    # STEP 1: Get Workspace ID
    # ========================================================================
    
    print_header("STEP 1: GET WORKSPACE ID")
    
    print("\n1. Go to Power BI Service: https://app.powerbi.com")
    print("2. Click on your workspace in the left sidebar")
    print("3. Look at the URL in your browser:")
    print("   https://app.powerbi.com/groups/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/...")
    print("                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    print("                                  This is your WORKSPACE ID")
    
    open_browser = input("\n👉 Open Power BI in browser? (y/n) [y]: ").strip().lower() or 'y'
    if open_browser == 'y':
        webbrowser.open("https://app.powerbi.com")
        time.sleep(2)
    
    workspace_id = input("\n✏️  Paste your WORKSPACE ID here: ").strip()
    
    if workspace_id and len(workspace_id) == 36:  # UUID format
        print(f"✅ Workspace ID: {workspace_id}")
    else:
        print("⚠️  Invalid format - should be like: 12345678-1234-1234-1234-123456789012")
    
    # ========================================================================
    # STEP 2: Get Dataset ID
    # ========================================================================
    
    print_header("STEP 2: GET DATASET ID")
    
    print("\n1. In Power BI Service, go to your workspace")
    print("2. Find your dataset (the one with your MongoDB data)")
    print("3. Click on the dataset name")
    print("4. Look at the URL:")
    print("   https://app.powerbi.com/groups/.../datasets/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX")
    print("                                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    print("                                               This is your DATASET ID")
    print("\n💡 TIP: Your dataset might have the same name as your report!")
    
    if workspace_id:
        datasets_url = f"https://app.powerbi.com/groups/{workspace_id}/datasets"
        print(f"\n📂 Direct link to datasets: {datasets_url}")
        
        open_datasets = input("\n👉 Open datasets page? (y/n) [y]: ").strip().lower() or 'y'
        if open_datasets == 'y':
            webbrowser.open(datasets_url)
            time.sleep(2)
    
    dataset_id = input("\n✏️  Paste your DATASET ID here: ").strip()
    
    if dataset_id and len(dataset_id) == 36:
        print(f"✅ Dataset ID: {dataset_id}")
    else:
        print("⚠️  Invalid format - should be like: 12345678-1234-1234-1234-123456789012")
    
    # ========================================================================
    # STEP 3: Get Access Token
    # ========================================================================
    
    print_header("STEP 3: GET ACCESS TOKEN")
    
    print("\n🔐 For this prototype, we'll use a simple method to get a token.")
    print("   (In production, you'd use proper OAuth2 flow)")
    
    print("\n📝 Choose a method:")
    print("   1. Use Power BI Playground (Easy - Recommended for testing)")
    print("   2. Use Azure CLI (Better for automation)")
    print("   3. Manual - I'll get it myself")
    
    method = input("\n👉 Select method (1-3) [1]: ").strip() or '1'
    
    if method == '1':
        print("\n📖 Using Power BI Playground:")
        print("   1. We'll open the Power BI JavaScript demo page")
        print("   2. Click the 'Embed Token' button")
        print("   3. Click 'Generate Embed Token'")
        print("   4. Copy the 'Access Token' (very long string)")
        print("   5. ⚠️  NOTE: Token expires in 1 hour!")
        
        open_playground = input("\n👉 Open Power BI Playground? (y/n) [y]: ").strip().lower() or 'y'
        if open_playground == 'y':
            webbrowser.open("https://microsoft.github.io/PowerBI-JavaScript/demo/v2-demo/index.html")
            time.sleep(2)
        
        print("\n⏳ Waiting for you to get the token...")
        access_token = input("\n✏️  Paste your ACCESS TOKEN here: ").strip()
        
        if access_token and len(access_token) > 100:
            print(f"✅ Access Token received ({len(access_token)} characters)")
            print("⚠️  Remember: Token expires in 1 hour!")
        else:
            print("⚠️  Token seems too short - make sure you copied the full token")
    
    elif method == '2':
        print("\n📖 Using Azure CLI:")
        print("\n   Run this command in your terminal:")
        print("   az login")
        print("   az account get-access-token --resource https://analysis.windows.net/powerbi/api")
        print("\n   Copy the 'accessToken' value from the JSON output")
        
        access_token = input("\n✏️  Paste your ACCESS TOKEN here: ").strip()
        
        if access_token and len(access_token) > 100:
            print(f"✅ Access Token received ({len(access_token)} characters)")
        else:
            print("⚠️  Token seems too short")
    
    else:
        print("\n📖 Get your token using your preferred method")
        print("   See: https://learn.microsoft.com/en-us/rest/api/power-bi/")
        
        access_token = input("\n✏️  Paste your ACCESS TOKEN here: ").strip()
    
    # ========================================================================
    # SUMMARY & CONFIG
    # ========================================================================
    
    print_header("CONFIGURATION SUMMARY")
    
    print("\n✅ Copy these values to ClearVueConfig.py:\n")
    
    print("# Power BI Refresh Configuration")
    print(f"POWERBI_WORKSPACE_ID = '{workspace_id}'")
    print(f"POWERBI_DATASET_ID = '{dataset_id}'")
    print(f"POWERBI_ACCESS_TOKEN = '{access_token[:20]}...'  # Full token here")
    
    print("\n" + "="*70)
    
    # Save to file
    config_text = f"""
# Power BI Refresh Configuration
# Add these to your ClearVueConfig.py

POWERBI_WORKSPACE_ID = '{workspace_id}'
POWERBI_DATASET_ID = '{dataset_id}'
POWERBI_ACCESS_TOKEN = '{access_token}'

# Refresh behavior
POWERBI_MIN_REFRESH_INTERVAL = 60   # Minimum 60 seconds between refreshes
POWERBI_BATCH_WINDOW = 30            # Wait 30 seconds to batch changes
"""
    
    with open('powerbi_config.txt', 'w') as f:
        f.write(config_text)
    
    print("💾 Configuration saved to: powerbi_config.txt")
    print("   Copy the contents to your ClearVueConfig.py")
    
    print("\n" + "="*70)
    print("   NEXT STEPS")
    print("="*70)
    print("\n1. Copy the config values above to ClearVueConfig.py")
    print("2. Update your ClearVueStreamingPipeline.py with the new code")
    print("3. Restart your streaming pipeline")
    print("4. Generate test data")
    print("5. Wait 30 seconds (batch window)")
    print("6. Power BI dataset will refresh automatically!")
    print("7. Refresh your Power BI dashboard to see new data")
    
    print("\n💡 TIP: The first refresh might take 1-2 minutes.")
    print("   Subsequent refreshes will be faster.\n")
    
    print("⚠️  REMEMBER: Access token expires in 1 hour!")
    print("   For long-running pipelines, you'll need to refresh the token.")
    print("   Consider using Azure service principal for production.\n")


if __name__ == "__main__":
    main()