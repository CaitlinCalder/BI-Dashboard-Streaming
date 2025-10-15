"""
Simple Power BI Connection Test
Just checks if the endpoint is reachable
"""

import requests

POWERBI_URL = 'https://api.powerbi.com/beta/b14d86f1-83ba-4b13-a702-b5c0231b9337/datasets/7bd18a13-0815-4cbf-bb41-84ab2a2383bc/rows?experience=power-bi&key=dUOGa9OQ%2B%2BgifLK%2F0%2BEzT9xJuvJ18UZrrz5Wfrf6WWamfa%2Fy20uYZB2buv6TCK9T9PWaWAVc3t%2FX%2B5TC6QtyLg%3D%3D'

print("\n" + "="*70)
print("🔌 SIMPLE POWER BI CONNECTION TEST")
print("="*70)

print("\nTesting connection to Power BI streaming endpoint...")
print("Dataset ID: 7bd18a13-0815-4cbf-bb41-84ab2a2383bc")

try:
    # Send empty array - Power BI should accept this
    response = requests.post(
        POWERBI_URL,
        json=[],  # Empty data
        headers={'Content-Type': 'application/json'},
        timeout=10
    )
    
    print(f"\nStatus Code: {response.status_code}")
    print(f"Response: {response.text if response.text else '(empty - success!)'}")
    
    if response.status_code == 200:
        print("\n✅ CONNECTION SUCCESSFUL!")
        print("✅ Power BI endpoint is reachable")
        print("✅ Authentication key is valid")
        print("✅ Dataset exists and is accessible")
        print("\n👉 You're connected! Now we just need to fix the schema.")
        
    elif response.status_code == 400:
        print("\n✅ CONNECTION SUCCESSFUL!")
        print("✅ Endpoint is reachable and authenticated")
        print("⚠️  Schema issue (expected - we sent empty data)")
        print("\n👉 Connection works! Now we need to match the schema.")
        
    elif response.status_code == 401:
        print("\n❌ AUTHENTICATION FAILED")
        print("The API key is invalid or expired")
        print("\n💡 Get a new key from Power BI:")
        print("   1. Go to your dataset in Power BI")
        print("   2. Click Settings → API info")
        print("   3. Copy the new Push URL")
        
    elif response.status_code == 404:
        print("\n❌ DATASET NOT FOUND")
        print("The dataset ID might be wrong")
        print("\n💡 Double-check:")
        print("   1. Dataset ID: 7bd18a13-0815-4cbf-bb41-84ab2a2383bc")
        print("   2. Make sure the dataset exists in Power BI")
        
    else:
        print(f"\n⚠️  Unexpected status: {response.status_code}")
        print(f"Response: {response.text}")

except requests.exceptions.Timeout:
    print("\n❌ CONNECTION TIMEOUT")
    print("Check your internet connection")
    
except requests.exceptions.ConnectionError:
    print("\n❌ CONNECTION ERROR")
    print("Cannot reach Power BI servers")
    print("Check your internet connection")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")

print("\n" + "="*70)