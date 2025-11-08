#!/usr/bin/env python3
"""
Helper script to fix GA4 API authentication.
This will delete old credentials and prompt for re-authentication with proper scopes.
"""

import os
import subprocess
import sys

def main():
    creds_path = os.path.join(os.getenv('APPDATA'), 'gcloud', 'application_default_credentials.json')
    
    print("GA4 API Authentication Fix")
    print("=" * 50)
    print()
    print("This script will:")
    print("1. Delete existing application default credentials")
    print("2. Prompt you to re-authenticate with proper scopes")
    print()
    
    if os.path.exists(creds_path):
        print(f"Found credentials at: {creds_path}")
        response = input("Delete and re-authenticate? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        
        try:
            os.remove(creds_path)
            print("✓ Deleted old credentials")
        except Exception as e:
            print(f"Error deleting credentials: {e}")
            return
    else:
        print("No existing credentials found.")
    
    print()
    print("Now re-authenticating with proper scopes...")
    print("A browser window will open for authentication.")
    print()
    
    # Re-authenticate with cloud-platform scope (includes analytics)
    cmd = [
        'gcloud', 'auth', 'application-default', 'login',
        '--scopes=https://www.googleapis.com/auth/cloud-platform'
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print()
        print("✓ Authentication successful!")
        print()
        print("You can now run:")
        print("  python country_spike_report.py --use-api --weeks 20 --property-id 427048881")
    except subprocess.CalledProcessError as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("ERROR: gcloud CLI not found. Please install Google Cloud SDK.")
        sys.exit(1)

if __name__ == "__main__":
    main()

