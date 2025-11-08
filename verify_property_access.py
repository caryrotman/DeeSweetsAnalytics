#!/usr/bin/env python3
"""Verify access to GA4 property via Management API."""

import requests
from google.auth import default
from google.auth.transport.requests import Request

try:
    credentials, project = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    if not credentials.valid:
        credentials.refresh(Request())
    
    # Try to list properties using Management API
    url = "https://analyticsadmin.googleapis.com/v1beta/accounts/-/properties"
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Management API Status: {response.status_code}")
    
    if response.status_code == 200:
        properties = response.json().get('properties', [])
        print(f"\nFound {len(properties)} properties:")
        for prop in properties:
            print(f"  - {prop.get('displayName')} (ID: {prop.get('name', '').split('/')[-1]})")
    else:
        print(f"Response: {response.text[:300]}")
        
except Exception as e:
    print(f"Error: {e}")

