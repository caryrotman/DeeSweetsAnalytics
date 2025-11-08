#!/usr/bin/env python3
"""
Country Spike Analyzer
- Reads the country weekly views file
- Calculates average views per week (across all countries)
- Identifies country spikes that are >=25% above average AND >=50 views above average
"""

import pandas as pd
import argparse
import os

def analyze_spikes(input_file: str, min_pct_above_avg: float = 25.0, min_views_above_avg: int = 50):
    """
    Analyze country spikes vs weekly averages.
    
    Args:
        input_file: Path to the tab-separated file with week, country, total_views
        min_pct_above_avg: Minimum percentage above average (default 25%)
        min_views_above_avg: Minimum views above average (default 50)
    """
    # Read the file (handle encoding issues with special characters)
    print(f"Reading {input_file}...")
    try:
        df = pd.read_csv(input_file, sep='\t', encoding='utf-8')
    except UnicodeDecodeError:
        # Try with different encoding if UTF-8 fails
        df = pd.read_csv(input_file, sep='\t', encoding='latin-1')
    
    # Calculate weekly averages (average views across all countries for each week)
    print("Calculating weekly averages...")
    weekly_avg = df.groupby('week')['total_views'].mean().reset_index()
    weekly_avg.columns = ['week', 'avg_views']
    
    # Calculate overall average (average of all weekly averages)
    overall_avg = weekly_avg['avg_views'].mean()
    print(f"Overall average views per country per week: {overall_avg:.1f}")
    
    # Calculate weekly totals (sum of all views across all countries for each week)
    weekly_totals = df.groupby('week')['total_views'].sum().reset_index()
    weekly_totals.columns = ['week', 'total_views']
    
    # Calculate ratio for each week (week's avg views / overall average)
    # Example: if overall avg is 100 and week avg is 150, ratio = 1.5
    weekly_stats = pd.merge(weekly_avg, weekly_totals, on='week')
    weekly_stats['week_ratio'] = weekly_stats['avg_views'] / overall_avg
    
    # Merge weekly averages back to main dataframe
    df_with_avg = pd.merge(df, weekly_avg, on='week')
    
    # Calculate spike metrics for each country-week
    df_with_avg['views_above_avg'] = df_with_avg['total_views'] - df_with_avg['avg_views']
    df_with_avg['pct_above_avg'] = ((df_with_avg['total_views'] - df_with_avg['avg_views']) / df_with_avg['avg_views']) * 100
    
    # Identify spikes: >=25% above average AND >=50 views above average
    spikes = df_with_avg[
        (df_with_avg['pct_above_avg'] >= min_pct_above_avg) &
        (df_with_avg['views_above_avg'] >= min_views_above_avg)
    ].copy()
    
    # Sort by week (descending) then by views_above_avg (descending)
    spikes = spikes.sort_values(['week', 'views_above_avg'], ascending=[False, False])
    
    # Print results
    print("\n" + "="*80)
    print("WEEKLY STATISTICS")
    print("="*80)
    print(f"\n{'Week':<12} {'Avg Views':>12} {'Total Views':>14} {'Week Ratio':>12}")
    print("-" * 80)
    for _, row in weekly_stats.sort_values('week', ascending=False).iterrows():
        print(f"{row['week']:<12} {row['avg_views']:>12.1f} {row['total_views']:>14.0f} {row['week_ratio']:>12.2f}")
    
    print("\n" + "="*80)
    print(f"COUNTRY SPIKES (≥{min_pct_above_avg}% above avg AND ≥{min_views_above_avg} views above avg)")
    print("="*80)
    
    if spikes.empty:
        print("\nNo spikes found that meet the criteria.")
    else:
        print(f"\nFound {len(spikes)} country-week spikes:\n")
        print(f"{'Week':<12} {'Country':<25} {'Views':>8} {'Avg':>8} {'Above Avg':>12} {'% Above':>10}")
        print("-" * 100)
        
        for _, row in spikes.iterrows():
            print(f"{row['week']:<12} {row['country']:<25} {row['total_views']:>8.0f} "
                  f"{row['avg_views']:>8.1f} {row['views_above_avg']:>12.0f} {row['pct_above_avg']:>9.1f}%")
    
    # Save results
    output_file = input_file.replace('.txt', '_spikes.txt')
    spikes_output = spikes[['week', 'country', 'total_views', 'avg_views', 'views_above_avg', 'pct_above_avg']].copy()
    spikes_output.to_csv(output_file, sep='\t', index=False)
    print(f"\nSaved spikes to: {output_file}")
    
    # Also save weekly statistics
    weekly_stats_file = input_file.replace('.txt', '_weekly_stats.txt')
    weekly_stats_output = weekly_stats[['week', 'avg_views', 'total_views', 'week_ratio']].copy()
    weekly_stats_output.to_csv(weekly_stats_file, sep='\t', index=False)
    print(f"Saved weekly statistics to: {weekly_stats_file}")
    
    return spikes, weekly_stats

def main():
    parser = argparse.ArgumentParser(description="Analyze country spikes vs weekly averages")
    parser.add_argument("input_file", help="Input file path (tab-separated: week, country, total_views)")
    parser.add_argument("--min-pct", type=float, default=25.0, 
                       help="Minimum percentage above average (default: 25)")
    parser.add_argument("--min-views", type=int, default=50,
                       help="Minimum views above average (default: 50)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: File not found: {args.input_file}")
        return
    
    analyze_spikes(args.input_file, args.min_pct, args.min_views)

if __name__ == "__main__":
    main()

