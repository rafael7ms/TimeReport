import pandas as pd
import os
import re
from fuzzywuzzy import fuzz, process
import logging
from datetime import datetime

def setup_logging():
    """Configure logging to file and console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('queue_processor_enhanced.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()

def normalize_name(name):
    """Enhanced name normalization with fuzzy matching preparation"""
    if pd.isna(name) or not isinstance(name, str):
        return None, None
    
    original = name.strip()
    if not original:
        return None, None
    
    # Remove extra spaces and special characters
    cleaned = re.sub(r'[^a-zA-Z, ]', '', original).strip()
    
    # Handle 'Lastname, Firstname' format
    if ',' in cleaned:
        parts = [p.strip() for p in cleaned.split(',')]
        if len(parts) >= 2:
            # Return both formats for matching
            standard = f"{parts[1]} {parts[0]}".strip()
            return standard, original
        return cleaned, original
    
    # Handle 'Firstname Lastname' format
    return cleaned, original

def match_names(tracking_names, affil_names, min_confidence=80):
    """Fuzzy match names with a minimum confidence threshold"""
    matches = {}
    unmatched = []
    
    for t_name in tracking_names:
        if pd.isna(t_name):
            continue
            
        # Try exact match first
        if t_name in affil_names:
            matches[t_name] = t_name
            continue
            
        # Try fuzzy match
        best_match, score = process.extractOne(t_name, affil_names, scorer=fuzz.token_sort_ratio)
        if score >= min_confidence:
            matches[t_name] = best_match
        else:
            unmatched.append(t_name)
            logging.warning(f"No good match found for '{t_name}' (best: '{best_match}' at {score}%)")
            
    return matches, unmatched

def add_queue_to_tracking(tracking_file, affiliation_file, output_file=None, min_confidence=80):
    logger = setup_logging()
    logger.info("Starting enhanced queue processing with fuzzy matching")
    
    try:
        # Read input files
        logger.info(f"Reading tracking file: {tracking_file}")
        tracking_df = pd.read_excel(tracking_file) if tracking_file.endswith('.xlsx') else pd.read_csv(tracking_file)
        
        logger.info(f"Reading affiliation file: {affiliation_file}")
        affiliation_df = pd.read_excel(affiliation_file) if affiliation_file.endswith('.xlsx') else pd.read_csv(affiliation_file)
        
        # Normalize names in both dataframes
        logger.info("Normalizing employee names")
        tracking_df[['Normalized_Name', 'Original_Tracking_Name']] = tracking_df['Employee Name'].apply(
            lambda x: pd.Series(normalize_name(x))
        )
        affiliation_df[['Normalized_Name', 'Original_Affil_Name']] = affiliation_df['Name'].apply(
            lambda x: pd.Series(normalize_name(x))
        )
        
        # Get unique normalized names
        tracking_names = tracking_df['Normalized_Name'].dropna().unique()
        affil_names = affiliation_df['Normalized_Name'].dropna().unique()
        
        # Match names with fuzzy logic
        logger.info("Matching names with fuzzy logic")
        name_matches, unmatched = match_names(tracking_names, affil_names, min_confidence)
        
        # Report matching results
        logger.info(f"Matched {len(name_matches)} out of {len(tracking_names)} names")
        if unmatched:
            logger.warning(f"Could not match {len(unmatched)} names: {unmatched}")
        
        # Create mapping dictionary
        name_map = {k: v for k, v in name_matches.items()}
        
        # Map the matched names
        tracking_df['Matched_Name'] = tracking_df['Normalized_Name'].map(name_map)
        
        # Merge dataframes
        logger.info("Merging data")
        merged_df = pd.merge(
            tracking_df,
            affiliation_df[['Normalized_Name', 'Queue']],
            left_on='Matched_Name',
            right_on='Normalized_Name',
            how='left'
        )
        
        # Clean up columns
        merged_df = merged_df.drop(columns=['Normalized_Name_x', 'Normalized_Name_y', 'Matched_Name'])
        
        # Reorder columns to include Queue
        columns = ['Employee Name', 'Queue', 'Start', 'Stop', 'Duration', 
                  'In Schedule', 'In Adherence', 'Scheduled State', 'Actual State']
        # Keep only existing columns
        columns = [col for col in columns if col in merged_df.columns]
        merged_df = merged_df[columns]
        
        # Set output filename
        if not output_file:
            base_name = os.path.splitext(tracking_file)[0]
            output_file = f"{base_name}_queued_enhanced.xlsx"
        
        # Save to Excel
        logger.info(f"Saving output to {output_file}")
        merged_df.to_excel(output_file, index=False)
        
        # Generate matching report
        match_rate = len(name_matches) / len(tracking_names) * 100
        logger.info(f"Name matching completed with {match_rate:.2f}% success rate")
        
        return output_file, match_rate
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Add Queue information to time tracking data with enhanced name matching')
    parser.add_argument('tracking_file', help='Path to time tracking file (CSV or XLSX)')
    parser.add_argument('affiliation_file', help='Path to agent affiliation file (CSV or XLSX)')
    parser.add_argument('-o', '--output', help='Output file path (optional)')
    parser.add_argument('--min-confidence', type=int, default=80, 
                       help='Minimum confidence percentage for name matching (default: 80)')
    
    args = parser.parse_args()
    
    try:
        result, match_rate = add_queue_to_tracking(
            args.tracking_file, 
            args.affiliation_file, 
            args.output,
            args.min_confidence
        )
        print(f"Successfully created output file: {result}")
        print(f"Name matching success rate: {match_rate:.2f}%")
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Check queue_processor_enhanced.log for details")
        exit(1)