import pandas as pd
import os
from fuzzywuzzy import fuzz, process
import logging
from datetime import datetime
import re
import argparse

def setup_logging():
    """Configure logging to file and console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('treport_etl.log'),
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

def process_employee_data(input_file, output_file=None):
    logger = setup_logging()
    logger.info(f"Starting processing of file: {input_file}")
    
    try:
        # Read the input file
        logger.info("Reading input file...")
        if input_file.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file, header=None, dtype=str)
        else:
            df = pd.read_csv(input_file, header=None, dtype=str, skip_blank_lines=False)
       
        df=df.dropna(how='all')
        df=df.dropna(axis='columns', how='all')
        logger.info(f"Successfully read file with {len(df)} rows")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return

    # Initialize variables
    employee_group = None
    employee_name = None
    records = []
    
    # Process each row
    for index, row in df.iterrows():
        # Convert row to list and clean up
        row_data = [str(x).strip() if pd.notna(x) and str(x).strip() != 'nan' else '' for x in row]
        
        # Remove empty columns from the row
        non_empty_cols = [i for i, val in enumerate(row_data) if val]
        if non_empty_cols:
            min_col, max_col = min(non_empty_cols), max(non_empty_cols)
            row_data = row_data[min_col:max_col+1]
        else:
            row_data = []
            
        # Skip empty rows
        if not any(row_data):
            continue
            
        # Check for Employee Group
        if any('Employee Group:' in x for x in row_data):
            employee_group = ' '.join(row_data).split('Employee Group:')[-1].strip().split(':')[0].strip()
            logger.info(f"Found Employee Group: {employee_group}")
            continue
            
        # Check for Employee Name
        if any('Employee Name:' in x for x in row_data):
            employee_name = ' '.join(row_data).split('Employee Name:')[-1].strip()
            logger.info(f"Found Employee Name: {employee_name}")
            continue
            
        # Check if this row matches the data pattern (date values present)
        if any('/' in x for x in row_data) and len(row_data) >= 7:
            try:
                # Extract data fields
                record = {
                    'Employee Group': employee_group,
                    'Employee Name': employee_name,
                    'Start': row_data[0] if len(row_data) > 0 else '',
                    'Stop': row_data[2] if len(row_data) > 1 else '',
                    'Duration': row_data[3] if len(row_data) > 2 else '',
                    'In Schedule': row_data[4] if len(row_data) > 3 else '',
                    'In Adherence': row_data[5] if len(row_data) > 4 else '',
                    'Scheduled State': row_data[6] if len(row_data) > 5 else '',
                    'Actual State': row_data[7] if len(row_data) > 6 else ''
                }
                records.append(record)
                logger.debug(f"Added record: {record}")
                
            except Exception as e:
                logger.warning(f"Row {index+1}: Error processing data - {e}")
                continue
    
    # Create output DataFrame
    if records:
        output_df = pd.DataFrame(records)
        
        # Reorder columns to have Employee Group and Name first
        column_order = ['Employee Group', 'Employee Name'] + [col for col in output_df.columns if col not in ['Employee Group', 'Employee Name']]
        output_df = output_df[column_order]
        
        logger.info(f"Created output with {len(output_df)} records")
        
        # Set default output filename if not provided
        if not output_file:
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            output_ext = '.xlsx' if input_file.lower().endswith(('.xlsx', '.xls')) else '.csv'
            output_file = f"{base_name}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}{output_ext}"
            logger.info(f"Using default output file: {output_file}")
        
        # Save to file
        try:
            if output_file.lower().endswith(('.xlsx', '.xls')):
                output_df.to_excel(output_file, index=False)
            else:
                output_df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved to: {output_file}")
            return output_df
        except Exception as e:
            logger.error(f"Error saving output file: {e}")
    else:
        logger.warning("No valid records found in the input file")

def add_queue_to_tracking_df(tracking_df, affiliation_file, min_confidence=80):
    """Add queue information to tracking data without saving intermediate files"""
    logger = setup_logging()
    logger.info("Starting enhanced queue processing with fuzzy matching")
    
    try:
        logger.info(f"Reading affiliation file: {affiliation_file}")
        # Read the roster Excel file, specifically the "7MS Main Roster" sheet
        affiliation_df = pd.read_excel(affiliation_file, sheet_name='7MS Main Roster ')
        
        # Use "Name" column as the employee name field (as specified in the task)
        print(affiliation_df.columns)
        
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
            affiliation_df[['Normalized_Name', 'Supervisor', 'Manager', 'Department', 'Role', 'Shift', 'Schedule']],
            left_on='Matched_Name',
            right_on='Normalized_Name',
            how='left'
        )
        
        # Clean up columns
        merged_df = merged_df.drop(columns=['Normalized_Name_x', 'Normalized_Name_y', 'Matched_Name'])
        
        # Reorder columns to include additional fields
        columns = ['Employee Name', 'Supervisor', 'Manager', 'Department', 'Role', 'Shift', 'Schedule', 
                  'Start', 'Stop', 'Duration', 'In Schedule', 'In Adherence', 'Scheduled State', 'Actual State']
        # Keep only existing columns
        columns = [col for col in columns if col in merged_df.columns]
        merged_df = merged_df[columns]
        merged_df = merged_df.rename(columns={'Department': 'Queue'})

        # Generate matching report
        match_rate = len(name_matches) / len(tracking_names) * 100
        logger.info(f"Name matching completed with {match_rate:.2f}% success rate")
        
        return merged_df, match_rate
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise

def add_queue_to_tracking(tracking_file, affiliation_file, output_file=None, min_confidence=80):
    logger = setup_logging()
    logger.info("Starting enhanced queue processing with fuzzy matching")
    
    try:
        # Read input files
        logger.info(f"Reading tracking file: {tracking_file}")
        tracking_df = pd.read_excel(tracking_file) if tracking_file.endswith('.xlsx') else pd.read_csv(tracking_file)
        
        logger.info(f"Reading affiliation file: {affiliation_file}")
        # Read the roster Excel file, specifically the "7MS Main Roster" sheet
        affiliation_df = pd.read_excel(affiliation_file, sheet_name='7MS Main Roster ')
        
        # Use "Name" column as the employee name field (as specified in the task)

        print(affiliation_df.columns)
        
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
            affiliation_df[['Normalized_Name', 'Supervisor', 'Manager', 'Department', 'Role', 'Shift', 'Schedule']],
            left_on='Matched_Name',
            right_on='Normalized_Name',
            how='left'
        )
        
        # Clean up columns
        merged_df = merged_df.drop(columns=['Normalized_Name_x', 'Normalized_Name_y', 'Matched_Name'])
        
        # Reorder columns to include additional fields
        columns = ['Employee Name', 'Supervisor', 'Manager', 'Department', 'Role', 'Shift', 'Schedule', 
                  'Start', 'Stop', 'Duration', 'In Schedule', 'In Adherence', 'Scheduled State', 'Actual State']
        # Keep only existing columns
        columns = [col for col in columns if col in merged_df.columns]
        merged_df = merged_df[columns]
        merged_df = merged_df.rename(columns={'Department': 'Queue'})

        # Set output filename
        if not output_file:
            base_name = os.path.splitext(tracking_file)[0]
            output_file = f"{base_name}_with_queue.xlsx"
        
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

def main():
    parser = argparse.ArgumentParser(description='Process time tracking data and enrich with employee information')
    parser.add_argument('input_file', help='Path to the input time tracking file (CSV or Excel)')
    parser.add_argument('roster_file', help='Path to the employee roster file (Excel)')
    parser.add_argument('-o', '--output', help='Output file path (optional)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--min-confidence', type=int, default=80, 
                       help='Minimum confidence percentage for name matching (default: 80)')
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return
    
    if not os.path.isfile(args.roster_file):
        print(f"Error: Roster file '{args.roster_file}' not found.")
        return
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    # Step 1: Process the raw time tracking data
    processed_data = process_employee_data(args.input_file)
    
    if processed_data is None or processed_data.empty:
        print("Error: No valid data found in input file.")
        return
    
    # Step 2: Enrich with employee information from roster (without saving intermediate files)
    try:
        result_df, match_rate = add_queue_to_tracking_df(
            processed_data, 
            args.roster_file, 
            args.min_confidence
        )
        print(f"Name matching success rate: {match_rate:.2f}%")
        
        # Set output filename
        if not args.output:
            base_name = os.path.splitext(os.path.basename(args.input_file))[0]
            args.output = f"{base_name}_with_queue.xlsx"
        
        # Save to Excel
        result_df.to_excel(args.output, index=False)
        print(f"Successfully created output file: {args.output}")
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Check treport_etl.log for details")
        exit(1)

if __name__ == "__main__":
    main()
