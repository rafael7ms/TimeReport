import pandas as pd
import os
import argparse
import logging
from datetime import datetime

def setup_logging():
    """Configure logging to show actions with timestamps"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger()

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
        print(df)
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

def main():
    parser = argparse.ArgumentParser(description='Process employee time tracking data.')
    parser.add_argument('input_file', help='Path to the input file (CSV or Excel)')
    parser.add_argument('-o', '--output', help='Path to the output file (optional)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    process_employee_data(args.input_file, args.output)

if __name__ == "__main__":
    main()