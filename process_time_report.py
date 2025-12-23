import pandas as pd

file_path = 'TimeReport/input.xlsx'
df = pd.read_excel(file_path)

# Clean the data
df = df.drop_duplicates()

if 'Login' in df.columns and 'Logout' in df.columns:
    df = df.dropna(subset=['Login', 'Logout'])

if 'Login' in df.columns:
    df['Login'] = pd.to_datetime(df['Login'], errors='coerce')
if 'Logout' in df.columns:
    df['Logout'] = pd.to_datetime(df['Logout'], errors='coerce')

cleaned_file_path = 'TimeReport/cleaned_login_logout.xlsx'
df.to_excel(cleaned_file_path, index=False)

print(f"ETL completed. Cleaned data saved to {cleaned_file_path}")
