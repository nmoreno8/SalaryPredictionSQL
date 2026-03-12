import pandas as pd

def clean_salary(df):
    # Remove rows with no salary data
    df = df[df['Salary Estimate'] != '-1']
    
    # Parse Salary Estimate
    salary = df['Salary Estimate'].apply(lambda x: x.split('(')[0])
    minus_kd = salary.apply(lambda x: x.replace('K','').replace('$',''))
    
    # Handle "Per Hour" and "Employer Provided"
    df['hourly'] = df['Salary Estimate'].apply(lambda x: 1 if 'per hour' in x.lower() else 0)
    df['employer_provided'] = df['Salary Estimate'].apply(lambda x: 1 if 'employer provided salary:' in x.lower() else 0)
    
    min_hr = minus_kd.apply(lambda x: x.lower().replace('per hour','').replace('employer provided salary:',''))
    
    df['min_salary'] = min_hr.apply(lambda x: int(x.split('-')[0]))
    df['max_salary'] = min_hr.apply(lambda x: int(x.split('-')[1]))
    df['avg_salary'] = (df.min_salary + df.max_salary) / 2
    
    return df

def clean_company(df):
    # Split Company Name from Rating (e.g. "Google\n3.8" -> "Google")
    df['company_txt'] = df.apply(lambda x: x['Company Name'] if x['Rating'] < 0 else x['Company Name'][:-4], axis=1)
    
    # Calculate Age
    df['age'] = df.Founded.apply(lambda x: x if x < 1 else 2025 - x) # Assuming current year context
    
    return df

def clean_location(df):
    df['job_state'] = df['Location'].apply(lambda x: x.split(',')[1] if ',' in x else x)
    df['same_state'] = df.apply(lambda x: 1 if x.Location == x.Headquarters else 0, axis=1)
    return df

def extract_skills(df):
    # Skill parsing
    df['python_yn'] = df['Job Description'].apply(lambda x: 1 if 'python' in x.lower() else 0)
    df['R_yn'] = df['Job Description'].apply(lambda x: 1 if 'r studio' in x.lower() or 'r-studio' in x.lower() else 0)
    df['spark'] = df['Job Description'].apply(lambda x: 1 if 'spark' in x.lower() else 0)
    df['aws'] = df['Job Description'].apply(lambda x: 1 if 'aws' in x.lower() else 0)
    df['excel'] = df['Job Description'].apply(lambda x: 1 if 'excel' in x.lower() else 0)
    return df

def process_data(input_file, output_file):
    print(f"Reading raw data from {input_file}...")
    df = pd.read_csv(input_file)
    
    df = clean_salary(df)
    df = clean_company(df)
    df = clean_location(df)
    df = extract_skills(df)
    
    # Drop the index column if it exists in raw
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis=1)
        
    print(f"Saving cleaned data to {output_file}...")
    df.to_csv(output_file, index=False)
    print("Preprocessing complete.")

if __name__ == "__main__":
    process_data('archive/glassdoor_jobs.csv', 'eda_data_cleaned.csv')