import sys
import textwrap
from dal import JobPostingsDAL

# Initialize the Data Access Layer
# We wrap this in a try/except block to catch connection errors immediately.
try:
    dal = JobPostingsDAL()
except Exception as e:
    print(f"CRITICAL ERROR: Could not connect to the database.\nDetails: {e}")
    sys.exit(1)

def clear_screen():
    print("\n" + "="*60 + "\n")

def get_input(prompt, required=True, val_func=None):
    """
    Helper to get and validate user input.
    - required: If True, loops until non-empty input is given.
    - val_func: A function that converts/validates the input (e.g., int).
    """
    while True:
        value = input(prompt).strip()
        
        if not value and not required:
            return None
        
        if not value and required:
            print("Error: This field is required.")
            continue
            
        if val_func:
            try:
                return val_func(value)
            except ValueError:
                print(f"Error: Invalid format. Please try again.")
                continue
        
        return value

def print_table(headers, rows, col_widths):
    """Simple helper to print tabular data."""
    header_str = " | ".join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
    print("-" * len(header_str))
    print(header_str)
    print("-" * len(header_str))
    
    for row in rows:
        print(" | ".join(f"{str(item):<{w}}" for item, w in zip(row, col_widths)))


# FEATURE DEMONSTRATIONS

def analyze_industries():
    print("\n--- Industry Salary Analysis ---")
    threshold = get_input("Enter minimum job count threshold [Default: 10]: ", required=False, val_func=int)
    if threshold is None: 
        threshold = 10

    try:
        results = dal.get_industry_salary_analysis(min_jobs_threshold=threshold)
        
        if not results:
            print("\nNo industries found meeting that threshold.")
            return

        print(f"\nFound {len(results)} industries with >{threshold} jobs:\n")
        
        # Prepare data for printing
        headers = ["Industry", "Avg Sal ($K)", "Min ($K)", "Max ($K)", "Count"]
        widths = [35, 12, 10, 10, 8]
        rows = []
        for r in results:
            rows.append([
                r['industry'][:33], # Truncate long names
                r['avg_salary'],
                r['avg_min_salary'],
                r['avg_max_salary'],
                r['job_count']
            ])
        
        print_table(headers, rows, widths)

    except Exception as e:
        print(f"Error executing analysis: {e}")

def analyze_location():
    print("\n--- Location Skill Demand Analysis ---")
    print("1. View list of available locations")
    print("2. Analyze a specific location")
    
    choice = input("Select option: ")
    
    if choice == '1':
        locs = dal.get_unique_locations()
        print("\nAvailable Locations:")
        # Print in columns of 3 to save space
        for i in range(0, len(locs), 3):
            print("   ".join(f"{loc:<25}" for loc in locs[i:i+3]))
        print("\n")
        return # Return to menu to let them choose option 2 next

    if choice == '2':
        city = get_input("Enter City (e.g., San Francisco): ")
        state = get_input("Enter State Abbreviation (e.g., CA): ")
        
        try:
            data = dal.get_location_skill_demand(city, state)
            
            if not data:
                print(f"\nNo data found for {city}, {state}.")
                return

            clear_screen()
            print(f"REPORT: {data['location']}")
            print(f"Total Jobs Tracked: {data['total_jobs']}")
            print(f"Average Salary:     ${data['avg_salary']}k")
            print("-" * 30)
            print("SKILL DEMAND (% of jobs req. skill):")
            print(f"  Python: {data['skill_demand']['python_pct']}%")
            print(f"  AWS:    {data['skill_demand']['aws_pct']}%")
            print(f"  Spark:  {data['skill_demand']['spark_pct']}%")
            print("-" * 30)
            print("TOP HIRING COMPANIES:")
            for comp in data['top_companies']:
                print(f"  - {comp}")
                
        except Exception as e:
            print(f"Error analyzing location: {e}")

def search_jobs():
    print("\n--- Search Job Postings ---")
    term = get_input("Enter title keyword (e.g., 'Analyst'): ")
    
    try:
        results = dal.search_job_postings_readable(term)
        
        if not results:
            print("No jobs found matching that keyword.")
            return

        print(f"\nFound {len(results)} matches:\n")
        headers = ["ID", "Title", "Company", "Location"]
        widths = [6, 30, 25, 20]
        rows = [[r['id'], r['title'][:28], r['company'][:23], r['location']] for r in results]
        print_table(headers, rows, widths)
        
    except Exception as e:
        print(f"Search failed: {e}")

def create_job():
    print("\n--- Create New Job Posting ---")
    try:
        title = get_input("Job Title: ")
        company = get_input("Company Name: ")
        city = get_input("City: ")
        state = get_input("State (Abbr): ")
        min_sal = get_input("Min Salary ($K): ", val_func=int)
        max_sal = get_input("Max Salary ($K): ", val_func=int)
        
        if min_sal > max_sal:
            print("Warning: Min salary was greater than Max. Swapping values.")
            min_sal, max_sal = max_sal, min_sal

        python = get_input("Requires Python? (y/n): ").lower().startswith('y')
        desc = get_input("Short Description (optional): ", required=False)

        job_id = dal.create_job_posting(
            job_title=title,
            company_name=company,
            city=city,
            state=state,
            min_salary=min_sal,
            max_salary=max_sal,
            python_yn=python,
            job_description=desc or "No description provided."
        )
        print(f"\nSUCCESS: Job created with ID: {job_id}")
        
    except Exception as e:
        print(f"Error creating job: {e}")

def view_job_details():
    print("\n--- View Job Details ---")
    jid = get_input("Enter Job ID: ", val_func=int)
    
    try:
        job = dal.get_job_posting_by_id(jid)
        if not job:
            print("Error: Job ID not found.")
            return

        print("\n" + "*"*40)
        print(f"Title:    {job['title']}")
        print(f"Company:  {job['company']}")
        print(f"Location: {job['location']}")
        print(f"Salary:   {job['salary_range']}")
        print("-" * 40)
        print("Description Snippet:")
        print(textwrap.fill(job['description'], width=40))
        print("*"*40)
        
    except Exception as e:
        print(f"Error retrieving job: {e}")

def update_job():
    print("\n--- Update Job Posting ---")
    jid = get_input("Enter Job ID to update: ", val_func=int)
    
    # Check existence first
    if not dal.get_job_posting_by_id(jid):
        print("Error: Job ID not found.")
        return

    print("Leave fields blank to keep current value.")
    
    try:
        new_min = get_input("New Min Salary (or enter to skip): ", required=False, val_func=int)
        new_max = get_input("New Max Salary (or enter to skip): ", required=False, val_func=int)
        
        updates = {}
        if new_min is not None: updates['min_salary'] = new_min
        if new_max is not None: updates['max_salary'] = new_max
        
        if not updates:
            print("No changes requested.")
            return

        dal.update_job_posting(jid, **updates)
        print("SUCCESS: Job updated.")
        
    except Exception as e:
        print(f"Update failed: {e}")

def delete_job():
    print("\n--- Delete Job Posting ---")
    jid = get_input("Enter Job ID to delete: ", val_func=int)
    
    confirm = get_input(f"Are you sure you want to delete Job {jid}? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Deletion cancelled.")
        return

    try:
        success = dal.delete_job_posting(jid)
        if success:
            print(f"SUCCESS: Job {jid} deleted.")
        else:
            print("Error: Job ID not found.")
    except Exception as e:
        print(f"Deletion failed: {e}")


# MAIN MENU LOOP

def main_menu():
    while True:
        print("\n" + "="*30)
        print("   JOB MARKET ANALYZER CLI")
        print("="*30)
        print("1. Analyze Industry Salaries")
        print("2. Analyze Location Skills")
        print("3. Search Jobs")
        print("4. View Job Details")
        print("5. Create New Job")
        print("6. Update Job Salary")
        print("7. Delete Job")
        print("8. Exit")
        
        choice = input("\nSelect an option (1-8): ").strip()
        
        if choice == '1':
            analyze_industries()
        elif choice == '2':
            analyze_location()
        elif choice == '3':
            search_jobs()
        elif choice == '4':
            view_job_details()
        elif choice == '5':
            create_job()
        elif choice == '6':
            update_job()
        elif choice == '7':
            delete_job()
        elif choice == '8':
            print("Exiting...")
            sys.exit(0)
        else:
            print("Invalid selection. Please try again.")

if __name__ == "__main__":
    main_menu()