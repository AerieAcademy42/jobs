import os
from datetime import datetime, timezone
import numpy as np
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SEARCH_QUERIES = [
    "Architectural Assistant",
    "Landscape Architecture Intern",
    "Urban Planning Intern",
    "Architectural Drafter",
    "Junior Architect",
    "BIM Modeler"
]

def clean_employment_type(job_type):
    intern_types = ['part-time', 'internship', 'contract', 'temporary', 'volunteer']
    if any(x in str(job_type).lower() for x in intern_types):
        return 'Internship'
    return 'Full-time'

def run_scraper():
    all_jobs_list = []

    for query in SEARCH_QUERIES:
        print(f"Searching for {query}...")
        try:
            # Removed Glassdoor because it was blocking (403)
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=query,
                location="India",
                results_wanted=25,
                hours_old=72, 
                country_indeed='India'
            )
            if not jobs.empty:
                all_jobs_list.append(jobs)
                print(f"Found {len(jobs)} results for {query}")
        except Exception as e:
            print(f"Error scraping {query}: {e}")

    if not all_jobs_list:
        print("No jobs found in this run.")
        return

    df = pd.concat(all_jobs_list)
    df = df.drop_duplicates(subset=['id'])
    
    # IMPORTANT: This replaces all "NaN" (Not a Number) with None 
    # This fixes the "Out of range float values" error
    df = df.replace({np.nan: None})

    print(f"Total unique jobs to process: {len(df)}")

    for _, row in df.iterrows():
        now_iso = datetime.now(timezone.utc).isoformat()
        
        # Date Handling
        try:
            if row.get('date_posted'):
                posted_date = pd.to_datetime(row['date_posted']).isoformat()
            else:
                posted_date = now_iso
        except:
            posted_date = now_iso

        # Salary Handling (Fixes the Float Error)
        salary_val = "Not specified"
        if row.get('min_amount') and row.get('max_amount'):
            salary_val = f"₹{row['min_amount']} - ₹{row['max_amount']}"
        elif row.get('min_amount'):
            salary_val = f"₹{row['min_amount']}"

        job_data = {
            "jobId": str(row['id']),
            "title": str(row['title']),
            "companyName": str(row['company']) if row['company'] else "Not specified",
            "location": str(row['location']) if row['location'] else "India",
            "salary": salary_val,
            "postedDate": posted_date,
            "applyUrl": str(row['job_url']),
            "source": str(row['site']),
            "employmentType": clean_employment_type(row.get('job_type', "")),
            "discription": str(row.get('description', ""))[:1000],
            "industry": "Architecture",
            "created_at": posted_date
        }

        try:
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            # Printing the error help us see which specific field is failing
            print(f"Supabase Error for job {row['id']}: {e}")

    print("Scrape and Upload Complete.")

if __name__ == "__main__":
    run_scraper()
