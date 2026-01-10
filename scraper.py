import os
from datetime import datetime
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
    "Architectural Drafter"
]

def clean_employment_type(job_type):
    intern_types = ['part-time', 'internship', 'contract', 'temporary', 'volunteer']
    if any(x in str(job_type).lower() for x in intern_types):
        return 'Internship'
    return 'Full-time'

def run_scraper():
    all_jobs = []

    for query in SEARCH_QUERIES:
        print(f"Searching for {query}...")
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=query,
                location="India",
                results_wanted=30,
                hours_old=24, 
                country_indeed='India'
            )
            if not jobs.empty:
                all_jobs.append(jobs)
        except Exception as e:
            print(f"Error scraping {query}: {e}")

    if not all_jobs:
        print("No jobs found today.")
        return

    df = pd.concat(all_jobs)
    
    # Format data to match your Supabase schema exactly
    for _, row in df.iterrows():
        
        # Robustly handle the description field to avoid the 'float' error
        raw_desc = row.get('description', "")
        if pd.isna(raw_desc):
            clean_desc = ""
        else:
            clean_desc = str(raw_desc)[:1000]

        job_data = {
            "jobId": str(row['id']),
            "title": row['title'],
            "companyName": row['company'] or "Not specified",
            "location": row['location'] or "India",
            "salary": f"{row['min_amount']} - {row['max_amount']}" if row.get('min_amount') else "Not specified",
            "postedDate": row['date_posted'].isoformat() if pd.notnull(row['date_posted']) else datetime.now().isoformat(),
            "applyUrl": row['job_url'],
            "source": row['site'],
            "employmentType": clean_employment_type(row.get('job_type', "")),
            "discription": clean_desc, # FIXED spelling to match your database
            "industry": "Architecture",
            "created_at": datetime.now().isoformat()
        }

        # Upsert into Supabase
        try:
            # Note: ensure "jobId" is set as a Primary Key or Unique constraint in Supabase
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            print(f"Supabase Error for job {row['id']}: {e}")

if __name__ == "__main__":
    run_scraper()
