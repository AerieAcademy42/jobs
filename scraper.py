import os
from datetime import datetime
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Configuration
SUPABASE_URL = os.environ.get("https://bkoniaoygybhrzlqmbnv.supabase.co")
SUPABASE_KEY = os.environ.get("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJrb25pYW95Z3liaHJ6bHFtYm52Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODcyOTIyOSwiZXhwIjoyMDc0MzA1MjI5fQ.aWoCc-XWTcaVJcBpbexOyezBxSnJyssUwwVjc_A8brY")
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
                hours_old=24, # Only get recent jobs to save time
                country_indeed='India'
            )
            all_jobs.append(jobs)
        except Exception as e:
            print(f"Error scraping {query}: {e}")

    if not all_jobs:
        return

    df = pd.concat(all_jobs)
    
    # Format data to match your Supabase schema
    for _, row in df.iterrows():
        job_data = {
            "jobId": str(row['id']),
            "title": row['title'],
            "companyName": row['company'] or "Not specified",
            "location": row['location'] or "India",
            "salary": f"{row['min_amount']} - {row['max_amount']}" if row['min_amount'] else "Not specified",
            "postedDate": row['date_posted'].isoformat() if pd.notnull(row['date_posted']) else datetime.now().isoformat(),
            "applyUrl": row['job_url'],
            "source": row['site'],
            "employmentType": clean_employment_type(row['job_type']),
            "description": (row['description'][:1000] if row['description'] else ""),
            "industry": "Architecture",
            "created_at": datetime.now().isoformat()
        }

        # Upsert into Supabase (prevents duplicates based on jobId)
        try:
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            print(f"Supabase Error: {e}")

if __name__ == "__main__":
    run_scraper()
