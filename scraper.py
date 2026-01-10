import os
from datetime import datetime, timezone
import numpy as np
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Connection
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
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=query,
                location="India",
                results_wanted=30,
                hours_old=72, 
                country_indeed='India'
            )
            if not jobs.empty:
                all_jobs_list.append(jobs)
        except Exception as e:
            print(f"Error: {e}")

    if not all_jobs_list: return

    df = pd.concat(all_jobs_list).drop_duplicates(subset=['id']).replace({np.nan: None})
    now = datetime.now(timezone.utc)
    current_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for _, row in df.iterrows():
        # Date Logic
        try:
            if row.get('date_posted'):
                p_date = pd.to_datetime(row['date_posted']).replace(tzinfo=timezone.utc)
                posted_date = p_date.strftime("%Y-%m-%dT%H:%M:%SZ") if p_date <= now else current_iso
            else:
                posted_date = current_iso
        except:
            posted_date = current_iso

        # Data map - REMOVED experienceLevel and scrap date to prevent errors
        job_data = {
            "jobId": str(row['id']),
            "title": str(row['title']),
            "companyName": str(row['company']) if row['company'] else "Not specified",
            "location": str(row['location']) if row['location'] else "India",
            "salary": f"₹{row['min_amount']}" if row.get('min_amount') else "Not specified",
            "postedDate": posted_date,
            "applyUrl": str(row['job_url']),
            "source": str(row['site']).capitalize(),
            "employmentType": clean_employment_type(row.get('job_type', "")),
            "discription": str(row.get('description', ""))[:1000],
            "industry": "Architecture",
            "created_at": posted_date 
        }

        try:
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            print(f"Supabase error for {row['id']}: {e}")

    print("Scrape and Upload Complete.")

if __name__ == "__main__":
    run_scraper()
