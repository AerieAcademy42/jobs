import os
from datetime import datetime, timezone
import numpy as np
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Supabase Connection
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 2. Search Parameters
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
            # Using Indeed, LinkedIn, and Google for maximum coverage in India
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
                print(f"Found {len(jobs)} jobs for {query}")
        except Exception as e:
            print(f"Scraper error for {query}: {e}")

    if not all_jobs_list:
        print("No jobs found in this run.")
        return

    # Process results
    df = pd.concat(all_jobs_list)
    df = df.drop_duplicates(subset=['id'])
    
    # Critical Fix: Turn "NaN" floats into None so Supabase doesn't error
    df = df.replace({np.nan: None})
    
    print(f"Total unique jobs to process: {len(df)}")

    # Time tracking for date fixes
    now = datetime.now(timezone.utc)
    current_iso_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for _, row in df.iterrows():
        
        # 3. Fix Date Formatting (Removes "NaN years ago")
        try:
            if row.get('date_posted'):
                p_date = pd.to_datetime(row['date_posted']).replace(tzinfo=timezone.utc)
                # Prevent future dates from pinning to the top
                if p_date > now:
                    posted_date = current_iso_time
                else:
                    posted_date = p_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                posted_date = current_iso_time
        except:
            posted_date = current_iso_time

        # 4. Map to your Supabase Schema
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
            "discription": str(row.get('description', ""))[:1000], # Your DB spelling
            "industry": "Architecture",
            "experienceLevel": "Entry Level",
            "scrap date": datetime.now().strftime("%Y-%m-%d"), # Matching your old n8n column
            "created_at": posted_date 
        }

        try:
            # Upsert ensures no duplicates if jobId is unique/Primary Key
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            print(f"Supabase error for {row['id']}: {e}")

    print("Scrape and Upload Complete.")

if __name__ == "__main__":
    run_scraper()
