import os
from datetime import datetime, timezone
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Expanded search queries for more results
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
            # Added more sites (google, glassdoor) to get more data
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google", "glassdoor"],
                search_term=query,
                location="India",
                results_wanted=20,
                hours_old=72, # Looking at last 3 days to ensure we find jobs
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
    df = df.drop_duplicates(subset=['id']) # Remove duplicates found across queries
    
    print(f"Total unique jobs to process: {len(df)}")

    for _, row in df.iterrows():
        # FIX FOR "NaN years ago": Ensure we have a valid ISO timestamp
        # If the scraper doesn't find a date, we use the current time.
        now_iso = datetime.now(timezone.utc).isoformat()
        
        try:
            if pd.notnull(row.get('date_posted')):
                # Convert the date_posted to a proper ISO string
                posted_date = pd.to_datetime(row['date_posted']).isoformat()
            else:
                posted_date = now_iso
        except:
            posted_date = now_iso

        # Clean Description
        raw_desc = row.get('description', "")
        clean_desc = str(raw_desc)[:1000] if pd.notnull(raw_desc) else ""

        job_data = {
            "jobId": str(row['id']),
            "title": row['title'],
            "companyName": row['company'] or "Not specified",
            "location": row['location'] or "India",
            "salary": f"{row['min_amount']} - {row['max_amount']}" if row.get('min_amount') else "Not specified",
            "postedDate": posted_date,
            "applyUrl": row['job_url'],
            "source": row['site'],
            "employmentType": clean_employment_type(row.get('job_type', "")),
            "discription": clean_desc,
            "industry": "Architecture",
            "created_at": posted_date # Frontend usually uses created_at for the "X days ago"
        }

        try:
            # on_conflict="jobId" ensures we update old jobs and add new ones
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            print(f"Supabase Error: {e}")

    print("Scrape and Upload Complete.")

if __name__ == "__main__":
    run_scraper()
