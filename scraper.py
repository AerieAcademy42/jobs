import os
import re
from datetime import datetime, timezone
import numpy as np
from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client, Client

# 1. Connection
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

SEARCH_QUERIES = [
    "Architectural Assistant",
    "Landscape Architecture Intern",
    "Urban Planning Intern",
    "Architectural Drafter",
    "Junior Architect",
    "BIM Modeler",
    "Interior Designer India"
]

def extract_salary_from_text(text):
    """Scans the job description for salary patterns like 3LPA, 25k, etc."""
    if not text: return None
    # Pattern looks for: ₹, INR, LPA, or patterns like 25,000 - 30,000
    patterns = [
        r"(?:₹|INR|Rs\.?)\s?(\d+(?:,\d+)*(?:\s?-\s?\d+(?:,\d+)*)?)", # ₹25,000 - 30,000
        r"(\d+(?:\.\d+)?\s?LPA)", # 3.5 LPA
        r"(\d+k\s?-\s?\d+k)", # 20k - 25k
        r"Salary[:\s]+(\d+(?:,\d+)*)" # Salary: 20000
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None

def clean_employment_type(job_type):
    intern_types = ['part-time', 'internship', 'contract', 'temporary', 'volunteer']
    if any(x in str(job_type).lower() for x in intern_types):
        return 'Internship'
    return 'Full-time'

def run_scraper():
    all_jobs_list = []
    for query in SEARCH_QUERIES:
        print(f"Searching {query}...")
        try:
            # Google Jobs aggregates Naukri, Foundit, etc.
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=query,
                location="India",
                results_wanted=40, 
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
        # 1. Salary Logic: Check official field first, then scan description
        official_salary = None
        if row.get('min_amount'):
            official_salary = f"₹{row['min_amount']} - {row['max_amount']}" if row.get('max_amount') else f"₹{row['min_amount']}"
        
        description_text = str(row.get('description', ""))
        found_salary = extract_salary_from_text(description_text)
        
        salary_to_show = official_salary if official_salary else (found_salary if found_salary else "Not specified")

        # 2. Description Logic: Ensure it's never "None"
        if description_text and len(description_text) > 20 and description_text.lower() != "none":
            clean_desc = description_text[:1000]
        else:
            clean_desc = f"Architecture opportunity at {row['company']}. Please visit the link for full job details and requirements."

        # Date Logic
        try:
            if row.get('date_posted'):
                p_date = pd.to_datetime(row['date_posted']).replace(tzinfo=timezone.utc)
                posted_date = p_date.strftime("%Y-%m-%dT%H:%M:%SZ") if p_date <= now else current_iso
            else:
                posted_date = current_iso
        except:
            posted_date = current_iso

        job_data = {
            "jobId": str(row['id']),
            "title": str(row['title']),
            "companyName": str(row['company']) if row['company'] else "Not specified",
            "location": str(row['location']) if row['location'] else "India",
            "salary": salary_to_show,
            "postedDate": posted_date,
            "applyUrl": str(row['job_url']),
            "source": str(row['site']).capitalize(),
            "employmentType": clean_employment_type(row.get('job_type', "")),
            "discription": clean_desc,
            "industry": "Architecture",
            "created_at": posted_date 
        }

        try:
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        except Exception as e:
            pass

    print(f"Scrape Complete. Processed {len(df)} jobs.")

if __name__ == "__main__":
    run_scraper()
