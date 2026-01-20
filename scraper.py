import os
import re
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timezone
from jobspy import scrape_jobs
from supabase import create_client, Client

# Connection
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

SEARCH_QUERIES = [
    "Architectural Assistant",
    "Junior Architect",
    "BIM Modeler India",
    "Interior Designer India",
    "Government Architecture Jobs India",  # Added Govt
    "CPWD Architect Recruitment",          # Added Govt
    "DDA Planning Assistant"               # Added Govt
]

def clean_salary_text(val):
    if not val or str(val).lower() == "nan": return "Not specified"
    # Aggressively strip all known currency markers
    cleaned = str(val).replace('₹', '').replace('INR', '').replace('Rs.', '').replace('Rs', '').strip()
    if not cleaned: return "Not specified"
    return f"₹{cleaned}"

def clean_text_formatting(text):
    """Removes ** stars, fixes alignment, removes extra whitespace"""
    if not text or str(text).lower() == "none": return ""
    # Remove stars (markdown)
    clean = str(text).replace('*', '')
    # Remove HTML
    clean = re.sub('<[^<]+?>', '', clean)
    # Collapse all newlines and tabs into a single space
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:1000]

def clean_employment_type(job_type):
    intern_types = ['part-time', 'internship', 'contract', 'temporary', 'volunteer']
    if any(x in str(job_type).lower() for x in intern_types):
        return 'Internship'
    return 'Full-time'

def import_aerie_manual_jobs():
    """Imports Govt/Recommended jobs from your Google Sheet"""
    csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtE8sIN-gq8QvZCrKCBxHe0iTRvjV-YCKg51R3xl83B0dJ56RwIsbImpvitQMqkiz1IW3m7mcQTuD0/pub?gid=717563757&single=true&output=csv"
    try:
        response = requests.get(csv_url)
        df = pd.read_csv(StringIO(response.text))
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, row in df.iterrows():
            job_data = {
                "jobId": f"manual_{row.iloc[0]}",
                "title": str(row.get('Job Title', 'Untitled')),
                "companyName": str(row.get('Company', 'Aerie Recommended')),
                "location": str(row.get('Location', 'India')),
                "salary": clean_salary_text(row.get('Salary')),
                "postedDate": now_iso,
                "applyUrl": str(row.get('Apply Link', '')),
                "source": "Aerie Recommended", # Updated Source Name
                "employmentType": clean_employment_type(row.get('Type', 'Full-time')),
                "discription": clean_text_formatting(row.get('Description', '')),
                "industry": "Architecture",
                "created_at": now_iso 
            }
            if job_data["title"] != 'nan':
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
    except Exception as e:
        print(f"Sheet Error: {e}")

def run_scraper():
    all_jobs_list = []
    for query in SEARCH_QUERIES:
        try:
            # results_wanted=100, includes google
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=query,
                location="India",
                results_wanted=100, 
                hours_old=72, 
                country_indeed='India'
            )
            if not jobs.empty:
                all_jobs_list.append(jobs)
        except:
            continue

    if all_jobs_list:
        df = pd.concat(all_jobs_list).drop_duplicates(subset=['id']).replace({np.nan: None})
        current_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, row in df.iterrows():
            # Salary strip logic
            official_salary = None
            if row.get('min_amount'):
                min_s = str(row['min_amount']).replace('₹', '').replace('INR', '').strip()
                official_salary = f"₹{min_s}"
            
            job_data = {
                "jobId": str(row['id']),
                "title": str(row['title']),
                "companyName": str(row['company']) or "Not specified",
                "location": str(row['location']) or "India",
                "salary": official_salary or "Not specified",
                "postedDate": current_iso,
                "applyUrl": str(row['job_url']),
                "source": str(row['site']).capitalize(),
                "employmentType": clean_employment_type(row.get('job_type', "")),
                "discription": clean_text_formatting(row.get('description', "")),
                "industry": "Architecture",
                "created_at": current_iso 
            }
            try:
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
            except:
                pass

    import_aerie_manual_jobs()

if __name__ == "__main__":
    run_scraper()
