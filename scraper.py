import os
import re
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timezone
from jobspy import scrape_jobs
from supabase import create_client, Client

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Strict filter to stop AI/IT jobs
BLACKLIST = ["software", "developer", "python", "javascript", "ai developer", "fin crime", "engineer -", "full stack"]

SEARCH_QUERIES = [
    "Architectural Assistant India",
    "Junior Architect India",
    "CPWD Architect recruitment",
    "DDA Planning Assistant",
    "Urban Planner India",
    "Interior Designer recruitment"
]

def is_valid_architecture_job(title):
    title_lower = str(title).lower()
    if any(word in title_lower for word in BLACKLIST):
        return False
    # Must contain one of these
    arch_keywords = ["arch", "interior", "design", "urban", "bim", "drafter", "planner", "landscape"]
    return any(word in title_lower for word in arch_keywords)

def clean_salary(val):
    if not val or str(val).lower() == "nan": return "Not specified"
    cleaned = str(val).replace('₹', '').replace('INR', '').replace('Rs.', '').replace('Rs', '').strip()
    return f"₹{cleaned}"

def clean_desc(text):
    if not text: return ""
    clean = str(text).replace('*', '') # Remove Stars
    return re.sub(r'\s+', ' ', clean).strip()[:1000]

def import_manual_sheet():
    csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtE8sIN-gq8QvZCrKCBxHe0iTRvjV-YCKg51R3xl83B0dJ56RwIsbImpvitQMqkiz1IW3m7mcQTuD0/pub?gid=717563757&single=true&output=csv"
    try:
        r = requests.get(csv_url)
        df = pd.read_csv(StringIO(r.text))
        now = datetime.now(timezone.utc).isoformat()
        for _, row in df.iterrows():
            comp = str(row.get('Company', '')).upper()
            # Professional labeling
            source_label = "Public Sector (Govt & PSU)" if any(x in comp for x in ["CPWD", "DDA", "ISRO", "GOVT"]) else "Aerie Curated Roles"
            
            job_data = {
                "jobId": f"manual_{row.iloc[0]}",
                "title": str(row.get('Job Title')),
                "companyName": str(row.get('Company', 'Aerie Academy')),
                "location": str(row.get('Location', 'India')),
                "salary": clean_salary(row.get('Salary')),
                "postedDate": now,
                "applyUrl": str(row.get('Apply Link')),
                "source": source_label,
                "employmentType": "Full-time",
                "discription": clean_desc(row.get('Description')),
                "industry": "Architecture",
                "created_at": now
            }
            supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
    except: pass

def run_scraper():
    all_jobs = []
    for q in SEARCH_QUERIES:
        try:
            jobs = scrape_jobs(site_name=["indeed", "linkedin", "google"], search_term=q, location="India", results_wanted=100)
            if not jobs.empty: all_jobs.append(jobs)
        except: continue

    if all_jobs:
        df = pd.concat(all_jobs).drop_duplicates(subset=['id']).replace({np.nan: None})
        now = datetime.now(timezone.utc).isoformat()
        for _, row in df.iterrows():
            if is_valid_architecture_job(row['title']):
                job_data = {
                    "jobId": str(row['id']),
                    "title": str(row['title']),
                    "companyName": str(row['company']) or "Company",
                    "location": str(row['location']) or "India",
                    "salary": clean_salary(row.get('min_amount')),
                    "postedDate": now,
                    "applyUrl": str(row['job_url']),
                    "source": str(row['site']).capitalize(),
                    "employmentType": "Full-time",
                    "discription": clean_desc(row.get('description')),
                    "industry": "Architecture",
                    "created_at": now 
                }
                try: supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
                except: pass
    import_manual_sheet()

if __name__ == "__main__":
    run_scraper()
