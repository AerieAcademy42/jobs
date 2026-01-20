import os
import re
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timezone
from jobspy import scrape_jobs
from supabase import create_client, Client

# 1. Connection
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Queries including Govt Architecture/Planning exams
SEARCH_QUERIES = [
    "Architectural Assistant India",
    "Junior Architect Government India",
    "CPWD Architecture Recruitment",
    "ISRO Architect Jobs",
    "DDA Planning Assistant",
    "Architecture Recruitment Board",
    "Public Service Commission Architect India"
]

def clean_salary_text(val):
    """Prevents double Rupee signs by stripping old ones first"""
    if not val or str(val).lower() == "nan": return "Not specified"
    # Strip ₹, INR, Rs., Rs and spaces
    cleaned = str(val).replace('₹', '').replace('INR', '').replace('Rs.', '').replace('Rs', '').strip()
    if not cleaned: return "Not specified"
    return f"₹{cleaned}"

def clean_description(text):
    """Removes ** stars and fixes alignment/spacing"""
    if not text or str(text).lower() == "none": return ""
    # Remove markdown stars
    clean = str(text).replace('*', '')
    # Remove HTML tags and collapse whitespace/newlines into one space
    clean = re.sub('<[^<]+?>', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:1000]

def import_aerie_manual_jobs():
    """Imports Govt/Recommended jobs from Google Sheet"""
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
                "applyUrl": str(row.get('Apply Link')),
                "source": "Aerie Recommended",
                "employmentType": "Full-time",
                "discription": clean_description(row.get('Description', '')),
                "industry": "Architecture",
                "created_at": now_iso 
            }
            if job_data["title"] != 'nan':
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
    except Exception as e:
        print(f"Sheet Import Error: {e}")

def run_scraper():
    all_jobs_list = []
    for query in SEARCH_QUERIES:
        try:
            # Added "google" to site_name and results_wanted to 100
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=query,
                location="India",
                results_wanted=100, 
                hours_old=72, 
                country_indeed='India'
            )
            if not jobs.empty: all_jobs_list.append(jobs)
        except: continue

    if all_jobs_list:
        df = pd.concat(all_jobs_list).drop_duplicates(subset=['id']).replace({np.nan: None})
        current_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, row in df.iterrows():
            job_data = {
                "jobId": str(row['id']),
                "title": str(row['title']),
                "companyName": str(row['company']) or "Not specified",
                "location": str(row['location']) or "India",
                "salary": clean_salary_text(row.get('min_amount')),
                "postedDate": current_iso,
                "applyUrl": str(row['job_url']),
                "source": str(row['site']).capitalize(),
                "employmentType": "Full-time",
                "discription": clean_description(row.get('description', "")),
                "industry": "Architecture",
                "created_at": current_iso 
            }
            try:
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
            except: pass

    import_aerie_manual_jobs()

if __name__ == "__main__":
    run_scraper()
