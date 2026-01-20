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

SEARCH_QUERIES = [
    "Architectural Assistant",
    "Landscape Architecture Intern",
    "Urban Planning Intern",
    "Architectural Drafter",
    "Junior Architect",
    "BIM Modeler",
    "Interior Designer India"
]

def clean_salary_text(val):
    """Removes existing currency symbols to prevent double signs like ₹₹"""
    if not val or str(val).lower() == "nan": return "Not specified"
    # Remove existing symbols/text so we can standardize
    cleaned = str(val).replace('₹', '').replace('INR', '').replace('Rs.', '').strip()
    # Handle cases where the text might still be empty after stripping
    if not cleaned: return "Not specified"
    return f"₹{cleaned}"

def extract_salary_from_text(text):
    """Scans description for salary if not in official field"""
    if not text: return None
    patterns = [
        r"(?:₹|INR|Rs\.?)\s?(\d+(?:,\d+)*(?:\s?-\s?\d+(?:,\d+)*)?)",
        r"(\d+(?:\.\d+)?\s?LPA)",
        r"(\d+k\s?-\s?\d+k)",
        r"Salary[:\s]+(\d+(?:,\d+)*)"
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_salary_text(match.group(0))
    return None

def clean_description_text(text):
    """Fixes alignment by removing extra whitespace/newlines"""
    if not text or str(text).lower() == "none": return ""
    # Remove HTML tags
    clean = re.sub('<[^<]+?>', '', str(text))
    # Replace all whitespace (tabs, newlines, multiple spaces) with a single space
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:1000]

def clean_employment_type(job_type):
    intern_types = ['part-time', 'internship', 'contract', 'temporary', 'volunteer']
    if any(x in str(job_type).lower() for x in intern_types):
        return 'Internship'
    return 'Full-time'

def import_aerie_manual_jobs():
    """Imports from Google Sheet CSV and maps to your Supabase keys"""
    csv_url = os.environ.get("AERIE_SHEET_URL")
    if not csv_url:
        print("Aerie URL not set in GitHub Secrets.")
        return

    try:
        response = requests.get(csv_url)
        # Use pandas to read the CSV
        df = pd.read_csv(StringIO(response.text))
        
        # If there are duplicate "Timestamp" columns, we rename them to be unique
        # This solves your "Timestamp twice" issue
        cols = []
        count = {}
        for column in df.columns:
            if column in count:
                count[column] += 1
                cols.append(f"{column}_{count[column]}")
            else:
                count[column] = 0
                cols.append(column)
        df.columns = cols

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, row in df.iterrows():
            # We use row.get() with the exact column names from your Sheet
            job_data = {
                "jobId": f"manual_{row.iloc[0]}", # Uses the very first column (Automatic Timestamp)
                "title": str(row.get('Job Title', 'Untitled')),
                "companyName": str(row.get('Company', 'Not specified')),
                "location": str(row.get('Location', 'India')),
                "salary": clean_salary_text(row.get('Salary', 'Not specified')),
                "postedDate": now_iso,
                "applyUrl": str(row.get('Apply Link', '')),
                "source": "Aerie",
                "employmentType": clean_employment_type(row.get('Type', 'Full-time')),
                "discription": clean_description_text(row.get('Description', '')),
                "industry": "Architecture",
                "created_at": now_iso 
            }
            # Only upsert if there is a title
            if job_data["title"] != 'nan' and job_data["applyUrl"] != 'nan':
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
        
        print("Aerie manual jobs processing finished.")
    except Exception as e:
        print(f"Aerie Import Error: {e}")

def run_scraper():
    all_jobs_list = []
    for query in SEARCH_QUERIES:
        print(f"Searching {query}...")
        try:
            # 100 results per query
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
        except Exception as e:
            print(f"Scraper Error for {query}: {e}")

    if all_jobs_list:
        df = pd.concat(all_jobs_list).drop_duplicates(subset=['id']).replace({np.nan: None})
        now = datetime.now(timezone.utc)
        current_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, row in df.iterrows():
            # Salary Fix: Prevent double rupee
            official_salary = None
            if row.get('min_amount'):
                # Strip existing symbols before adding our own
                min_val = str(row['min_amount']).replace('₹', '').replace('INR', '').strip()
                official_salary = f"₹{min_val}"
                if row.get('max_amount'):
                    max_val = str(row['max_amount']).replace('₹', '').replace('INR', '').strip()
                    official_salary += f" - {max_val}"
            
            desc_raw = str(row.get('description', ""))
            found_salary = extract_salary_from_text(desc_raw)
            salary_to_show = official_salary if official_salary else (found_salary if found_salary else "Not specified")

            # Description alignment fix
            clean_desc = clean_description_text(desc_raw)
            if len(clean_desc) < 20:
                clean_desc = f"Architecture opportunity at {row['company']}. Please visit the link for full details."

            # Date Logic
            try:
                posted_date = pd.to_datetime(row['date_posted']).strftime("%Y-%m-%dT%H:%M:%SZ") if row.get('date_posted') else current_iso
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
                "created_at": current_iso 
            }

            try:
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
            except Exception:
                pass

    # Run the manual import from Google Sheets
    import_aerie_manual_jobs()

if __name__ == "__main__":
    run_scraper()
