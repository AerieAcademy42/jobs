import os
import re
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timezone
from jobspy import scrape_jobs
from supabase import create_client, Client

# --- CONFIGURATION ---
# Ensure you have these in your .env or environment variables
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Strict filter to stop AI/IT jobs from automated scraper
BLACKLIST = ["software", "developer", "python", "javascript", "ai developer", "fin crime", "engineer -", "full stack"]

# Automated Search Queries
SEARCH_QUERIES = [
    "Architectural Assistant India",
    "Junior Architect India",
    "CPWD Architect recruitment",
    "DDA Planning Assistant",
    "Urban Planner India",
    "Interior Designer recruitment"
]

# If the Company Name in your Google Sheet contains these, it becomes a "Govt" job
GOVT_KEYWORDS = ["CPWD", "DDA", "ISRO", "NBCC", "UPSC", "MUNICIPAL", "CORPORATION", "GOVT", "PUBLIC WORKS", "AUTHORITY", "COUNCIL", "RAILWAY"]

def is_valid_architecture_job(title):
    title_lower = str(title).lower()
    if any(word in title_lower for word in BLACKLIST):
        return False
    arch_keywords = ["arch", "interior", "design", "urban", "bim", "drafter", "planner", "landscape"]
    return any(word in title_lower for word in arch_keywords)

def clean_salary(val):
    if not val or pd.isna(val) or str(val).lower() in ["nan", "none", ""]: 
        return "Not specified"
    cleaned = str(val).replace('₹', '').replace('INR', '').replace('Rs.', '').replace('Rs', '').strip()
    return f"₹{cleaned}"

def clean_text(text):
    if not text or pd.isna(text) or str(text).lower() in ["nan", "none", ""]: 
        return ""
    clean = str(text).replace('*', '') 
    return re.sub(r'\s+', ' ', clean).strip()

def is_valid_field(val):
    """Checks if a Google Sheet cell actually has data"""
    if val is None: return False
    if pd.isna(val): return False
    s = str(val).strip().lower()
    return s not in ["", "nan", "none", "null"]

def format_govt_description(desc, seats, exams, opening_date):
    """Builds a custom description for Govt jobs."""
    header = ""
    if is_valid_field(opening_date):
        header += f"📅 **Opening Date:** {str(opening_date).strip()}\n"
    if is_valid_field(seats):
        header += f"🪑 **Number of Seats:** {str(seats).strip()}\n"
    if is_valid_field(exams):
        header += f"📝 **Exam/Selection:** {str(exams).strip()}\n"
    
    base_desc = clean_text(desc)
    if header:
        return f"{header}\n---\n{base_desc}"
    return base_desc

def import_google_sheet():
    # Update with your specific Sheet ID and GID
    SHEET_ID = "1JAnklMpeGZYnhqvJk5LcUJngpkFgjnM9yfsacoFIX5Y"
    GID = "717563757"
    csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    
    print("Fetching Google Sheet...")
    try:
        r = requests.get(csv_url)
        r.raise_for_status()
        
        # Read CSV, ensure all columns are strings
        df = pd.read_csv(StringIO(r.text), dtype=str)
        df.columns = df.columns.str.strip()
        
        now = datetime.now(timezone.utc).isoformat()
        count = 0
        
        for index, row in df.iterrows():
            if not is_valid_field(row.get('Job Title')) or not is_valid_field(row.get('Apply Link')):
                continue

            title = str(row.get('Job Title')).strip()
            comp = str(row.get('Company', 'Aerie Academy')).strip()
            
            # --- Source Logic ---
            is_govt = any(k in comp.upper() for k in GOVT_KEYWORDS)
            
            # UNIFIED SOURCE NAMES HERE
            if is_govt:
                source_label = "Govt Jobs"
                final_desc = format_govt_description(
                    row.get('Description'), 
                    row.get('Seats'), 
                    row.get('Exams'), 
                    row.get('Opening Date')
                )
            else:
                source_label = "Aerie Recommended"
                final_desc = clean_text(row.get('Description'))

            unique_id = f"manual_{abs(hash(row.get('Apply Link')))}"

            job_data = {
                "jobId": unique_id,
                "title": title,
                "companyName": comp,
                "location": str(row.get('Location', 'India')),
                "salary": clean_salary(row.get('Salary')),
                "postedDate": now,
                "applyUrl": str(row.get('Apply Link')),
                "source": source_label,
                "employmentType": str(row.get('Type', 'Full-time')),
                "discription": final_desc,
                "industry": "Architecture",
                "created_at": now
            }
            
            try:
                supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
                count += 1
            except Exception as e:
                print(f"Error inserting {title}: {e}")
        
        print(f"Successfully processed {count} jobs from Sheet.")
                
    except Exception as e:
        print(f"Failed to process Google Sheet: {e}")

def run_scraper():
    print("--- Starting Automated Scraper ---")
    all_jobs = []
    
    # 1. Automated Scrape (JobSpy)
    for q in SEARCH_QUERIES:
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor"], 
                search_term=q, 
                location="India", 
                results_wanted=15, 
                hours_old=72
            )
            if not jobs.empty: all_jobs.append(jobs)
        except Exception as e:
            print(f"Error scraping {q}: {e}")
            continue

    if all_jobs:
        df = pd.concat(all_jobs).drop_duplicates(subset=['id']).replace({np.nan: None})
        now = datetime.now(timezone.utc).isoformat()
        
        for _, row in df.iterrows():
            if is_valid_architecture_job(row['title']):
                
                comp_name = str(row['company'])
                is_scraped_govt = any(k in comp_name.upper() for k in GOVT_KEYWORDS)
                
                # Use standard source names
                if is_scraped_govt:
                    source = "Govt Jobs"
                else:
                    source = str(row['site']).capitalize() # Indeed, Linkedin, etc.

                job_data = {
                    "jobId": str(row['id']),
                    "title": str(row['title']),
                    "companyName": comp_name or "Company",
                    "location": str(row['location']) or "India",
                    "salary": clean_salary(row.get('min_amount')),
                    "postedDate": str(row.get('date_posted') or now),
                    "applyUrl": str(row['job_url']),
                    "source": source,
                    "employmentType": str(row.get('job_type') or "Full-time"),
                    "discription": clean_text(row.get('description')),
                    "industry": "Architecture",
                    "created_at": now 
                }
                try: 
                    supabase.table("jobs").upsert(job_data, on_conflict="jobId").execute()
                except: pass
    
    # 2. Run Manual Sheet Import
    print("--- Starting Manual Sheet Import ---")
    import_google_sheet()
    print("--- Finished ---")

if __name__ == "__main__":
    run_scraper()
