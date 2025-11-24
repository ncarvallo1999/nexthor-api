import os
import sys
import datetime
import re
import time
import requests
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker

# DB setup
DB_URL = os.getenv('DB_URL')
if not DB_URL:
    print("⚠️ No DB_URL found. Using local SQLite.")
    DB_URL = 'sqlite:///reg_d_treasure.db'
elif "postgres" in DB_URL and "sslmode" not in DB_URL:
     pass

engine = create_engine(DB_URL, pool_pre_ping=True)
Base = declarative_base()

class Filing(Base):
    __tablename__ = 'filings'
    id = Column(Integer, primary_key=True)
    cik = Column(String(10), nullable=False)
    company_name = Column(String(500))
    raise_amount = Column(String(50))
    filing_date = Column(Date, nullable=False)
    processed = Column(Boolean, default=False)
    raw_xml_url = Column(String(500))
    ai_score = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint('cik', 'filing_date', name='unique_cik_date'),)

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

# --- HARDCODED USER AGENT (The Fix) ---
# We force this exactly as SEC wants it: "AppName <Email>"
HEADERS = {
    'User-Agent': 'NexthorAi <nestorcarvallo.jr@gmail.com>',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}
NS = {'d': 'http://www.sec.gov/edgar/formd'}

def get_daily_idx_url(year, quarter, date_str):
    return f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str}.idx"

def parse_index_lines(lines):
    entries = []
    for line in lines[11:]: 
        parts = [p.strip() for p in line.split('|') if p.strip()]
        if len(parts) == 5 and parts[2] == 'D':
            cik = parts[0].zfill(10)
            company_name = parts[1]
            filing_date_str = parts[3]
            filename = parts[4]
            path_match = re.match(r'Archives/edgar/data/(\d+)/(\S+)/(\S+)', filename)
            if path_match:
                accession = path_match.group(2)
                primary_doc = path_match.group(3)
                raw_xml_url = f"https://www.sec.gov/{filename}"
                entries.append({
                    'cik': cik,
                    'company_name': company_name,
                    'filing_date': filing_date_str,
                    'accession': accession,
                    'filename': primary_doc,
                    'raw_xml_url': raw_xml_url
                })
    return entries

def download_and_parse_xml(cik, accession, primary_doc):
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_doc}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            parsed = parse_form_d_xml(resp.content, url)
            parsed['cik'] = cik
            return parsed
        return None
    except Exception as e:
        print(f"⚠️ Network error: {e}")
        return None

def parse_form_d_xml(xml_content, raw_url):
    try:
        root = ET.fromstring(xml_content)
        accept_elem = root.find('.//d:acceptanceDateTime', NS)
        date_str = accept_elem.text[:10] if accept_elem is not None else None
        filing_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None
        
        company_elem = root.find('.//d:companyName', NS)
        company_name = company_elem.text.strip() if company_elem is not None else ''
        
        min_inv_elem = root.find('.//d:totalOfferingAmount', NS)
        if min_inv_elem is None:
             min_inv_elem = root.find('.//d:minimumInvestment', NS)
        
        raise_amount = min_inv_elem.text.strip() if min_inv_elem is not None else 'Unknown'
        
        return {'filing_date': filing_date, 'company_name': company_name, 'raise_amount': raise_amount, 'raw_xml_url': raw_url}
    except ET.ParseError:
        return {}

def insert_if_new(session, data):
    if not data.get('filing_date') or not data.get('cik'):
        return False
    existing = session.query(Filing).filter_by(cik=data['cik'], filing_date=data['filing_date']).first()
    if not existing:
        new_filing = Filing(
            cik=data['cik'],
            company_name=data['company_name'],
            raise_amount=data['raise_amount'],
            filing_date=data['filing_date'],
            processed=False,
            raw_xml_url=data.get('raw_xml_url', ''),
            ai_score=0 
        )
        session.add(new_filing)
        print(f"✅ Inserted: {data['company_name']} - {data['raise_amount']}")
        return True
    return False

def process_daily(session, year, quarter, date_str):
    url = get_daily_idx_url(year, quarter, date_str)
    print(f"📥 Fetching SEC Index: {url}")
    resp = requests.get(url, headers=HEADERS)
    
    # --- DEBUG SECTION: TELL ME WHAT THE SEC SAID ---
    if resp.status_code == 200:
        lines = resp.text.splitlines()
        
        # Check if we are blocked (HTML instead of Data)
        if len(lines) > 0 and "<html" in lines[0].lower():
            print("\n❌ BLOCKED BY SEC. RESPONSE CONTENT:")
            for l in lines[:10]: # Print first 10 lines of error
                print(l)
            print("----------------------------------\n")
            return

        entries = parse_index_lines(lines)
        print(f"🔎 Found {len(entries)} Form D entries. Processing...")
        count = 0
        for entry in entries:
            data = download_and_parse_xml(entry['cik'], entry['accession'], entry['filename'])
            if data and insert_if_new(session, data):
                count += 1
            time.sleep(0.15) 
        print(f"🚀 Batch Complete: Added {count} new leads.")
    else:
        print(f"❌ Index not found (Status {resp.status_code}).")

def daily_update():
    session = SessionLocal()
    try:
        test_date_str = os.getenv('TEST_DATE') 
        if test_date_str:
            print(f"🧪 TEST MODE: Using {test_date_str}")
            target_date = datetime.datetime.strptime(test_date_str, '%Y-%m-%d').date()
        else:
            target_date = datetime.date.today() - datetime.timedelta(days=1)
        
        year = target_date.year
        month = target_date.month
        quarter = ((month - 1) // 3) + 1
        date_str = target_date.strftime('%Y%m%d')
        
        process_daily(session, year, quarter, date_str)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"🔥 Critical Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    daily_update()
