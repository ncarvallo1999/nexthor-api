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

# HEADERS
HEADERS = {
    'User-Agent': 'NexthorData/1.0 (nestorcarvallo.jr@gmail.com)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}
NS = {'d': 'http://www.sec.gov/edgar/formd'}

def get_daily_idx_url(year, quarter, date_str):
    return f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str}.idx"

def parse_index_lines(lines):
    entries = []
    print(f"DEBUG: Processing {len(lines)} lines...")
    
    for i, line in enumerate(lines):
        if i < 5: continue # Skip header
        
        parts = line.split('|')
        if len(parts) < 5: continue
            
        cik = parts[0].strip()
        company_name = parts[1].strip()
        form_type = parts[2].strip()
        date_filed = parts[3].strip()
        filename = parts[4].strip()
        
        if form_type == 'D' or form_type == 'D/A':
            # Regex Fix: Match 2 slashes "edgar/data/CIK/FILENAME"
            path_match = re.match(r'edgar/data/(\d+)/(.+)', filename)
            if path_match:
                # Filename is like: 0001437749-23-034978.txt
                txt_name = path_match.group(2)
                # Accession is filename without .txt
                accession = txt_name.replace('.txt', '')
                # SEC XML folder usually removes dashes from accession
                accession_no_dashes = accession.replace('-', '')
                
                # Construct the likely XML URL
                raw_xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/primary_doc.xml"
                
                entries.append({
                    'cik': cik,
                    'company_name': company_name,
                    'filing_date': date_filed,
                    'raw_xml_url': raw_xml_url
                })
    return entries

def download_and_parse_xml(entry):
    url = entry['raw_xml_url']
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            parsed = parse_form_d_xml(resp.content, url)
            parsed['cik'] = entry['cik']
            # Fallback if date missing in XML, use index date
            if not parsed.get('filing_date') and entry['filing_date']:
                 parsed['filing_date'] = datetime.datetime.strptime(entry['filing_date'], '%Y%m%d').date()
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
    
    try:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            entries = parse_index_lines(lines)
            print(f"🔎 Found {len(entries)} Form D entries. Processing...")
            
            count = 0
            for entry in entries:
                data = download_and_parse_xml(entry)
                if data and insert_if_new(session, data):
                    count += 1
                time.sleep(0.15) 
            print(f"🚀 Batch Complete: Added {count} new leads.")
        else:
            print(f"❌ Failed to download index (Status {resp.status_code}).")
            
    except Exception as e:
        print(f"🔥 Download Error: {e}")

def daily_update():
    session = SessionLocal()
