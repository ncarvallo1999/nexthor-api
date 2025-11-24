import os
import sys
import datetime
import re
import time
import requests
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError

# DB setup (your path)
DB_URL = os.getenv('DB_URL', 'sqlite:///C:/Users/super/Desktop/Nexthor Ai/reg_d_treasure.db')
engine = create_engine(DB_URL, echo=False)
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

# Create/update table
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

HEADERS = {'User-Agent': os.getenv('EDGAR_USER_AGENT', 'NestorCarvallo nestorcarvallo.jr@gmail.com')}

NS = {'d': 'http://www.sec.gov/edgar/formd'}

def get_daily_idx_url(year, quarter, date_str):
    """Daily IDX with stamped filename."""
    return f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/{date_str}/master.{date_str}.idx"

def parse_index_lines(lines):
    """Parse .idx lines for Form D."""
    entries = []
    for line in lines[11:]:  # Skip header
        parts = [p.strip() for p in line.split('|') if p.strip()]
        if len(parts) == 5 and parts[2] == 'D':
            cik = parts[0].zfill(10)
            company_name = parts[1]
            filing_date_str = parts[3]
            filename = parts[4]
            # Parse accession from filename path
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
    """Download & parse XML."""
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_doc}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        parsed = parse_form_d_xml(resp.content, url)
        parsed['cik'] = cik
        return parsed
    print(f"Download fail {resp.status_code}: {url}")
    return None

def parse_form_d_xml(xml_content, raw_url):
    """XML extract."""
    try:
        root = ET.fromstring(xml_content)
        accept_elem = root.find('.//d:acceptanceDateTime', NS)
        date_str = accept_elem.text[:10] if accept_elem is not None else None
        filing_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None
        
        company_elem = root.find('.//d:companyName', NS)
        company_name = company_elem.text.strip() if company_elem is not None else ''
        
        min_inv_elem = root.find('.//d:minimumInvestment', NS)
        raise_amount = min_inv_elem.text.strip() if min_inv_elem is not None else ''
        
        return {'filing_date': filing_date, 'company_name': company_name, 'raise_amount': raise_amount, 'raw_xml_url': raw_url}
    except ET.ParseError:
        print(f"Parse fail for {raw_url}")
        return {}

def insert_if_new(session, data):
    """Dupe-proof insert."""
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
        print(f"Inserted: {data['company_name']} ({data['cik']}) - ${data['raise_amount']} on {data['filing_date']}")
        return True
    return False

def process_daily(session, year, quarter, date_str):
    """One day's Form D."""
    url = get_daily_idx_url(year, quarter, date_str)
    print(f"Fetching daily index: {url}")
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        lines = resp.text.splitlines()
        entries = parse_index_lines(lines)
        print(f"Found {len(entries)} Form D entries for {date_str}")
        count = 0
        for entry in entries:
            data = download_and_parse_xml(entry['cik'], entry['accession'], entry['filename'])
            if data and insert_if_new(session, data):
                count += 1
            time.sleep(0.2)  # Nicer SEC throttle
        print(f"Added {count} new to DB for {date_str}")
    else:
        print(f"Fetch fail status {resp.status_code} for {date_str}—SEC nap or future date? Try past for test.")

def historical_90days():
    """MVP historical: Last 90 days (fast test, ~300 Form D)."""
    session = SessionLocal()
    try:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=90)
        current = start_date
        while current < today:
            year = current.year
            month = current.month
            quarter = ((month - 1) // 3) + 1
            date_str = current.strftime('%Y%m%d')
            process_daily(session, year, quarter, date_str)
            current += datetime.timedelta(days=1)
        session.commit()
        print("90-day historical complete—vault primed for MVP!")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

def daily_update():
    """Daily: Yesterday (or test_date)."""
    session = SessionLocal()
    try:
        test_date_str = os.getenv('TEST_DATE', None)
        if test_date_str:
            yesterday = datetime.datetime.strptime(test_date_str, '%Y-%m-%d').date()
            print(f"Test mode: Using {test_date_str} for pull.")
        else:
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
        year = yesterday.year
        month = yesterday.month
        quarter = ((month - 1) // 3) + 1
        date_str = yesterday.strftime('%Y%m%d')
        process_daily(session, year, quarter, date_str)
        session.commit()
        print(f"Daily update for {yesterday} complete!")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python form_d_ingest.py [historical|daily]")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == "historical":
        historical_90days()
    elif mode == "daily":
        daily_update()
    else:
        print("Use 'historical' or 'daily'.")