import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, ForeignKey, Index
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.engine import URL

# 1. DATABASE CONNECTION & SETUP
# Using SQLite. 

DATABASE_URL = "sqlite:///job_postings.db"
engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()


# 2. SCHEMA DEFINITION 

class Location(Base):
    __tablename__ = 'dim_location'
    
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    state = Column(String(50))
    
    # Enforce uniqueness to prevent duplicate locations
    __table_args__ = (
        Index('idx_loc_city_state', 'city', 'state', unique=True),
    )

    def __repr__(self):
        return f"<Location(city='{self.city}', state='{self.state}')>"


class Company(Base):
    __tablename__ = 'dim_company'
    
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(255), nullable=False)
    headquarters = Column(String(255))
    rating = Column(Float)
    size = Column(String(100))
    founded = Column(Integer)
    type_of_ownership = Column(String(100))
    industry = Column(String(100))
    sector = Column(String(100))
    revenue = Column(String(100))
    competitors = Column(Text)

    # Index on Company Name for fast lookups
    __table_args__ = (
        Index('idx_company_name', 'company_name'),
    )

    def __repr__(self):
        return f"<Company(name='{self.company_name}')>"


class JobPosting(Base):
    __tablename__ = 'fact_job_posting'
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    company_id = Column(Integer, ForeignKey('dim_company.company_id'), nullable=False)
    location_id = Column(Integer, ForeignKey('dim_location.location_id'), nullable=False)
    
    # Job Details
    job_title = Column(String(255))
    job_simplified = Column(String(100))
    seniority = Column(String(50))
    job_description = Column(Text)
    
    # Salary Metrics
    min_salary = Column(Integer)
    max_salary = Column(Integer)
    avg_salary = Column(Integer)
    is_hourly = Column(Boolean)
    employer_provided = Column(Boolean)
    
    # Tech Stack (Boolean Flags)
    python_yn = Column(Boolean)
    r_yn = Column(Boolean)
    spark_yn = Column(Boolean)
    aws_yn = Column(Boolean)
    excel_yn = Column(Boolean)
    
    # Metadata
    desc_len = Column(Integer)
    num_comp = Column(Integer)


    # 3. INDEXING STRATEGY

    __table_args__ = (
        # 1. Performance: Indexes on Foreign Keys are crucial for Joins
        Index('idx_fact_company', 'company_id'),
        Index('idx_fact_location', 'location_id'),
        
        # 2. Analytics: Composite index for filtering by Role and Salary (Common Query)
        Index('idx_job_role_salary', 'job_simplified', 'avg_salary'),
        
        # 3. Analytics: Index for Skill filtering (Python is the most queried skill)
        Index('idx_skill_python', 'python_yn'),
    )


# 4. DATA CLEANING & LOADING LOGIC

def clean_val(val, target_type=str):
    """
    Cleans values:
    - Converts -1, '-1', 'na', 'nan' to None (NULL).
    - Handles boolean conversion for 1/0.
    """
    if pd.isna(val) or val == -1 or str(val) == '-1' or str(val).lower() == 'na':
        return None
    
    if target_type == bool:
        return bool(val)
        
    return val

def load_data(csv_path):
    print("Creating tables...")
    Base.metadata.create_all(engine)
    
    session = Session()
    
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Cache to avoid repeated DB lookups for Dimensions
    # Structure: {(City, State): Location_Object}
    location_cache = {}
    # Structure: {Company_Name: Company_Object} 
    # (Note: In a real scenario, we might key by more attributes, but Name is unique enough here)
    company_cache = {}
    
    job_postings = []
    
    print("Processing rows...")
    for _, row in df.iterrows():
        
        # --- Handle Location Dimension ---
        city = clean_val(row.get('Location', '').split(',')[0].strip()) if ',' in str(row.get('Location', '')) else row.get('Location')
        state = clean_val(row.get('job_state'))
        
        loc_key = (city, state)
        if loc_key not in location_cache:
            loc = Location(city=city, state=state)
            session.add(loc)
            session.flush() # Flush to get the ID immediately
            location_cache[loc_key] = loc
        else:
            loc = location_cache[loc_key]

        # --- Handle Company Dimension ---
        # Using 'company_txt' as it appears cleaner in the dataset
        comp_name = clean_val(row.get('company_txt'))
        if not comp_name:
            comp_name = "Unknown"
            
        if comp_name not in company_cache:
            comp = Company(
                company_name=comp_name,
                headquarters=clean_val(row.get('Headquarters')),
                rating=clean_val(row.get('Rating'), float),
                size=clean_val(row.get('Size')),
                founded=clean_val(row.get('Founded'), int),
                type_of_ownership=clean_val(row.get('Type of ownership')),
                industry=clean_val(row.get('Industry')),
                sector=clean_val(row.get('Sector')),
                revenue=clean_val(row.get('Revenue')),
                competitors=clean_val(row.get('Competitors'))
            )
            session.add(comp)
            session.flush()
            company_cache[comp_name] = comp
        else:
            comp = company_cache[comp_name]

        # --- Handle Fact Table ---
        job = JobPosting(
            company_id=comp.company_id,
            location_id=loc.location_id,
            job_title=clean_val(row.get('Job Title')),
            job_simplified=clean_val(row.get('job_simp')),
            seniority=clean_val(row.get('seniority')),
            job_description=clean_val(row.get('Job Description')),
            min_salary=clean_val(row.get('min_salary'), int),
            max_salary=clean_val(row.get('max_salary'), int),
            avg_salary=clean_val(row.get('avg_salary'), int),
            is_hourly=clean_val(row.get('hourly'), bool),
            employer_provided=clean_val(row.get('employer_provided'), bool),
            python_yn=clean_val(row.get('python_yn'), bool),
            r_yn=clean_val(row.get('R_yn'), bool),
            spark_yn=clean_val(row.get('spark'), bool),
            aws_yn=clean_val(row.get('aws'), bool),
            excel_yn=clean_val(row.get('excel'), bool),
            desc_len=clean_val(row.get('desc_len'), int),
            num_comp=clean_val(row.get('num_comp'), int)
        )
        job_postings.append(job)

    print(f"Bulk inserting {len(job_postings)} job postings...")
    session.add_all(job_postings)
    session.commit()
    print("Data load complete.")
    session.close()

if __name__ == "__main__":
    load_data('eda_data.csv')