import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, ForeignKey, Index, func, desc
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, joinedload


# 1. CONFIGURATION CODE DRIVEN BY ENVIRONMENT VARIABLES

# The database URL is fetched from the 'JOBS_DB_URL' environment variable.
# If not set, it defaults to the local SQLite file 'job_postings.db'.
# This allows the DAL to be easily switched between dev/test/prod environments.
DATABASE_URL = os.environ.get("JOBS_DB_URL", "sqlite:///job_postings.db")

Base = declarative_base()


# SCHEMA DEFINITION (Must match the database schema)

class Location(Base):
    __tablename__ = 'dim_location'
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    state = Column(String(50))
    __table_args__ = (Index('idx_loc_city_state', 'city', 'state', unique=True),)

class Company(Base):
    __tablename__ = 'dim_company'
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(255), nullable=False)
    headquarters = Column(String(255))
    rating = Column(Float)
    size = Column(String(100))
    founded = Column(Integer)
    type_of_ownership = Column(String(100))
    industry = Column(String(255))
    sector = Column(String(255))
    revenue = Column(String(100))
    competitors = Column(Text)

class JobPosting(Base):
    __tablename__ = 'job_posting'
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('dim_company.company_id'))
    location_id = Column(Integer, ForeignKey('dim_location.location_id'))
    
    job_title = Column(String(255))
    job_simplified = Column(String(50))
    seniority = Column(String(50))
    job_description = Column(Text)
    min_salary = Column(Integer)
    max_salary = Column(Integer)
    avg_salary = Column(Integer)
    is_hourly = Column(Boolean)
    employer_provided = Column(Boolean)
    python_yn = Column(Boolean)
    r_yn = Column(Boolean)
    spark_yn = Column(Boolean)
    aws_yn = Column(Boolean)
    excel_yn = Column(Boolean)
    desc_len = Column(Integer)
    num_comp = Column(Integer)

    company = relationship("Company", backref="jobs")
    location = relationship("Location", backref="jobs")


# DATA ACCESS LAYER (DAL) CLASS

class JobPostingsDAL:
    def __init__(self, db_url=None):
        """
        Initialize the DAL with a database engine and session factory.
        """
        url = db_url or DATABASE_URL
        self.engine = create_engine(url, echo=False)
        self.Session = sessionmaker(bind=self.engine)

    # FEATURED QUERY FUNCTIONS

    def get_industry_salary_analysis(self, min_jobs_threshold=10):
        """
        Analyzes salary trends across different industries.
        
        This query performs a grouped aggregation joining JobPostings and Companies.
        It calculates the average minimum, maximum, and overall average salary for 
        each industry, filtering out industries with fewer job postings than the threshold.
        
        Parameters:
            min_jobs_threshold (int): The minimum number of job postings an industry 
                                      must have to be included in the results. Default is 10.
        
        Returns:
            list[dict]: A list of dictionaries, where each dictionary contains:
                        - 'industry': Name of the industry
                        - 'avg_salary': Average yearly salary
                        - 'avg_min_salary': Average minimum salary range
                        - 'avg_max_salary': Average maximum salary range
                        - 'job_count': Total number of jobs in this industry
        
        Example Usage:
            dal = JobPostingsDAL()
            stats = dal.get_industry_salary_analysis(min_jobs_threshold=20)
            # Result: [{'industry': 'Biotech & Pharmaceuticals', 'avg_salary': 112.5, ...}, ...]
        """
        session = self.Session()
        try:
            results = (
                session.query(
                    Company.industry,
                    func.avg(JobPosting.avg_salary).label('avg_sal'),
                    func.avg(JobPosting.min_salary).label('avg_min'),
                    func.avg(JobPosting.max_salary).label('avg_max'),
                    func.count(JobPosting.job_id).label('count')
                )
                .join(JobPosting, Company.company_id == JobPosting.company_id)
                .filter(Company.industry != None, Company.industry != '-1')
                .group_by(Company.industry)
                .having(func.count(JobPosting.job_id) >= min_jobs_threshold)
                .order_by(desc('avg_sal'))
                .all()
            )
            
            return [
                {
                    "industry": r.industry,
                    "avg_salary": round(r.avg_sal, 2),
                    "avg_min_salary": round(r.avg_min, 2),
                    "avg_max_salary": round(r.avg_max, 2),
                    "job_count": r.count
                }
                for r in results
            ]
        finally:
            session.close()

    def get_location_skill_demand(self, city, state):
        """
        Retrieves a market summary for a specific location, focusing on skill demand.
        
        This query joins JobPostings and Locations to find jobs in a specific city/state.
        It calculates the percentage of jobs requiring key technical skills (Python, AWS, Spark)
        and provides a snapshot of the top hiring companies in that location.
        
        Parameters:
            city (str): The city name (e.g., 'San Francisco').
            state (str): The state abbreviation (e.g., 'CA').
            
        Returns:
            dict: A dictionary containing:
                  - 'location': "City, State"
                  - 'total_jobs': Total job count
                  - 'avg_salary': Average salary in this location
                  - 'skill_demand': Dict with % of jobs requiring Python, AWS, Spark
                  - 'top_companies': List of names of the top 5 companies hiring here
        
        Example Usage:
            dal = JobPostingsDAL()
            sf_stats = dal.get_location_skill_demand("San Francisco", "CA")
            # Result: {
            #   'location': 'San Francisco, CA', 
            #   'total_jobs': 45, 
            #   'skill_demand': {'python': 0.75, 'aws': 0.40, ...},
            #   'top_companies': ['Company A', 'Company B', ...]
            # }
        """
        session = self.Session()
        try:
            # Subquery for basic stats
            stats = (
                session.query(
                    func.count(JobPosting.job_id).label('total'),
                    func.avg(JobPosting.avg_salary).label('salary'),
                    func.avg(JobPosting.python_yn).label('python'),
                    func.avg(JobPosting.aws_yn).label('aws'),
                    func.avg(JobPosting.spark_yn).label('spark')
                )
                .join(Location, JobPosting.location_id == Location.location_id)
                .filter(Location.city == city, Location.state == state)
                .one()
            )
            
            if stats.total == 0:
                return None

            # Subquery for top companies
            top_companies = (
                session.query(Company.company_name)
                .join(JobPosting, Company.company_id == JobPosting.company_id)
                .join(Location, JobPosting.location_id == Location.location_id)
                .filter(Location.city == city, Location.state == state)
                .group_by(Company.company_name)
                .order_by(desc(func.count(JobPosting.job_id)))
                .limit(5)
                .all()
            )

            return {
                "location": f"{city}, {state}",
                "total_jobs": stats.total,
                "avg_salary": round(stats.salary, 2) if stats.salary else 0,
                "skill_demand": {
                    "python_pct": round(stats.python * 100, 1),
                    "aws_pct": round(stats.aws * 100, 1),
                    "spark_pct": round(stats.spark * 100, 1)
                },
                "top_companies": [c.company_name for c in top_companies]
            }
        finally:
            session.close()


    # 3. FULL-CYCLE CRUD FOR ONE ENTITY (JobPosting)

    def create_job_posting(self, job_title, company_name, city, state, min_salary, max_salary, **kwargs):
        """
        Creates a new Job Posting in the database.
        
        **TRANSACTION REQUIREMENT**: This function performs a complex transactional operation.
        It handles the creation of related entities (Company and Location) if they do not exist
        before creating the JobPosting itself. All three operations occur within a single atomic
        transaction.
        
        Parameters:
            job_title (str): Title of the job.
            company_name (str): Name of the company.
            city (str): City of the job.
            state (str): State of the job.
            min_salary (int): Minimum salary (in thousands).
            max_salary (int): Maximum salary (in thousands).
            **kwargs: Additional optional fields (e.g., description, python_yn, rating).
            
        Returns:
            int: The ID of the newly created job posting.
            
        Example Usage:
            dal = JobPostingsDAL()
            new_id = dal.create_job_posting(
                job_title="Junior Data Analyst", 
                company_name="TechStart LLC", 
                city="Austin", 
                state="TX", 
                min_salary=60, 
                max_salary=85,
                python_yn=True
            )
        """
        session = self.Session()
        try:
            # 1. Get or Create Company (Atomic-like within transaction)
            company = session.query(Company).filter_by(company_name=company_name).first()
            if not company:
                company = Company(company_name=company_name, rating=kwargs.get('rating', -1))
                session.add(company)
                session.flush() # Get ID without committing

            # 2. Get or Create Location
            location = session.query(Location).filter_by(city=city, state=state).first()
            if not location:
                location = Location(city=city, state=state)
                session.add(location)
                session.flush()

            # 3. Create Job Posting
            avg_salary = (min_salary + max_salary) / 2
            new_job = JobPosting(
                company_id=company.company_id,
                location_id=location.location_id,
                job_title=job_title,
                min_salary=min_salary,
                max_salary=max_salary,
                avg_salary=avg_salary,
                job_description=kwargs.get('job_description', 'No description'),
                python_yn=kwargs.get('python_yn', False),
                aws_yn=kwargs.get('aws_yn', False),
                excel_yn=kwargs.get('excel_yn', False)
            )
            session.add(new_job)
            session.commit()
            return new_job.job_id
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def search_job_postings_readable(self, search_term):
        """
        User-readable search function.
        
        Finds job postings matching a keyword in the title. Returns a human-readable list
        that includes the ID, allowing the user to select an ID for Update/Delete operations.
        
        Parameters:
            search_term (str): Keyword to search for in job titles (case-insensitive).
            
        Returns:
            list[dict]: A list of simplified job objects {id, title, company, location}.
            
        Example Usage:
            results = dal.search_job_postings_readable("analyst")
            # Result: [{'id': 101, 'title': 'Data Analyst', 'company': 'IBM', 'location': 'NY, NY'}, ...]
        """
        session = self.Session()
        try:
            jobs = (
                session.query(JobPosting)
                .join(Company).join(Location)
                .filter(JobPosting.job_title.ilike(f"%{search_term}%"))
                .limit(20)
                .all()
            )
            return [
                {
                    "id": j.job_id,
                    "title": j.job_title,
                    "company": j.company.company_name,
                    "location": f"{j.location.city}, {j.location.state}"
                }
                for j in jobs
            ]
        finally:
            session.close()

    def get_job_posting_by_id(self, job_id):
        """
        Retrieve a single full entity by ID.
        
        Parameters:
            job_id (int): The unique ID of the job posting.
            
        Returns:
            dict: Full details of the job posting or None if not found.
        """
        session = self.Session()
        try:
            job = session.query(JobPosting).get(job_id)
            if not job: 
                return None
            
            # Convert SQLAlchemy object to dict
            return {
                "job_id": job.job_id,
                "title": job.job_title,
                "company": job.company.company_name,
                "location": f"{job.location.city}, {job.location.state}",
                "salary_range": f"${job.min_salary}k-${job.max_salary}k",
                "description": job.job_description[:100] + "..." # Snippet
            }
        finally:
            session.close()

    def update_job_posting(self, job_id, **kwargs):
        """
        Updates an existing job posting.
        
        Parameters:
            job_id (int): ID of the job to update.
            **kwargs: Key-value pairs of fields to update (e.g., min_salary=90).
            
        Returns:
            bool: True if successful, False if job not found.
            
        Example Usage:
            dal.update_job_posting(101, min_salary=70, max_salary=95)
        """
        session = self.Session()
        try:
            job = session.query(JobPosting).get(job_id)
            if not job:
                return False
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            # Recalculate average if salary bounds changed
            if 'min_salary' in kwargs or 'max_salary' in kwargs:
                job.avg_salary = (job.min_salary + job.max_salary) / 2
                
            session.commit()
            return True
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def delete_job_posting(self, job_id):
        """
        Deletes a job posting by ID.
        
        Parameters:
            job_id (int): ID of the job to delete.
            
        Returns:
            bool: True if deleted, False if not found.
            
        Example Usage:
            dal.delete_job_posting(101)
        """
        session = self.Session()
        try:
            job = session.query(JobPosting).get(job_id)
            if not job:
                return False
            session.delete(job)
            session.commit()
            return True
        except:
            session.rollback()
            raise
        finally:
            session.close()


    # 4. HELPER FUNCTIONS FOR USABILITY

    def get_unique_locations(self):
        """
        Helper to retrieve all available locations for filtering.
        
        This allows a UI to populate a dropdown menu so users know valid inputs
        for queries like `get_location_skill_demand`.
        
        Parameters:
            None
        
        Returns:
            list[str]: Sorted list of strings in "City, State" format.
            
        Example Usage:
            locs = dal.get_unique_locations()
            # Result: ['Akron, OH', 'Albany, NY', ...]
        """
        session = self.Session()
        try:
            locs = session.query(Location.city, Location.state).distinct().all()
            formatted = [f"{city}, {state}" for city, state in locs]
            return sorted(formatted)
        finally:
            session.close()