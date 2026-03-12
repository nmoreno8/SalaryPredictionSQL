-- Schema for Job Postings Database

-- Dimension Table: Location
-- Captures the geographical information for job postings.
CREATE TABLE Dim_Location (
    Location_ID SERIAL PRIMARY KEY,
    City VARCHAR(100),
    State VARCHAR(50)
);

-- Dimension Table: Company
-- Stores attributes related to the employers.
-- 'Founded' is nullable to handle missing values (represented as -1 in raw data).
CREATE TABLE Dim_Company (
    Company_ID SERIAL PRIMARY KEY,
    Company_Name VARCHAR(255) NOT NULL,
    Headquarters VARCHAR(255),
    Rating DECIMAL(3, 1),
    Size VARCHAR(100),
    Founded INT,
    Type_of_ownership VARCHAR(100),
    Industry VARCHAR(100),
    Sector VARCHAR(100),
    Revenue VARCHAR(100),
    Competitors TEXT
);

-- Fact Table: Job_Posting
-- Central table containing job metrics and keys to dimensions.
CREATE TABLE Fact_Job_Posting (
    Job_ID SERIAL PRIMARY KEY,
    Company_ID INT REFERENCES Dim_Company(Company_ID),
    Location_ID INT REFERENCES Dim_Location(Location_ID),
    Job_Title VARCHAR(255),
    Job_Simplified VARCHAR(100),
    Seniority VARCHAR(50),
    Job_Description TEXT,
    Min_Salary INT,
    Max_Salary INT,
    Avg_Salary INT,
    Is_Hourly BOOLEAN,
    Employer_Provided BOOLEAN,
    Python_YN BOOLEAN,
    R_YN BOOLEAN,
    Spark_YN BOOLEAN,
    AWS_YN BOOLEAN,
    Excel_YN BOOLEAN,
    Desc_Len INT,
    Num_Comp INT
);

-- Indexes for performance
CREATE INDEX idx_job_company ON Fact_Job_Posting(Company_ID);
CREATE INDEX idx_job_location ON Fact_Job_Posting(Location_ID);