### Step 1: Install Prerequisites
Ensure you have Python installed, and install the necessary libraries for data processing and database interaction.

### Step 2: Create the Database Schema
The `eda_loader.py` script is designed to handle both schema creation and data loading.

Run the Python script to create the database structure and load the data from the data set:

```Bash
python eda_loader.py
```

This will create a new SQLite database file named `job_postings.db` and populate the tables: `dim_location`, `dim_company`, and `job_posting`.

### Step 3: Verify Data Loading
You can connect to the SQLite database to verify the tables were created and populated correctly.

```Bash
sqlite3 job_postings.db
```

Once inside the SQLite prompt, run the following commands:

```Bash
.tables
-- Expected Output: dim_company  dim_location  job_posting

SELECT COUNT(*) FROM job_posting;
-- Expected Output: (A number greater than 0, e.g., 742)
.quit
```
## For Indexes

### 1. Baseline Performance (No Index on `job_simplified`)
We will run a query and examine the query plan (EXPLAIN QUERY PLAN) and the execution time (.timer on).

```Bash
sqlite3 job_postings.db
.timer on
.mode column
.headers on

-- A. Show the query plan (Full Table Scan)
EXPLAIN QUERY PLAN 
SELECT * FROM job_posting WHERE job_simplified = 'data scientist';
-- B. Show the execution time
SELECT * FROM job_posting WHERE job_simplified = 'data scientist';
```

### 2. Create the Index
We will now create a standard index on the target column.

```Bash
CREATE INDEX idx_job_simp ON job_posting (job_simplified);
```

### 3. Post-Index Performance
We run the exact same query again to observe the change in the query plan and the execution time.

```Bash
-- A. Show the query plan (Index Search)
EXPLAIN QUERY PLAN 
SELECT * FROM job_posting WHERE job_simplified = 'data scientist';
-- B. Show the execution time
SELECT * FROM job_posting WHERE job_simplified = 'data scientist';
.quit
```
