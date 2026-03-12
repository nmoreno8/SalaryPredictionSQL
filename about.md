### Link to Dataset: https://www.kaggle.com/datasets/thedevastator/jobs-dataset-from-glassdoor?select=eda_data.csv

### Content Description: 

This dataset contains about 742 records of job listings with 33 columns, offering a mix of raw text data and pre-processed features suitable for analysis.

* Job Details: Includes `Job Title`, `Job Description` (full text), and a simplified role category (`job_simp`) like "data scientist" or "analyst."

* Salary Information: Contains parsed salary data, including `min_salary`, `max_salary`, `avg_salary`, and `hourly` flags.

* Company Profile: Detailed attributes such as `Company Name`, `Rating`, `Size`, `Founded`, `Type of ownership`, `Industry`, `Sector`, and `Revenue`.

* Location: `Location` (city/state), `job_state`, and whether the job is at the `Headquarters`.

* Skill Extraction: Binary columns indicating the presence of key technical skills in the job description: `python_yn`, `R_yn`, `spark`, `aws`, and `excel`. 

### Application Description: 

This dataset could power a Career Intelligence Dashboard designed for data science professionals and recruitment agencies. The application would serve as a strategic tool to navigate the job market, moving beyond simple job searching to provide actionable insights into compensation, skill requirements, and company landscapes.

<u>Example:</u>

Salary Prediction Engine:

* User Action: A user inputs their location, years of experience, and known tech stack (e.g., Python, AWS).

* System Output: Using the avg_salary, Location, and seniority columns, the app predicts a competitive salary range, helping candidates negotiate better offers.

### *The SQL logical data model was the best for this dataset because it has a fixed number of columns with consistent data types (integers for salary, strings for names, booleans for skills)*

