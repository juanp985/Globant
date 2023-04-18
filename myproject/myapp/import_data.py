import pandas as pd
import mysql.connector

# Connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="juan",
    password="password",
    database="personal_db"
)
cursor = mydb.cursor()

# Load hired_employees.csv into a DataFrame
hired_employees_df = pd.read_csv("hired_employees.csv")

# Loop through each row in hired_employees_df and insert into the MySQL table
for index, row in hired_employees_df.iterrows():
    cursor.execute(f"INSERT INTO hired_employees (employee_id, first_name, last_name, hire_date) VALUES ({row['employee_id']}, '{row['first_name']}', '{row['last_name']}', '{row['hire_date']}')")
    mydb.commit()

# Load jobs.csv into a DataFrame
jobs_df = pd.read_csv("jobs.csv")

# Loop through each row in jobs_df and insert into the MySQL table
for index, row in jobs_df.iterrows():
    cursor.execute(f"INSERT INTO jobs (job_id, job_title, min_salary, max_salary) VALUES ('{row['job_id']}', '{row['job_title']}', {row['min_salary']}, {row['max_salary']})")
    mydb.commit()

# Load departments.csv into a DataFrame
departments_df = pd.read_csv("departments.csv")

# Loop through each row in departments_df and insert into the MySQL table
for index, row in departments_df.iterrows():
    cursor.execute(f"INSERT INTO departments (department_id, department_name, manager_id, location_id) VALUES ({row['department_id']}, '{row['department_name']}', {row['manager_id']}, {row['location_id']})")
    mydb.commit()

# Close the MySQL connection
cursor.close()
mydb.close()

print("Data has been successfully inserted into MySQL tables!")
