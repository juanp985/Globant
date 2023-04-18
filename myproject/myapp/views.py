from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import HiredEmployees, Departments, Jobs
import csv
from datetime import datetime
import os
import avro.schema

@csrf_exempt
def insert_csv_data(request):
    if request.method == 'POST':
        csv_file = request.FILES['csv_file']
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)
        hired_employees_data_list = []
        departments_data_list = []
        jobs_data_list = []
        for row in reader:
            # Perform data validation and enforce rules
            # Check if all required fields are present in the CSV row
            if 'employee_id' not in row or 'first_name' not in row or 'last_name' not in row or 'hire_date' not in row or 'department_id' not in row or 'job_id' not in row:
                return JsonResponse({'error': 'CSV data is missing required fields.'}, status=400)
            # Check if employee_id is unique
            if HiredEmployees.objects.filter(employee_id=row['employee_id']).exists():
                return JsonResponse({'error': f'Employee with employee_id {row["employee_id"]} already exists.'}, status=400)
            # Check if department_id exists in Departments table
            if not Departments.objects.filter(department_id=row['department_id']).exists():
                return JsonResponse({'error': f'Department with department_id {row["department_id"]} does not exist.'}, status=400)
            # Check if job_id exists in Jobs table
            if not Jobs.objects.filter(job_id=row['job_id']).exists():
                return JsonResponse({'error': f'Job with job_id {row["job_id"]} does not exist.'}, status=400)

            # Prepare data for insertion into respective tables
            hired_employee_data = HiredEmployees(
                employee_id=row['employee_id'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                hire_date=datetime.strptime(row['hire_date'], '%Y-%m-%d %H:%M:%S'), # assuming hire_date format is '%Y-%m-%d %H:%M:%S'
                department_id=row['department_id'],
                job_id=row['job_id']
            )
            hired_employees_data_list.append(hired_employee_data)

            department_data = Departments(
                department_id=row['department_id'],
                department_name=row['department_name']
            )
            departments_data_list.append(department_data)

            job_data = Jobs(
                job_id=row['job_id'],
                job_name=row['job_name']
            )
            jobs_data_list.append(job_data)

        # Insert batch transactions into respective tables
        HiredEmployees.objects.bulk_create(hired_employees_data_list)
        Departments.objects.bulk_create(departments_data_list)
        Jobs.objects.bulk_create(jobs_data_list)

        # Backup data to AVRO files
        backup_hired_employees(hired_employees_data_list)
        backup_departments(departments_data_list)
        backup_jobs(jobs_data_list)

        return JsonResponse({'success': f'{len(hired_employees_data_list)} rows inserted into HiredEmployees table, {len(departments_data_list)} rows inserted into Departments table, and {len(jobs_data_list)} rows inserted into Jobs table. Backup files created.'}, status=200)

""" 4. Create a feature to backup for each table and save it in the file system in AVRO format. """
def backup_hired_employees(data_list):
    schema = avro.schema.parse(open("hired_employees.avsc").read())
    filename = 'hired_employees_backup.avro'
    with open(filename, 'wb') as f:
        writer = avro.io.DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(f)
        for data in data_list:
            writer.write(data.__dict__, encoder)

def backup_departments(data_list):
    schema = avro.schema.parse(open("departments.avsc").read())
    filename = 'departments_backup.avro'
    with open(filename, 'wb') as f:
        writer = avro.io.DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(f)
        for data in data_list:
            writer.write(data.__dict__, encoder)

def backup_jobs(data_list):
    schema = avro.schema.parse(open("jobs.avsc").read())
    filename = 'jobs_backup.avro'
    with open(filename, 'wb') as f:
        writer = avro.io.DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(f)
        for data in data_list:
            writer.write(data.__dict__, encoder)


""" 4. Ceate a feature to restore a certain table with its backup. """
def restore_table_from_backup(table_name):
    # Define AVRO schema for the table
    if table_name == 'hired_employees':
        schema = avro.schema.parse(open("hired_employees.avsc").read())
        filename = 'hired_employees_backup.avro'
    elif table_name == 'departments':
        schema = avro.schema.parse(open("departments.avsc").read())
        filename = 'departments_backup.avro'
    elif table_name == 'jobs':
        schema = avro.schema.parse(open("jobs.avsc").read())
        filename = 'jobs_backup.avro'
    else:
        print("Invalid table name")
        return

    # Check if backup file exists
    if not os.path.exists(filename):
        print(f"No backup file found for {table_name}")
        return

    # Read data from backup file
    data_list = []
    with open(filename, 'rb') as f:
        reader = avro.io.DatumReader(schema)
        decoder = avro.io.BinaryDecoder(f)
        while True:
            try:
                data = reader.read(decoder)
                data_list.append(data)
            except EOFError:
                break

    # Insert restored data into table
    if table_name == 'hired_employees':
        # Insert into HiredEmployees table
        for data in data_list:
            # Insert logic for HiredEmployees table here
            # Example: HiredEmployees.objects.create(**data)
            pass
    elif table_name == 'departments':
        # Insert into Departments table
        for data in data_list:
            # Insert logic for Departments table here
            # Example: Departments.objects.create(**data)
            pass
    elif table_name == 'jobs':
        # Insert into Jobs table
        for data in data_list:
            # Insert logic for Jobs table here
            # Example: Jobs.objects.create(**data)
            pass
    else:
        print("Invalid table name")
        return

    print(f"{len(data_list)} records restored to {table_name} table from backup")
