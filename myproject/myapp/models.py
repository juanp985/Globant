from django.db import models

class HiredEmployees(models.Model):
    employee_id = models.IntegerField(unique=True)
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    hire_date = models.DateTimeField()
    department_id = models.IntegerField()
    job_id = models.IntegerField()

class Departments(models.Model):
    department_id = models.IntegerField(unique=True)
    department_name = models.CharField(max_length=100)

class Jobs(models.Model):
    job_id = models.CharField(max_length=100, unique=True)
    job_name = models.CharField(max_length=100)
