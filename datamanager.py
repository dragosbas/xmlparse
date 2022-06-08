from flask import jsonify
import database_connection


@database_connection.connection_handler
def get_users(cursor, username):
    query = """
        select * from app_user
        """
    username = {"username": username}
    cursor.execute(query, username)
    return cursor.fetchone()


@database_connection.connection_handler
def get_users(cursor):
    query = """
        SELECT * FROM users
        """
    # username = {'username': username}
    cursor.execute(query)
    return cursor.fetchall()


@database_connection.connection_handler
def execute_query(cursor, query):
    cursor.execute(query)
    return jsonify(cursor.fetchall())


@database_connection.connection_handler
def insert_employee(cursor, user_data):
    query = """
        INSERT INTO employees (id,company_id,job_title,salary,start_date,end_date)
        VALUES
        (%(id)s,%(company_id)s,%(job_title)s,%(salary)s,%(start_date)s,%(end_date)s)
        """
    cursor.execute(query, user_data)
    # return cursor.fetchone()


@database_connection.connection_handler
def insert_users(cursor, user_data):
    query = """
       INSERT INTO users VALUES
        (%(username)s,%(pass)s)
        """
    cursor.execute(query, user_data)
    # return cursor.fetchone()


@database_connection.connection_handler
def get_own_employees(cursor, company_data):
    query = f"""
        SELECT id,
        job_title,
        salary,
        start_date,
        end_date FROM employees
        WHERE company_id=%(company_id)s
        ORDER BY {company_data["orderBy"]}
        {company_data["direction"]}
        LIMIT %(limit)s
        OFFSET %(offset)s 
        """
    cursor.execute(query, company_data)
    return cursor.fetchall()


@database_connection.connection_handler
def get_user_details(cursor, user_id):
    query = """
        SELECT * FROM users
        WHERE username=%(username)s 
        """
    cursor.execute(query, user_id)
    return cursor.fetchone()


@database_connection.connection_handler
def get_company_snapshot_date(cursor, details):
    query = """
    SELECT
    -- job_title,
    COUNT(id) as total_employees,
    ROUND(avg(salary)) as average_salary
    -- SUM(salary) as total_salaries
    FROM employees
    WHERE company_id = %(company_id)s
    AND end_date > %(date)s
    AND start_date <= %(date)s
    -- group by job_title
    """
    cursor.execute(query, details)
    return cursor.fetchall()


@database_connection.connection_handler
def check_if_employee_exists(cursor, employee):
    query = """
    SELECT id 
    FROM employees
    WHERE id = %(id)s
    AND company_id = %(company_id)s
    """
    cursor.execute(query, employee)
    return cursor.fetchall()


@database_connection.connection_handler
def update_employee(cursor, employee):
    query = """
    UPDATE employees
    SET job_title = %(job_title)s,
    salary = %(salary)s,
    start_date = %(start_date)s,
    end_date = %(end_date)s
    WHERE id = %(id)s
    AND company_id = %(company_id)s
    """
    cursor.execute(query, employee)
