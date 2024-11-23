import sqlite3
import random
from datetime import date, timedelta
from faker import Faker

class UniversityDatabaseManager:
    def __init__(self, db_name='university_database.db'):
        self.db_name = db_name
        self.fake = Faker()
        self.conn = None
        self.cursor = None
        self.used_emails = set()

    def create_connection(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def generate_unique_email(self, first_name, last_name):
        """Generate a unique email address"""
        base_email = f"{first_name.lower()}.{last_name.lower()}"
        email = f"{base_email}@university.edu"
        counter = 1
        
        while email in self.used_emails:
            email = f"{base_email}{counter}@university.edu"
            counter += 1
        
        self.used_emails.add(email)
        return email

    def create_tables(self):
        """Create database schema"""
        # Drop existing tables to prevent constraint issues
        tables = ['Enrollments', 'Courses', 'Departments', 'Students']
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # Students Table
        self.cursor.execute('''
        CREATE TABLE Students (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            gender TEXT CHECK(gender IN ('Male', 'Female', 'Other')),
            date_of_birth DATE,
            email TEXT UNIQUE,
            enrollment_status TEXT CHECK(enrollment_status IN ('Active', 'Inactive', 'Graduated', 'Suspended')),
            total_credits INTEGER,
            gpa REAL CHECK(gpa BETWEEN 0.0 AND 4.0)
        )''')

        # Departments Table
        self.cursor.execute('''
        CREATE TABLE Departments (
            department_id INTEGER PRIMARY KEY AUTOINCREMENT,
            department_name TEXT UNIQUE NOT NULL,
            established_year INTEGER
        )''')

        # Courses Table
        self.cursor.execute('''
        CREATE TABLE Courses (
            course_id TEXT PRIMARY KEY,
            course_name TEXT NOT NULL,
            department_id INTEGER,
            credit_hours INTEGER,
            course_level TEXT CHECK(course_level IN ('Introductory', 'Intermediate', 'Advanced')),
            FOREIGN KEY(department_id) REFERENCES Departments(department_id)
        )''')

        # Enrollments Table
        self.cursor.execute('''
        CREATE TABLE Enrollments (
            enrollment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER,
            course_id TEXT,
            semester TEXT CHECK(semester IN ('Fall', 'Spring', 'Summer')),
            academic_year INTEGER,
            grade REAL CHECK(grade BETWEEN 0.0 AND 4.0),
            FOREIGN KEY(student_id) REFERENCES Students(student_id),
            FOREIGN KEY(course_id) REFERENCES Courses(course_id),
            UNIQUE(student_id, course_id, semester, academic_year)
        )''')

    def generate_departments(self):
        """Generate department data"""
        departments = [
            ('Computer Science', 1985),
            ('Mathematics', 1970),
            ('Physics', 1960),
            ('Biology', 1975),
            ('Chemistry', 1965),
            ('Engineering', 1980),
            ('Economics', 1990)
        ]
        self.cursor.executemany("""
            INSERT INTO Departments (department_name, established_year) 
            VALUES (?, ?)
        """, departments)

    def generate_courses(self):
        """Generate courses for each department"""
        self.cursor.execute("SELECT department_id, department_name FROM Departments")
        departments = self.cursor.fetchall()

        courses = []
        course_levels = ['Introductory', 'Intermediate', 'Advanced']
        for dept_id, dept_name in departments:
            for level in course_levels:
                course_name = f"{dept_name} {level} Course"
                course_id = f"{dept_name[:3].upper()}{random.randint(100, 999)}"
                credit_hours = random.choice([3, 4])
                
                courses.append((course_id, course_name, dept_id, credit_hours, level))
        
        self.cursor.executemany("""
            INSERT INTO Courses 
            (course_id, course_name, department_id, credit_hours, course_level) 
            VALUES (?, ?, ?, ?, ?)
        """, courses)

    def generate_students(self, num_students=1200):
        """Generate student data"""
        gender_options = ['Male', 'Female', 'Other']
        status_options = ['Active', 'Inactive', 'Graduated', 'Suspended']
        
        students = []
        for _ in range(num_students):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            gender = random.choice(gender_options)
            dob = self.fake.date_of_birth(minimum_age=18, maximum_age=30)
            email = self.generate_unique_email(first_name, last_name)
            status = random.choice(status_options)
            total_credits = random.randint(0, 120)
            gpa = round(random.uniform(2.0, 4.0), 2)

            students.append((
                first_name, last_name, gender, dob, email, 
                status, total_credits, gpa
            ))

        self.cursor.executemany("""
            INSERT INTO Students 
            (first_name, last_name, gender, date_of_birth, 
            email, enrollment_status, total_credits, gpa) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, students)

    def generate_enrollments(self):
        """Generate enrollment data"""
        # Get all students and courses
        self.cursor.execute("SELECT student_id FROM Students")
        students = [student[0] for student in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT course_id FROM Courses")
        courses = [course[0] for course in self.cursor.fetchall()]
        
        semesters = ['Fall', 'Spring', 'Summer']
        academic_years = list(range(2018, 2024))

        enrollments = []
        # Each student enrolls in 3-5 courses
        for student_id in students:
            num_courses = random.randint(3, 5)
            enrolled_courses = random.sample(courses, num_courses)
            
            for course_id in enrolled_courses:
                semester = random.choice(semesters)
                academic_year = random.choice(academic_years)
                grade = round(random.uniform(2.0, 4.0), 2)

                enrollments.append((
                    student_id, course_id, semester, 
                    academic_year, grade
                ))

        self.cursor.executemany("""
            INSERT INTO Enrollments 
            (student_id, course_id, semester, academic_year, grade) 
            VALUES (?, ?, ?, ?, ?)
        """, enrollments)

    def run_database_generation(self):
        """Execute full database generation process"""
        try:
            self.create_connection()
            self.create_tables()
            
            self.generate_departments()
            self.conn.commit()
            
            self.generate_courses()
            self.conn.commit()
            
            self.generate_students()
            self.conn.commit()
            
            self.generate_enrollments()
            self.conn.commit()
            
            print("University Database generated successfully!")
            
            # Verification queries
            self.cursor.execute("SELECT COUNT(*) FROM Students")
            student_count = self.cursor.fetchone()[0]
            print(f"Total Students: {student_count}")
            
            self.cursor.execute("SELECT COUNT(*) FROM Courses")
            course_count = self.cursor.fetchone()[0]
            print(f"Total Courses: {course_count}")
            
            self.cursor.execute("SELECT COUNT(*) FROM Enrollments")
            enrollment_count = self.cursor.fetchone()[0]
            print(f"Total Enrollments: {enrollment_count}")
            
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            if self.conn:
                self.conn.close()

def main():
    db_manager = UniversityDatabaseManager()
    db_manager.run_database_generation()

if __name__ == "__main__":
    main()