import logging
import random
from faker import Faker

from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Grade, Teacher, Student, Group, Subject


fake = Faker('uk-Ua')


# Adding groups
def insert_groups():
    for _ in range(3):
        group = Group(
            name=fake.word()
        )
        session.add(group)


# Adding teachers
def insert_teachers():
    for _ in range(3):
        teacher = Teacher(
            fullname=fake.name()
        )
        session.add(teacher)


# Addition of subjects with the teacher's indication
def insert_subjects():
    teachers = session.query(Teacher).all()
    for _ in range(5):
        subject = Subject(
            name=fake.word(),
            teacher_id=random.choice(teachers).id
        )
        session.add(subject)


# Addition of students with the group's indication
def insert_students():
    groups = session.query(Group).all()
    for _ in range(40):
        student = Student(
            fullname=fake.name(),
            group_id=random.choice(groups).id
        )
        session.add(student)


# Adding grades
def insert_grades():
    students = session.query(Student).all()
    subjects = session.query(Subject).all()

    for student in students:
        number_of_grades = random.randint(10, 20)
        for _ in range(number_of_grades):
            grade = Grade(
                grade=random.randint(0, 100),
                grade_date=fake.date_this_decade(),
                student_id=student.id,
                subjects_id=random.choice(subjects).id
            )
            session.add(grade)


if __name__ == '__main__':
    try:
        insert_groups()
        insert_teachers()
        session.commit()
        insert_subjects()
        insert_students()
        session.commit()
        insert_grades()
        session.commit()
    except SQLAlchemyError as e:
        logging.error(e)
        session.rollback()
    finally:
        session.close()
