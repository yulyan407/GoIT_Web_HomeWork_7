from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    --1. Find the 5 students with the highest GPA across all subjects.
    SELECT
        st.id,
        st.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students as st
    JOIN grades AS g ON st.id = g.student_id
    GROUP BY st.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        st.id,
        st.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades AS g
    JOIN students AS st ON st.id = g.student_id
    WHERE g.subject_id = 1 -- The subject in which you want to find the average grade
    GROUP BY st.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    --3. Find the average grade in groups for a certain subject.
    SELECT
        st.group_id,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades AS g
    JOIN students AS st ON st.id = g.student_id
    WHERE g.subject_id = 1 -- The subject in which you want to find the average grade
    GROUP BY st.group_id
    ORDER BY st.group_id;
    """
    result = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.group_id) \
        .order_by(Student.group_id).all()
    return result


def select_04():
    """
    --4. Find the average grade on the stream (across the entire gradeboard).
    SELECT
        ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).select_from(Grade).all()
    return result


def select_05():
    """
    --5. Find what courses a particular teacher teaches.
    SELECT t.fullname, sub.name
    FROM teachers AS t
    JOIN subjects AS sub ON t.id = sub.teacher_id
    WHERE t.id = 1; -- The teacher for which you want to find the subjects
    """
    result = session.query(Teacher.fullname, Subject.name) \
        .select_from(Teacher).join(Subject).filter(Teacher.id == 1).all()
    return result


def select_06():
    """
    --6. Find a list of students in a specific group.
    SELECT fullname
    FROM students
    WHERE group_id = 1; -- The group for which you want to find the students
    """
    result = session.query(Student.fullname).select_from(Student).filter(Student.group_id == 1).all()
    return result


def select_07():
    """
    --7. Find the grades of students in a separate group for a specific subject.
    SELECT st.fullname, g.grade
    FROM students AS st
    JOIN grades AS g ON st.id = g.student_id
    WHERE st.group_id = 1 -- The group for which you want to find the students
        AND g.subject_id = 1; -- The subject for which you want to find the grades
    """
    result = session.query(Student.fullname, Grade.grade) \
        .select_from(Student).join(Grade).filter(and_(Student.group_id == 1, Grade.subjects_id == 1)).all()
    return result


def select_08():
    """
    --8. Find the average grade given by a certain teacher in his subjects.
    SELECT t.fullname AS teacher, sub.name AS subject, ROUND(AVG(g.grade), 2) AS average_grade
    FROM teachers AS t
    JOIN subjects AS sub ON t.id = sub.teacher_id
    JOIN grades AS g ON sub.id = g.subject_id
    WHERE t.id = 1 -- The teacher for which you want to find the average grades
    GROUP BY t.fullname, sub.name;
    """
    result = session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade'))\
        .select_from(Teacher).join(Subject).join(Grade).filter(Teacher.id == 1)\
        .group_by(Teacher.fullname, Subject.name).all()
    return result


def select_09():
    """
    --9. Find a list of courses a student is taking.
    SELECT sub.name as courses
    FROM grades AS g
    JOIN subjects AS sub ON g.subject_id = sub.id
    WHERE g.student_id = 1 -- The student for which you want to find the courses
    GROUP BY sub.id;
    """
    result = session.query(Subject.name).select_from(Grade).join(Subject).filter(Grade.student_id == 1)\
        .group_by(Subject.id).all()
    return result


def select_10():
    """
    --10. A list of courses taught to a specific student by a specific teacher.
    SELECT sub.name AS subject
    FROM subjects AS sub
    JOIN grades AS g ON sub.id = g.subject_id
    WHERE sub.teacher_id = 1 -- The teacher for which you want to find the courses
        AND g.student_id = 1 -- The student for which you want to find the courses
    GROUP BY subject;
    """
    result = session.query(Subject.name).select_from(Grade).join(Subject)\
        .filter(and_(Subject.teacher_id == 1, Grade.student_id == 1))\
        .group_by(Subject.name).all()
    return result


def select_11():
    """
    --11. The average grade given by a particular teacher to a particular student.
    SELECT t.fullname AS teacher, st.fullname AS student, ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades AS g
    JOIN students AS st ON st.id = g.student_id
    JOIN subjects AS sub ON sub.id = g.subject_id
    JOIN teachers AS t ON t.id = sub.teacher_id
    WHERE g.student_id = 1 -- The student for which you want to find the average grade
        AND t.id = 1 -- The teacher for which you want to find the average grade
    GROUP BY student, teacher;
    """
    result = session.query(Teacher.fullname, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade'))\
        .select_from(Grade).join(Student).join(Subject).join(Teacher)\
        .filter(and_(Grade.student_id == 1, Teacher.id == 1)).group_by(Student.fullname, Teacher.fullname).all()
    return result


def select_12():
    """
    SELECT MAX(grade_date)
    FROM grades AS g
    JOIN students AS st on st.id = g.student_id
    WHERE g.subject_id = 2 and st.group_id = 3;

    SELECT st.id, st.fullname, g.grade, g.grade_date
    FROM grades AS g
    JOIN students AS st on g.student_id = st.id
    WHERE g.subject_id = 2 AND st.group_id = 3 AND g.grade_date = (
        SELECT MAX(grade_date)
        FROM grades AS g2
        JOIN students AS st2 on st2.id=g2.student_id
        WHERE g2.subject_id = 2 AND st2.group_id = 3
    );
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())
    print(select_11())
    print(select_12())
