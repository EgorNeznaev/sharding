from pymongo import MongoClient, ASCENDING
import hashlib
from datetime import datetime
import random
from bson import ObjectId
class UniversityShardManager:
    def __init__(self):
        self.shards = [
            {'name': 'Shard1', 'host': 'localhost:27017', 'client': None, 'db': None},
            {'name': 'Shard2', 'host': 'localhost:27018', 'client': None, 'db': None},
            {'name': 'Shard3', 'host': 'localhost:27019', 'client': None, 'db': None}
        ]       
        print("Подключение к шардам")
        for shard in self.shards:
            try:
                shard['client'] = MongoClient(shard['host'])
                shard['client'].admin.command('ping')
                shard['db'] = shard['client']['university']
                print(f"  {shard['name']} - {shard['host']}")
            except Exception as e:
                print(f"  {shard['name']} - ошибка: {e}")
                exit(1)    
    def _get_shard_index(self, key):
        hash_val = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        return hash_val % len(self.shards)   
    def _get_shard_for_student(self, student_id):
        idx = self._get_shard_index(student_id)
        return self.shards[idx]
    def add_student(self, student_data):
        student_id = student_data.get('student_id')
        if not student_id:
            raise ValueError("student_id обязателен")       
        shard = self._get_shard_for_student(student_id)
        collection = shard['db']['students']
        existing = collection.find_one({'student_id': student_id})
        if existing:
            print(f"Студент {student_id} уже существует в {shard['name']}, пропускаем")
            return None        
        student_data['created_at'] = datetime.now()
        student_data['updated_at'] = datetime.now()
        student_data['_shard'] = shard['name']
        collection.create_index([('student_id', ASCENDING)], unique=True)        
        result = collection.insert_one(student_data)
        print(f"Студент {student_id} добавлен в {shard['name']}")
        return result   
    def add_students_batch(self, students_list):
        added = 0
        skipped = 0        
        for student in students_list:
            result = self.add_student(student)
            if result:
                added += 1
            else:
                skipped += 1       
        print(f"\nИтог: добавлено {added}, пропущено {skipped} (уже существуют)")
        return added, skipped    
    def get_student(self, student_id):
        shard = self._get_shard_for_student(student_id)
        student = shard['db']['students'].find_one({'student_id': student_id})
        if student:
            print(f"Студент {student_id} найден в {shard['name']}")
        return student
    def add_teacher(self, teacher_data):
        teacher_id = teacher_data.get('teacher_id')
        idx = self._get_shard_index(teacher_id)
        shard = self.shards[idx]       
        collection = shard['db']['teachers']
        existing = collection.find_one({'teacher_id': teacher_id})
        if existing:
            print(f"Преподаватель {teacher_id} уже существует в {shard['name']}")
            return None        
        teacher_data['created_at'] = datetime.now()
        teacher_data['_shard'] = shard['name']      
        collection.create_index([('teacher_id', ASCENDING)], unique=True)
        collection.create_index([('email', ASCENDING)], unique=True)   
        result = collection.insert_one(teacher_data)
        print(f"Преподаватель {teacher_id} добавлен в {shard['name']}")
        return result
    def add_course(self, course_data):
        course_code = course_data.get('course_code')
        idx = self._get_shard_index(course_code)
        shard = self.shards[idx]       
        collection = shard['db']['courses']       
        existing = collection.find_one({'course_code': course_code})
        if existing:
            print(f"Курс {course_code} уже существует в {shard['name']}")
            return None        
        course_data['_shard'] = shard['name']      
        collection.create_index([('course_code', ASCENDING)], unique=True)        
        result = collection.insert_one(course_data)
        print(f"Курс {course_code} добавлен в {shard['name']}")
        return result
    def add_course_group(self, group_data):
        group_code = group_data.get('group_code')
        idx = self._get_shard_index(group_code)
        shard = self.shards[idx]       
        collection = shard['db']['course_groups']        
        existing = collection.find_one({'group_code': group_code})
        if existing:
            print(f"Группа {group_code} уже существует в {shard['name']}")
            return None        
        group_data['_shard'] = shard['name']        
        collection.create_index([('group_code', ASCENDING)], unique=True)        
        result = collection.insert_one(group_data)
        print(f"Группа {group_code} добавлена в {shard['name']}")
        return result
    def add_grade(self, grade_data):
        student_id = grade_data.get('student_id')
        idx = self._get_shard_index(str(student_id))
        shard = self.shards[idx]       
        grade_data['date'] = datetime.now()
        grade_data['_shard'] = shard['name']       
        collection = shard['db']['grades']
        result = collection.insert_one(grade_data)
        return result
    def add_attendance(self, attendance_data):
        student_id = attendance_data.get('student_id')
        idx = self._get_shard_index(str(student_id))
        shard = self.shards[idx]        
        attendance_data['_shard'] = shard['name']        
        collection = shard['db']['attendance']
        result = collection.insert_one(attendance_data)
        return result
    def add_rating(self, rating_data):
        student_id = rating_data.get('student_id')
        idx = self._get_shard_index(str(student_id))
        shard = self.shards[idx]        
        collection = shard['db']['ratings']
        existing = collection.find_one({
            'student_id': student_id,
            'semester': rating_data.get('semester'),
            'year': rating_data.get('year')
        })      
        if existing:
            print(f"Рейтинг для студента {student_id} за {rating_data.get('semester')} семестр {rating_data.get('year')} года уже существует")
            return None        
        rating_data['updated_at'] = datetime.now()
        rating_data['_shard'] = shard['name']        
        result = collection.insert_one(rating_data)
        return result
    def get_shard_stats(self):
        stats = {}
        for shard in self.shards:
            stats[shard['name']] = {
                'students': shard['db']['students'].count_documents({}),
                'teachers': shard['db']['teachers'].count_documents({}),
                'courses': shard['db']['courses'].count_documents({}),
                'course_groups': shard['db']['course_groups'].count_documents({}),
                'grades': shard['db']['grades'].count_documents({}),
                'attendance': shard['db']['attendance'].count_documents({}),
                'ratings': shard['db']['ratings'].count_documents({})
            }
        return stats
    def get_all_students(self):
        all_students = []
        for shard in self.shards:
            students = list(shard['db']['students'].find({}, {'_id': 0}))
            all_students.extend(students)
        return all_students    
    def get_students_with_debts(self):
        results = []
        for shard in self.shards:
            pipeline = [
                {'$match': {'is_final': True, 'grade': {'$lt': 3}}},
                {'$lookup': {
                    'from': 'students',
                    'localField': 'student_id',
                    'foreignField': 'student_id',
                    'as': 'student'
                }},
                {'$unwind': '$student'},
                {'$project': {
                    'student_id': '$student.student_id',
                    'name': {'$concat': ['$student.last_name', ' ', '$student.first_name']},
                    'grade': 1,
                    'shard': shard['name']
                }}
            ]
            debts = list(shard['db']['grades'].aggregate(pipeline))
            results.extend(debts)
        return results    
    def clear_all_collections(self):
        confirm = input("yes/no: ")
        if confirm.lower() == 'yes':
            for shard in self.shards:
                shard['db']['students'].delete_many({})
                shard['db']['teachers'].delete_many({})
                shard['db']['courses'].delete_many({})
                shard['db']['course_groups'].delete_many({})
                shard['db']['grades'].delete_many({})
                shard['db']['attendance'].delete_many({})
                shard['db']['ratings'].delete_many({})
            print("Все данные удалены")
        else:
            print("Операция отменена")