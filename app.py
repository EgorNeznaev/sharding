from shard_manager import UniversityShardManager
from full import *
def print_menu():
    print("\n" + "="*60)
    print("Database")
    print("="*60)
    print("\nМЕНЮ:")
    print("  1. Показать статистику шардов")
    print("  2. Найти студента по ID")
    print("  3. Показать всех студентов")
    print("  4. Найти студентов с задолженностями")
    print("  5. Добавить нового студента")
    print("  6. Заполнить базу тестовыми данными")
    print("  7. Выход")
    return input("\nВыберите действие: ")
def main():
    db = UniversityShardManager()
    while True:
        choice = print_menu()      
        if choice == '1':
            stats = db.get_shard_stats()
            print("\nСТАТИСТИКА ПО ШАРДАМ:")
            total = {}
            for shard, counts in stats.items():
                print(f"\n{shard}:")
                for coll, count in counts.items():
                    print(f"  {coll}: {count}")
                    total[coll] = total.get(coll, 0) + count       
            print("\nВСЕГО ПО ВСЕМ ШАРДАМ:")
            for coll, count in total.items():
                print(f"  {coll}: {count}")       
        elif choice == '2':
            student_id = input("  Введите ID студента (например ST2025001): ")
            student = db.get_student(student_id)
            if student:
                print(f"\n  Найден студент:")
                print(f"    Имя: {student.get('last_name')} {student.get('first_name')}")
                print(f"    Группа: {student.get('group')}")
                print(f"    Шард: {student.get('_shard')}")
            else:
                print("Студент не найден")        
        elif choice == '3':
            students = db.get_all_students()
            print(f"\nВсего студентов: {len(students)}")
            for s in students[:10]:
                print(f"  {s.get('student_id')}: {s.get('last_name')} {s.get('first_name')} -> {s.get('_shard')}")       
        elif choice == '4':
            debts = db.get_students_with_debts()
            print(f"\nСтуденты с задолженностями: {len(debts)}")
            for d in debts:
                print(f"  {d.get('name')} (оценка: {d.get('grade')}) - шард {d.get('shard')}")       
        elif choice == '5':
            print("\nДОБАВЛЕНИЕ НОВОГО СТУДЕНТА")
            student = {
                "student_id": input("  ID студента: "),
                "first_name": input("  Имя: "),
                "last_name": input("  Фамилия: "),
                "email": input("  Email: "),
                "faculty": input("  Факультет: "),
                "specialty": input("  Специальность: "),
                "group": input("  Группа: "),
                "admission_year": int(input("  Год поступления: "))
            }
            db.add_student(student)      
        elif choice == '6':
            print("\nЗаполнение базы тестовыми данными...")
            pass        
        elif choice == '7':
            break
if __name__ == "__main__":
    main()