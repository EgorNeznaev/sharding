from shard_manager import UniversityShardManager
import time
import random
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import hashlib
class LoadTester:
    def __init__(self):
        self.db = UniversityShardManager()
        self.results = {}     
    def generate_test_students(self, count):
        students = []
        faculties = ["Факультет компьютерных наук", "Факультет математики", "Факультет физики"]
        specialties = ["Программная инженерия", "Прикладная математика", "Физика"]
        names = ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Васильев"]      
        for i in range(count):
            student_id = f"TEST{datetime.now().strftime('%Y%m%d')}{i:04d}"
            students.append({
                "student_id": student_id,
                "first_name": f"Test{i}",
                "last_name": random.choice(names),
                "email": f"{student_id}@test.ru",
                "faculty": random.choice(faculties),
                "specialty": random.choice(specialties),
                "group": f"ГР-{random.randint(1,5)}",
                "admission_year": 2025
            })
        return students    
    def test_insert_speed(self, counts=[100, 500, 1000, 5000]):
        print("ТЕСТ 1: СКОРОСТЬ ВСТАВКИ ДАННЫХ")       
        insert_times = []       
        for count in counts:
            students = self.generate_test_students(count)
            start = time.time()
            for student in students:
                self.db.add_student(student)
            end = time.time()            
            elapsed = end - start
            speed = count / elapsed
            insert_times.append(elapsed)           
            print(f"\n Вставка {count} записей:")
            print(f"   Время: {elapsed:.2f} сек")
            print(f"   Скорость: {speed:.0f} записей/сек")
            print(f"   Среднее время на запись: {(elapsed/count)*1000:.2f} мс")        
        self.results['insert'] = {
            'counts': counts,
            'times': insert_times
        }        
        return insert_times    
    def test_read_speed(self, iterations=1000):
        print("ТЕСТ 2: СКОРОСТЬ ЧТЕНИЯ ПО ID")
        all_students = self.db.get_all_students()
        if not all_students:
            print("Нет данных для тестирования чтения")
            return        
        student_ids = [s['student_id'] for s in all_students]
        read_times = []
        for i in range(iterations):
            student_id = random.choice(student_ids)            
            start = time.time()
            self.db.get_student(student_id)
            end = time.time()           
            read_times.append((end - start) * 1000)  
        avg_time = np.mean(read_times)
        median_time = np.median(read_times)
        p95_time = np.percentile(read_times, 95)
        p99_time = np.percentile(read_times, 99)
        max_time = max(read_times)
        min_time = min(read_times)       
        print(f"\nСтатистика чтения ({iterations} запросов):")
        print(f"   Минимальное время: {min_time:.2f} мс")
        print(f"   Максимальное время: {max_time:.2f} мс")
        print(f"   Среднее время: {avg_time:.2f} мс")
        print(f"   Медианное время: {median_time:.2f} мс")
        print(f"   95-й перцентиль: {p95_time:.2f} мс")
        print(f"   99-й перцентиль: {p99_time:.2f} мс")       
        self.results['read'] = {
            'avg': avg_time,
            'median': median_time,
            'p95': p95_time,
            'p99': p99_time,
            'min': min_time,
            'max': max_time,
            'all_times': read_times[:100]  
        }       
        return read_times    
    def test_parallel_read(self):
        print("ТЕСТ 3: ПАРАЛЛЕЛЬНОЕ ЧТЕНИЕ")       
        start = time.time()
        all_students = self.db.get_all_students()
        end = time.time()        
        elapsed = end - start        
        print(f"\nПолучение всех студентов ({len(all_students)} записей):")
        print(f"   Время: {elapsed:.3f} сек")
        print(f"   Скорость: {len(all_students)/elapsed:.0f} записей/сек")        
        self.results['parallel'] = {
            'students_count': len(all_students),
            'time': elapsed
        }   
    def test_shard_distribution(self):
        print("ТЕСТ 4: РАВНОМЕРНОСТЬ РАСПРЕДЕЛЕНИЯ")       
        stats = self.db.get_shard_stats()
        total = sum(s['students'] for s in stats.values())        
        print("\nРаспределение студентов по шардам:")
        distribution = []       
        for shard_name, counts in stats.items():
            students_count = counts['students']
            percentage = (students_count / total * 100) if total > 0 else 0
            distribution.append(students_count)
            bar = "█" * int(percentage / 2)
            print(f"\n{shard_name}:")
            print(f"   {students_count} записей ({percentage:.1f}%)")
            print(f"   [{bar:<50}]")
        if len(distribution) > 1:
            mean = np.mean(distribution)
            std = np.std(distribution)
            cv = (std / mean) * 100          
            print(f"\nАнализ распределения:")
            print(f"   Среднее: {mean:.0f} записей")
            print(f"   Стандартное отклонение: {std:.1f}")
            print(f"   Коэффициент вариации: {cv:.1f}%")           
            if cv < 15:
                print("   Распределение равномерное")
            else:
                print("   Распределение неравномерное")       
        self.results['distribution'] = stats   
    def compare_without_sharding(self, count=500):
        """Сравнение с работой без шардинга (только один шард)"""
        print("\n" + "="*60)
        print("ТЕСТ 5: СРАВНЕНИЕ С РАБОТОЙ БЕЗ ШАРДИНГА")
        students = self.generate_test_students(count)
        print("\nТест на одном шарде (без шардинга):")
        single_shard_start = time.time()
        for student in students[:count//2]:
            student_id = student['student_id']
            shard = self.db.shards[0] 
            collection = shard['db']['students']
            collection.insert_one(student)
        single_shard_time = time.time() - single_shard_start       
        print(f"   Вставка {count//2} записей: {single_shard_time:.3f} сек")
        print(f"   Скорость: {(count//2)/single_shard_time:.0f} записей/сек")
        print("\nТест на трёх шардах (с шардингом):")
        multi_shard_start = time.time()
        for student in students[count//2:]:
            self.db.add_student(student)
        multi_shard_time = time.time() - multi_shard_start        
        print(f"   Вставка {count//2} записей: {multi_shard_time:.3f} сек")
        print(f"   Скорость: {(count//2)/multi_shard_time:.0f} записей/сек")
        speedup = single_shard_time / multi_shard_time
        print(f"\nУскорение при шардировании: {speedup:.2f}x")
        
        self.results['comparison'] = {
            'single_shard_time': single_shard_time,
            'multi_shard_time': multi_shard_time,
            'speedup': speedup
        }    
    def plot_results(self):
        """Построение графиков результатов"""
        print("\nПостроение графиков...")        
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle('Результаты нагрузочного тестирования шардирования', fontsize=16)
        if 'insert' in self.results:
            ax1 = axes[0, 0]
            data = self.results['insert']
            ax1.plot(data['counts'], data['times'], 'bo-', linewidth=2, markersize=8)
            ax1.set_xlabel('Количество записей')
            ax1.set_ylabel('Время (сек)')
            ax1.set_title('Скорость вставки данных')
            ax1.grid(True, alpha=0.3)
        if 'read' in self.results:
            ax2 = axes[0, 1]
            read_data = self.results['read']
            times = read_data['all_times']
            ax2.hist(times, bins=20, color='green', alpha=0.7, edgecolor='black')
            ax2.axvline(read_data['avg'], color='red', linestyle='--', label=f"Среднее: {read_data['avg']:.1f}мс")
            ax2.axvline(read_data['median'], color='blue', linestyle='--', label=f"Медиана: {read_data['median']:.1f}мс")
            ax2.set_xlabel('Время ответа (мс)')
            ax2.set_ylabel('Количество запросов')
            ax2.set_title('Распределение времени чтения')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        if 'read' in self.results:
            ax3 = axes[0, 2]
            read_data = self.results['read']
            percentiles = ['min', 'p95', 'p99', 'max']
            values = [read_data['min'], read_data['p95'], read_data['p99'], read_data['max']]
            colors = ['green', 'orange', 'red', 'darkred']
            bars = ax3.bar(percentiles, values, color=colors, alpha=0.7)
            ax3.set_xlabel('Перцентили')
            ax3.set_ylabel('Время (мс)')
            ax3.set_title('Перцентили времени ответа')
            for bar, val in zip(bars, values):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{val:.1f}', ha='center', va='bottom', fontsize=9)
        if 'distribution' in self.results:
            ax4 = axes[1, 0]
            stats = self.results['distribution']
            shards = list(stats.keys())
            students_counts = [stats[s]['students'] for s in shards]
            colors = ['#ff9999', '#66b3ff', '#99ff99']
            bars = ax4.bar(shards, students_counts, color=colors, alpha=0.7)
            ax4.set_xlabel('Шард')
            ax4.set_ylabel('Количество студентов')
            ax4.set_title('Распределение студентов по шардам')
            for bar, val in zip(bars, students_counts):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{val}', ha='center', va='bottom', fontsize=10)
        if 'comparison' in self.results:
            ax5 = axes[1, 1]
            comp = self.results['comparison']
            labels = ['Один шард', 'Три шарда']
            times = [comp['single_shard_time'], comp['multi_shard_time']]
            colors = ['#ff9999', '#99ff99']
            bars = ax5.bar(labels, times, color=colors, alpha=0.7)
            ax5.set_ylabel('Время (сек)')
            ax5.set_title('Сравнение производительности')
            for bar, val in zip(bars, times):
                height = bar.get_height()
                ax5.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{val:.2f}с', ha='center', va='bottom', fontsize=10)
            ax5.text(0.5, 0.9, f'Ускорение: {comp["speedup"]:.2f}x', 
                    transform=ax5.transAxes, ha='center', fontsize=12, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))
        ax6 = axes[1, 2]
        ax6.axis('off')
        text = f"ИТОГИ ТЕСТИРОВАНИЯ\n\n"
        if 'insert' in self.results:
            text += f"Вставка 1000 записей:\n"
            text += f"  {self.results['insert']['times'][-1]:.2f} сек\n\n"
        if 'read' in self.results:
            text += f"Чтение (среднее):\n"
            text += f"  {self.results['read']['avg']:.2f} мс\n\n"
        if 'comparison' in self.results:
            text += f"Ускорение:\n"
            text += f"  {self.results['comparison']['speedup']:.2f}x\n\n"
        
        ax6.text(0.1, 0.5, text, transform=ax6.transAxes, fontsize=14,
                verticalalignment='center', fontfamily='monospace',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.7))
        
        plt.tight_layout()
        plt.savefig('sharding_benchmark_results.png', dpi=100, bbox_inches='tight')
        print("Графики сохранены в 'sharding_benchmark_results.png'")
        plt.show()
    
    def save_report(self):
        """Сохранение отчета"""
        with open('benchmark_report.txt', 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ ШАРДИРОВАНИЯ\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*70 + "\n\n")
            
            f.write("1. СКОРОСТЬ ВСТАВКИ\n")
            f.write("-"*40 + "\n")
            if 'insert' in self.results:
                for i, (count, time_taken) in enumerate(zip(
                    self.results['insert']['counts'],
                    self.results['insert']['times']
                )):
                    f.write(f"{count} записей: {time_taken:.3f} сек ({count/time_taken:.0f} зап/сек)\n")
            
            f.write("\n2. СКОРОСТЬ ЧТЕНИЯ\n")
            f.write("-"*40 + "\n")
            if 'read' in self.results:
                r = self.results['read']
                f.write(f"Среднее: {r['avg']:.2f} мс\n")
                f.write(f"Медиана: {r['median']:.2f} мс\n")
                f.write(f"95-й перцентиль: {r['p95']:.2f} мс\n")
                f.write(f"99-й перцентиль: {r['p99']:.2f} мс\n")
                f.write(f"Мин: {r['min']:.2f} мс\n")
                f.write(f"Макс: {r['max']:.2f} мс\n")
            
            f.write("\n3. РАСПРЕДЕЛЕНИЕ ПО ШАРДАМ\n")
            f.write("-"*40 + "\n")
            if 'distribution' in self.results:
                for shard, counts in self.results['distribution'].items():
                    f.write(f"{shard}: {counts['students']} студентов\n")
            
            f.write("\n4. СРАВНЕНИЕ С ОДНИМ ШАРДОМ\n")
            f.write("-"*40 + "\n")
            if 'comparison' in self.results:
                c = self.results['comparison']
                f.write(f"Один шард: {c['single_shard_time']:.3f} сек\n")
                f.write(f"Три шарда: {c['multi_shard_time']:.3f} сек\n")
                f.write(f"Ускорение: {c['speedup']:.2f}x\n")
            
            f.write("\n" + "="*70 + "\n")
            f.write("ВЫВОД: Шардирование эффективно распределяет данные ")
            if 'comparison' in self.results and self.results['comparison']['speedup'] > 1.5:
                f.write("и значительно ускоряет операции.\n")
            else:
                f.write("и обеспечивает отказоустойчивость.\n")
        
        print("Отчет сохранен в 'benchmark_report.txt'")
    
    def run_all_tests(self):
        print("ЗАПУСК НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")        
        print("Проверка подключения к шардам")
        stats = self.db.get_shard_stats()
        for shard, counts in stats.items():
            print(f"  {shard}: {counts['students']} студентов")
        input("\nНажмите Enter для теста вставки...")
        self.test_insert_speed([100, 500])
        input("\nНажмите Enter для теста чтения...")
        self.test_read_speed(500)
        input("\nНажмите Enter для теста параллельного чтения...")
        self.test_parallel_read()
        input("\nНажмите Enter для проверки распределения...")
        self.test_shard_distribution()
        input("\nНажмите Enter для сравнения с одним шардом...")
        self.compare_without_sharding(200)
        self.plot_results()
        self.save_report()
        print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")

if __name__ == "__main__":
    tester = LoadTester()
    tester.run_all_tests()