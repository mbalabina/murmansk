import googlemaps
import livepopulartimes
import pandas as pd
from datetime import datetime
import os

API_KEY = os.environ["G_MAPS_KEY"]

MURMANSK_CENTER = (68.970682, 33.074690)
SEARCH_RADIUS = 10000
TARGET_TYPES = [
    # 1.  "ТРЕТЬИ МЕСТА" (Учеба, общение)
    'cafe',             # Кофейни
    'library',          # Библиотеки
    'book_store',       # Книжные (часто с кафе)
    'art_gallery',      # Галереи
    'museum',           # Музеи

    # 2. ПОТРЕБЛЕНИЕ И КОМФОРТ 
    'shopping_mall',    # ТЦ (ключевые точки)
    'restaurant',       # Рестораны
    'food_court',       # Фудкорты 

    # 3. ОТКРЫТЫЕ ПРОСТРАНСТВА (Для сравнения "улица vs помещение")
    'park',             # Парки
    'town_square',      # Площади

    # 4. АКТИВНЫЙ И ВЕЧЕРНИЙ ДОСУГ
    'movie_theater',    # Кинотеатры
    'bar',              # Бары
    'night_club',       # Клубы
    'bowling_alley',    # Боулинг
    'gym',              # Спортзалы (фитнес)

    # 5. ТРАНСПОРТНЫЕ УЗЛЫ (Маркер ритма города)
    'train_station',    # Вокзал
    'transit_station'   # Остановки
]
CSV_FILE = "murmansk_data.csv"

def run_collection():
    print(f"--- ЗАПУСК СБОРА: {datetime.now()} ---")
    gmaps = googlemaps.Client(key=API_KEY)
    
    # 1. Поиск места 
    places_to_check = {}
    for t in TARGET_TYPES:
        try:
            res = gmaps.places_nearby(location=MURMANSK_CENTER, radius=SEARCH_RADIUS, type=t)
            for p in res.get('results', []):
                if p.get('user_ratings_total', 0) > 10:
                    places_to_check[p['place_id']] = {'name': p['name'], 'type': t}
        except: pass
        
    print(f"Найдено мест: {len(places_to_check)}")

    # 2. Сбор данных
    new_rows = []
    for pid, info in places_to_check.items():
        try:
            details = livepopulartimes.get_populartimes_by_PlaceID(API_KEY, pid)
            current = details.get('current_popularity')
            
            # Сохраняем, даже если нет live-данных (пишем None), чтобы видеть "пропуски"
            # Но лучше сохранять только когда current не None, чтобы не раздувать файл
            if current is not None:
                new_rows.append({
                    "timestamp": datetime.now().isoformat(),
                    "place_name": info['name'],
                    "place_type": info['type'],
                    "place_id": pid,
                    "live_popularity": current
                })
        except: pass

    # 3.  CSV
    if new_rows:
        df = pd.DataFrame(new_rows)
        # Если файла нет - создаем с заголовками. Если есть - дописываем без заголовков.
        file_exists = os.path.isfile(CSV_FILE)
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
        print(f" Добавлено {len(new_rows)} записей в {CSV_FILE}")
    else:
        print("Нет данных для записи.")

if __name__ == "__main__":
    run_collection()
