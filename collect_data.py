import googlemaps
import livepopulartimes
import pandas as pd
from datetime import datetime, timedelta
import os

API_KEY = os.environ["G_MAPS_KEY"]

MURMANSK_CENTER = (68.970682, 33.074690)
SEARCH_RADIUS = 10000
TARGET_TYPES = [
    # 1. "ТРЕТЬИ МЕСТА"
    'cafe',             
    'library',          
    'book_store',       
    'art_gallery',      
    'museum',           

    # 2. ПОТРЕБЛЕНИЕ И КОМФОРТ 
    'shopping_mall',    
    'restaurant',       
    'food_court',       

    # 3. ОТКРЫТЫЕ ПРОСТРАНСТВА
    'park',             
    'town_square',      

    # 4. АКТИВНЫЙ И ВЕЧЕРНИЙ ДОСУГ
    'movie_theater',    
    'bar',              
    'night_club',       
    'bowling_alley',    
    'gym',              

    # 5. ТРАНСПОРТНЫЕ УЗЛЫ
    'train_station',    
    'transit_station'   
]
CSV_FILE = "murmansk_data.csv"

def run_collection():
    # Красивое время запуска (MSK)
    msk_now = datetime.now() + timedelta(hours=3)
    print(f"--- ЗАПУСК СБОРА: {msk_now} (MSK) ---")
    
    gmaps = googlemaps.Client(key=API_KEY)
    
    # 1. Поиск мест
    places_to_check = {}
    for t in TARGET_TYPES:
        try:
            res = gmaps.places_nearby(location=MURMANSK_CENTER, radius=SEARCH_RADIUS, type=t)
            for p in res.get('results', []):
                # Берем места, где больше 10 отзывов (чтобы отсеять мусор)
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
            
            # Добавляем 3 часа к UTC, чтобы в файле было время Мурманска
            msk_time = datetime.now() + timedelta(hours=3)
            
            # Сохраняем ВСЕГДА. Если данных нет (None), пишем 0.
            new_rows.append({
                "timestamp": msk_time.strftime('%Y-%m-%d %H:%M:%S'),
                "place_name": info['name'],
                "place_type": info['type'],
                "place_id": pid,
                "live_popularity": current if current is not None else 0
            })
            
        except Exception:
            # Если ошибка с конкретным местом (нет прав, удалено и т.д.) - пропускаем
            pass

    # 3. Сохранение в CSV
    if new_rows:
        df = pd.DataFrame(new_rows)
        # Если файла нет - создаем с заголовками. Если есть - дописываем.
        file_exists = os.path.isfile(CSV_FILE)
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
        print(f" Добавлено {len(new_rows)} записей в {CSV_FILE}")
    else:
        print(" Нет данных для записи (странно, должно быть хоть что-то с нулями).")

if __name__ == "__main__":
    run_collection()
