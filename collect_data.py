import googlemaps
import livepopulartimes
import pandas as pd
from datetime import datetime, timedelta
import os
import time

API_KEY = os.environ["G_MAPS_KEY"]

# Точки поиска: Центр, Север (Алеша), Юг (Северное Нагорное)
SEARCH_POINTS = [
    (68.970682, 33.074690), 
    (68.997285, 33.069177), 
    (68.914757, 33.090333)  
]

# Ищем в радиусе 3 км от каждой точки (покроет весь город)
SEARCH_RADIUS = 3000

TARGET_TYPES = [
    'cafe',             
    'library',          
    'book_store',       
    'art_gallery',      
    'museum',           
    'shopping_mall',    
    'restaurant',       
    'food_court',       
    'park',             
    'town_square',      
    'movie_theater',    
    'bar',              
    'night_club',       
    'bowling_alley',    
    'gym',              
    'train_station',    
    'transit_station'   
]
CSV_FILE = "murmansk_data_v2.csv"

def run_collection():
    msk_now = datetime.now() + timedelta(hours=3)
    print(f"--- ЗАПУСК СБОРА: {msk_now.strftime('%Y-%m-%d %H:%M:%S')} (MSK) ---")
    
    gmaps = googlemaps.Client(key=API_KEY)
    places_to_check = {} # Словарь для уникальных мест
    
    # --- ЭТАП 1: поиск
    print(" Сканируем город (3 района)...")
    
    for point in SEARCH_POINTS:
        for t in TARGET_TYPES:
            try:
                # 1. Первая страница результатов
                res = gmaps.places_nearby(location=point, radius=SEARCH_RADIUS, type=t)
                if 'results' in res:
                    for p in res['results']:
                        if p.get('user_ratings_total', 0) > 10: # Фильтр мусора
                            places_to_check[p['place_id']] = {'name': p['name'], 'type': t}

                # 2. Вторая страница (если есть) - чтобы найти >20 мест
                if 'next_page_token' in res:
                    time.sleep(2) # Пауза обязательна для Google API
                    try:
                        res_next = gmaps.places_nearby(location=point, radius=SEARCH_RADIUS, type=t, page_token=res['next_page_token'])
                        for p in res_next.get('results', []):
                            if p.get('user_ratings_total', 0) > 10:
                                places_to_check[p['place_id']] = {'name': p['name'], 'type': t}
                    except: pass
                    
            except Exception: 
                pass
        
    print(f" Найдено уникальных мест для слежки: {len(places_to_check)}")

    # --- ЭТАП 2: СБОР ДАННЫХ 
    new_rows = []
    print(" Собираем загруженность...")
    
    for pid, info in places_to_check.items():
        try:
            details = livepopulartimes.get_populartimes_by_PlaceID(API_KEY, pid)
            current = details.get('current_popularity')
            
            # Время Мурманска для записи
            timestamp_msk = (datetime.now() + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Сохраняем ВСЕГДА (пишем 0, если данных нет)
            new_rows.append({
                "timestamp": timestamp_msk,
                "place_name": info['name'],
                "place_type": info['type'],
                "place_id": pid,
                "live_popularity": current if current is not None else 0
            })
            
        except Exception:
            pass

    # --- ЭТАП 3: СОХРАНЕНИЕ В CSV ---
    if new_rows:
        df = pd.DataFrame(new_rows)
        # Если файла нет - создаем, если есть - дописываем
        file_exists = os.path.isfile(CSV_FILE)
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
        print(f" УСПЕХ: Добавлено {len(new_rows)} записей в {CSV_FILE}")
    else:
        print(" Странно: список мест пуст или произошла ошибка.")

if __name__ == "__main__":
    run_collection()
