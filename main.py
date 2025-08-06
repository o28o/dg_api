from fastapi import FastAPI
from typing import List, Optional
from pydantic import BaseModel
import subprocess
import json
import re

# --- Configuration ---

# Карта, связывающая короткие имена коллекций с их реальными путями.
# Это центральное место для управления источниками данных.
COLLECTION_PATHS = {
    # SuttaCentral Pali
    "sc_pali": {
        "dn": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/dn",
        "mn": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/mn",
        "sn": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/sn",
        "an": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/an",
        "kn_dhp": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/dhp",
        "kn_ud": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/ud",
        "kn_iti": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/iti",
        "kn_snp": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/snp",
        "kn_thag": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/thag",
        "kn_thig": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/thig",
        "vinaya": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya",
        "abhidhamma": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/abhidhamma",
    },
    # SuttaCentral English
    "sc_en": {
        "dn": "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/translation/en/sujato/sutta/dn",
        # ... и так далее для других переводов
    },
    # Другие источники
    "tbw": {
        "root": "/var/www/html/bw/root",
        "translation": "/var/www/html/bw/translation"
    },
    "theravada_ru": {
        "canon": "/var/www/html/theravada.ru/Teaching/Canon/Suttanta"
    }
}

# --- Pydantic Models (Data Structures) ---

# Модели для структурирования ответа API.
# Это помогает FastAPI автоматически валидировать данные и генерировать документацию.

class Quote(BaseModel):
    pi: Optional[str] = None
    en: Optional[str] = None
    ru: Optional[str] = None

class QuoteResult(BaseModel):
    sutta_id: str
    type: str
    count: int
    title: Optional[str] = None
    quotes: List[Quote]

class SearchResponse(BaseModel):
    data: List[QuoteResult]

# --- FastAPI Application ---

app = FastAPI()

@app.get("/api/v1/search", response_model=SearchResponse)
def search(
    q: str,
    source: str = "sc_pali",
    collections: Optional[str] = None,
    method: str = "all_matches",
    report_type: str = "quotes"
):
    """
    Эндпоинт для поиска по текстам.
    
    - q: Строка для поиска.
    - source: Источник данных (например, "sc_pali").
    - collections: Коллекции для поиска через запятую (например, "dn,mn,kn_dhp").
    - method: Метод поиска ("all_matches", "definitions", "similes").
    - report_type: Тип отчета ("quotes", "words").
    """
    
    # 1. Определить пути для поиска
    paths_to_search = []
    if source in COLLECTION_PATHS:
        source_collections = COLLECTION_PATHS[source]
        if collections:
            # Пользовательский выбор коллекций
            collection_keys = collections.split(',')
            for key in collection_keys:
                if key in source_collections:
                    paths_to_search.append(source_collections[key])
        else:
            # Выбор по умолчанию (все коллекции в источнике)
            paths_to_search = list(source_collections.values())

    if not paths_to_search:
        return {"data": []}

    # 2. Сформировать команду для ripgrep
    # (Здесь будет логика для разных 'method')
    search_term = q
    command = ["rg", "--json", "-i", search_term] + paths_to_search
    
    # 3. Выполнить поиск
    # В реальном приложении здесь будет вызов subprocess
    # process = subprocess.run(command, capture_output=True, text=True, check=True)
    # ripgrep_output = process.stdout
    
    # --- MOCK DATA (для демонстрации) ---
    # Заменим реальный вызов rg на моковые данные, чтобы проверить логику парсинга.
    # В следующем шаге мы заменим это на реальный вызов.
    # --- REAL SEARCH ---
    try:
        # Используем -i для регистронезависимого поиска и --glob для фильтрации файлов
        command = ["rg", "--json", "-i", search_term] + paths_to_search
        process = subprocess.run(command, capture_output=True, text=True, check=False) # check=False, чтобы не падать, если ничего не найдено
        ripgrep_output = process.stdout
    except FileNotFoundError:
        # Если ripgrep не установлен
        return {"data": []}
    # --- END REAL SEARCH ---
    ripgrep_output = mock_output
    # --- END MOCK DATA ---

    # 4. Обработать результаты
    results = {} # Словарь для группировки по sutta_id
    
    for line in ripgrep_output.strip().split('\n'):
        try:
            entry = json.loads(line)
            if entry.get("type") == "match":
                path = entry["data"]["path"]["text"]
                quote_text = entry["data"]["lines"]["text"].strip()
                
                # Извлекаем sutta_id из пути, он обычно идет после последней части коллекции
                sutta_id_match = re.search(r'([^/]+)_root-pli-ms.json', path)
                if not sutta_id_match:
                    sutta_id_match = re.search(r'([^/]+)_translation-..-.*.json', path)
                
                sutta_id = sutta_id_match.group(1) if sutta_id_match else "unknown"

                # Определяем тип по пути
                sutta_type = "unknown"
                if "/sutta/kn/" in path:
                    sutta_type = "khudakka"
                elif "/vinaya/" in path:
                    sutta_type = "vinaya"
                elif "/sutta/" in path:
                    sutta_type = "dhamma"
                
                # Группируем результаты
                if sutta_id not in results:
                    results[sutta_id] = {
                        "sutta_id": sutta_id,
                        "type": sutta_type,
                        "count": 0,
                        "quotes": []
                    }
                
                results[sutta_id]["count"] += 1
                results[sutta_id]["quotes"].append({"pi": quote_text})

        except (json.JSONDecodeError, KeyError):
            # Игнорируем строки, которые не являются валидным JSON от ripgrep
            continue
            
    # 5. Сформировать и вернуть ответ
    formatted_results = list(results.values())
    return {"data": formatted_results}

# Для локального запуска: uvicorn main:app --reload
