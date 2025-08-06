
import subprocess
import json
import re
import os
from functools import lru_cache
from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union

# --- Global Caches ---
TEXT_INFO_CACHE = {}

# --- Configuration ---

# 1. Granular source paths
SOURCE_PATHS: Dict[str, Union[str, List[str]]] = {
    'dn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/dn",
    'mn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/mn",
    'sn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/sn",
    'an': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/an",
    'kn_dhp': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/dhp",
    'kn_ud': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/ud",
    'kn_iti': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/iti",
    'kn_snp': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/snp",
    'kn_thag': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/thag",
    'kn_thig': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn/thig",
    'kn_full': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/sutta/kn",
    'vin_vb': [
        "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya/pli-tv-bu-vb",
        "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya/pli-tv-bi-vb",
    ],
    'vin_kd_pvr': [
        "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya/pli-tv-kd",
        "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya/pli-tv-pvr",
    ],
    'vin_full': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/root/pli/ms/vinaya",
    'tbw': "/var/www/html/bw/",
    'theravada_ru': "/var/www/html/theravada.ru/Teaching/Canon/Suttanta",
    'sc_en_dn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/translation/en/sujato/sutta/dn",
    'sc_en_mn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/translation/en/sujato/sutta/mn",
    'sc_en_sn': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/translation/en/sujato/sutta/sn",
    'sc_en_an': "/var/www/html/suttacentral.net/sc-data/sc_bilara_data/translation/en/sujato/sutta/an",
}

TEXT_INFO_PATH = "/var/www/dg_api/textinfo.js"

SCOPE_PRESETS = {
    'pali_4n_6kn': ['dn', 'mn', 'sn', 'an', 'kn_dhp', 'kn_ud', 'kn_iti', 'kn_snp', 'kn_thag', 'kn_thig'],
    'four_nikayas': ['dn', 'mn', 'sn', 'an'],
    'vinaya': ['vin_vb'],
    'all_kn': ['dn', 'mn', 'sn', 'an', 'kn_full'],
    'vinaya_plus_kd_pvr': ['vin_full'],
    'tbw': ['tbw'],
    'sc': ['sc_en_dn', 'sc_en_mn', 'sc_en_sn', 'sc_en_an'],
    'theravada_ru': ['theravada_ru'],
}

SEARCH_PATTERNS = {
    'definitions': r"\bKata.{0,40} {keyword}.{0,9}[?,]|\bKo .{0,40}{keyword}|\bayaṁ .{0,40}{keyword}|\bKatha.{0,40} \b{keyword}.{0,5}[?,]|{keyword}.{0,15}, {keyword}.{0,25} vucca|{keyword}.{0,25} vucca|Kiñ.*{keyword}.{0,9} va|{keyword}.*ariyassa vinaye|ariyassa vinaye.*{keyword}",
    'comparisons': r"seyyathāpi.*{keyword}|{keyword}.*adhivacan|{keyword}.*(ūpam|upam|opam|opamm)|(ūpam|upam|opam|opamm).*{keyword}",
}

EXCLUSION_PATTERNS = {
    'comparisons': r"condition|adhivacanasamphass|adhivacanapath|\banopam|\battūpa|\bnillopa|opamaññ"
}

class DataTablesResponse(BaseModel):
    draw: int
    recordsTotal: int
    recordsFiltered: int
    data: List[Dict[str, Any]]

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8001", # Your test_datatable.html server
    "http://127.0.0.1",
    "http://127.0.0.1:8001", # Your test_datatable.html server
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.on_event("startup")
def load_text_info():
    global TEXT_INFO_CACHE
    if not os.path.exists(TEXT_INFO_PATH):
        print(f"Warning: Text info file not found at {TEXT_INFO_PATH}")
        return
    try:
        with open(TEXT_INFO_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
            content = re.sub(r'^\s*var\s+TextInfo\s*=\s*', '', content, count=1)
            content = re.sub(r';\s*$', '', content, count=1)
            content = re.sub(r',(\s*[}\]])', r'\1', content)
            TEXT_INFO_CACHE = json.loads(content)
        print(f"Loaded {len(TEXT_INFO_CACHE)} text info records into cache.")
    except Exception as e:
        print(f"Error loading or parsing {TEXT_INFO_PATH}: {e}")

def run_ripgrep(pattern: str, paths: List[str], case_insensitive: bool = True, exclude_pattern: Optional[str] = None) -> List[Dict]:
    command = ["rg", "--json"]
    if case_insensitive:
        command.append("-i")
    command.extend(["-e", pattern])
    command.extend(paths)
    try:
        process = subprocess.run(command, capture_output=True, text=True, check=False)
        if process.returncode not in [0, 1]: return []
        output = process.stdout
        if exclude_pattern:
            exclude_command = ["rg", "-v", "-e", exclude_pattern]
            process = subprocess.run(exclude_command, input=output, capture_output=True, text=True, check=False)
            if process.returncode not in [0, 1]: return []
            output = process.stdout
        results = []
        for line in output.strip().split('\n'):
            if line:
                try: results.append(json.loads(line))
                except json.JSONDecodeError: pass
        return results
    except FileNotFoundError: return []
    except Exception: return []

@lru_cache(maxsize=None)
def get_text_info(sutta_id: str) -> Dict:
    info = TEXT_INFO_CACHE.get(sutta_id, {})
    return {
        "title_pi": info.get("pi", ""),
        "title_en": info.get("en", ""),
        "title_ru": info.get("ru", ""),
        "metaphor_count": int(info.get("mtph")) if str(info.get("mtph")).isdigit() else 0
    }

def get_sutta_id_from_path(path: str) -> str:
    match = re.search(r'([^/]+)_(root|translation)-.*\.json$', path)
    if match: return match.group(1)
    return os.path.basename(path).split('.')[0]

@app.get("/api/search", response_model=DataTablesResponse)
def search(
    q: str,
    draw: int = 1,
    start: int = 0,
    length: int = 10,
    scope: str = "pali_4n_6kn",
    collections: Optional[str] = None,
    mode: str = "by_text",
    order: Optional[str] = None,
    columns: Optional[str] = None,
):
    collection_keys = collections.split(',') if collections else SCOPE_PRESETS.get(scope, [])
    paths_to_search = []
    for key in collection_keys:
        path = SOURCE_PATHS.get(key.strip())
        if path:
            if isinstance(path, list): paths_to_search.extend(path)
            else: paths_to_search.append(path)

    if not paths_to_search: 
        return DataTablesResponse(draw=draw, recordsTotal=0, recordsFiltered=0, data=[])

    search_pattern = q
    exclude_pattern = None
    if mode in ['definitions', 'comparisons']:
        mod_keyword = re.sub(r'([aiīoā]|aṁ)$', '', q)
        search_pattern = SEARCH_PATTERNS[mode].format(keyword=re.escape(mod_keyword))
        if mode == 'comparisons': exclude_pattern = EXCLUSION_PATTERNS['comparisons']

    rg_results = run_ripgrep(search_pattern, list(set(paths_to_search)), exclude_pattern=exclude_pattern)
    
    all_results_list = []
    if mode == 'by_word':
        word_results = {}
        word_pattern = re.compile(f"\\w*{re.escape(q)}\\w*", re.IGNORECASE)
        for item in rg_results:
            if item['type'] == 'match':
                data = item['data']
                sutta_id = get_sutta_id_from_path(data['path']['text'])
                for word in word_pattern.findall(data['lines']['text']):
                    word_lower = word.lower()
                    if word_lower not in word_results:
                        word_results[word_lower] = {"word": word_lower, "total_count": 0, "suttas": set()}
                    word_results[word_lower]["total_count"] += 1
                    word_results[word_lower]["suttas"].add(sutta_id)
        all_results_list = sorted(list(word_results.values()), key=lambda x: x['total_count'], reverse=True)
    else:
        grouped_results = {}
        for item in rg_results:
            if item['type'] == 'match':
                data = item['data']
                sutta_id = get_sutta_id_from_path(data['path']['text'])
                if sutta_id not in grouped_results:
                    info = get_text_info(sutta_id)
                    grouped_results[sutta_id] = {**info, "sutta_id": sutta_id, "match_count": 0, "quotes": []}
                line_num = data.get('line_number')
                anchor = f"#{line_num}" if line_num else ""
                grouped_results[sutta_id]['match_count'] += 1
                grouped_results[sutta_id]['quotes'].append({"text": data['lines']['text'].strip(), "link": f"/read/{sutta_id}{anchor}"})
        all_results_list = list(grouped_results.values())

    if order and columns:
        try:
            order_info = json.loads(order)[0]
            col_index = order_info['column']
            col_name = json.loads(columns)[col_index]['data']
            direction = order_info['dir']
            all_results_list.sort(key=lambda x: x.get(col_name, 0), reverse=(direction == 'desc'))
        except (json.JSONDecodeError, IndexError, KeyError):
            pass

    if mode in ['top_5', 'top_10']:
        if not order: all_results_list.sort(key=lambda x: x.get('match_count', 0), reverse=True)
        limit = 5 if mode == 'top_5' else 10
        all_results_list = all_results_list[:limit]

    total_records = len(all_results_list)
    paginated_data = all_results_list[start : start + length]

    response_data = []
    if mode == 'by_word':
        for res in paginated_data:
            response_data.append({
                "word": res['word'],
                "total_count": res['total_count'],
                "sutta_count": len(res['suttas']),
                "suttas_preview": ", ".join(list(res['suttas'])[:5])
            })
    else:
        for res in paginated_data:
            quotes_text_list = [q["text"] for q in res['quotes'][:5]]
            links_list = [f'<a href="{q['link']}">{q['link']}</a>' for q in res['quotes'][:5]]

            response_data.append({
                "sutta_id": res['sutta_id'],
                "title": res.get('title_pi', res.get('sutta_id')),
                "metaphor_count": res.get('metaphor_count', 0),
                "count": res.get('match_count', 0),
                "quotes_text": "<br>".join(quotes_text_list),
                "links": "<br>".join(links_list)
            })

    return DataTablesResponse(draw=draw, recordsTotal=total_records, recordsFiltered=total_records, data=response_data)

