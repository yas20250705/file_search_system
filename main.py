from fastapi import FastAPI, Request, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional, List
import sqlite3
import re
import os
import threading
import time
import logging
from datetime import datetime, timedelta

# --- ロギング設定 ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)

# --- データベースの初期化 ---
from database import (
    initialize_meta_database, get_meta_db_connection, get_index_db_connection, create_index_tables,
    add_index_config, get_all_index_configs, get_index_config_by_id, delete_index_config, update_index_status,
    get_setting, set_setting, add_directory_to_history, get_directory_history,
    update_indexing_status, get_indexing_status, set_indexing_stop_requested, is_indexing_stop_requested,
    INDEXES_DIR
)

logger.info("アプリケーションの起動プロセスを開始します。")
try:
    # インデックスディレクトリが存在しない場合は作成
    if not os.path.exists(INDEXES_DIR):
        os.makedirs(INDEXES_DIR)
        logger.info(f"ディレクトリ '{INDEXES_DIR}' を作成しました。")

    initialize_meta_database() # メタデータベースの初期化
    logger.info("データベースの初期化が正常に完了しました。")
except Exception as e:
    logger.critical(f"データベースの初期化中に致命的なエラーが発生しました: {e}", exc_info=True)
    exit(1)

# --- FastAPIアプリケーションのセットアップ --- 
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # 起動時にすべてのインデックスのステータスをリセット
    # これは、以前の実行が予期せず終了した場合に 'running' ステータスが残るのを防ぐため
    indexes = get_all_index_configs()
    for index in indexes:
        conn = None
        try:
            conn = get_index_db_connection(index['db_path'])
            create_index_tables(index['db_path'])
        except Exception as e:
            logger.error(f"インデックスID {index['id']} の個別DBテーブル作成中にエラー: {e}")
            continue # 次のインデックスへ

        if index['status'] == 'running':
            update_index_status(index['id'], 'stopped')
        # 個別DBのindexing_statusもリセット
        try:
            conn = get_index_db_connection(index['db_path'])
            update_indexing_status(conn, index['db_path'], "not_started")
            set_indexing_stop_requested(conn, index['db_path'], False)
        except Exception as e:
            logger.warning(f"インデックスID {index['id']} の個別DBステータスリセット中にエラー: {e}")
        finally:
            if conn:
                conn.close()
    logger.info("FastAPIアプリケーションの起動イベントが完了しました。")

COMMON_EXTENSIONS = [
    # テキスト系
    ".txt", ".md", ".csv", ".json", ".xml",
    # ドキュメント系
    ".pdf", ".xlsx", ".docx", ".pptx"
]

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- 検索クエリパーサー ---
def parse_search_query(query: str) -> str:
    """
    検索クエリをパースしてFTS5クエリ文字列に変換します。
    
    サポートする機能:
    - AND検索（デフォルト）: スペース区切りで全ての語を含む
    - OR検索: 'OR' または '|' 演算子
    - 除外ワード: '-' プレフィックス
    - フレーズ検索: '"..."' で囲む
    - 厳密フレーズ検索: '""...""' で囲む
    
    例:
        'python tutorial' -> 'python tutorial' (AND検索)
        'python OR tutorial' -> 'python OR tutorial'
        'python | tutorial' -> 'python OR tutorial'
        'python -tutorial' -> 'python NOT tutorial'
        '"machine learning"' -> '"machine learning"'
        厳密フレーズ検索: 二重引用符2つで囲むと厳密フレーズ検索になります
    """
    if not query or not query.strip():
        return ""
    
    query = query.strip()
    tokens = []
    i = 0
    length = len(query)
    
    # 空白文字（半角スペース、タブ、全角スペース）を定義
    whitespace_chars = ' \t　'  # 半角スペース、タブ、全角スペース
    
    while i < length:
        # 空白をスキップ（全角スペースも含む）
        if query[i] in whitespace_chars:
            i += 1
            continue
        
        # 厳密フレーズ検索: ""...""
        if i + 1 < length and query[i:i+2] == '""':
            end_pos = query.find('""', i + 2)
            if end_pos != -1:
                phrase = query[i+2:end_pos].strip()
                if phrase:
                    tokens.append(f'"""{phrase}"""')
                i = end_pos + 2
                continue
        
        # フレーズ検索: "..."
        if query[i] == '"':
            end_pos = query.find('"', i + 1)
            if end_pos != -1:
                phrase = query[i+1:end_pos].strip()
                if phrase:
                    tokens.append(f'"{phrase}"')
                i = end_pos + 1
                continue
        
        # OR演算子
        if i + 1 < length and query[i:i+2].upper() == 'OR' and \
           (i == 0 or query[i-1] in whitespace_chars + '(') and \
           (i + 2 >= length or query[i+2] in whitespace_chars + ')'):
            tokens.append('OR')
            i += 2
            continue
        
        # | 演算子 (ORとして扱う)
        if query[i] == '|':
            tokens.append('OR')
            i += 1
            continue
        
        # 除外ワード: -word（全角スペースも考慮）
        if query[i] == '-' and (i == 0 or query[i-1] in whitespace_chars + '('):
            i += 1
            word_start = i
            # 次の語まで読み取る（全角スペースも考慮）
            while i < length and query[i] not in whitespace_chars + '|()':
                if query[i] == '"':
                    break
                i += 1
            word = query[word_start:i].strip()
            if word:
                tokens.append(f'NOT {word}')
            continue
        
        # 通常の語を読み取る（全角スペースも考慮）
        word_start = i
        while i < length and query[i] not in whitespace_chars + '|()':
            if query[i] in '"-':
                break
            i += 1
        word = query[word_start:i].strip()
        if word and word.upper() != 'OR':
            tokens.append(word)
    
    # トークンを結合してFTS5クエリを構築
    if not tokens:
        return ""
    
    # トークンを処理してFTS5クエリを構築
    logger.debug(f"Parsed tokens: {tokens}")
    fts_parts = []
    i = 0
    prev_was_operator = False  # 前のトークンが演算子かどうか
    
    while i < len(tokens):
        token = tokens[i]
        
        # OR演算子
        if token.upper() == 'OR':
            fts_parts.append('OR')
            prev_was_operator = True
            i += 1
            continue
        
        # NOT演算子（除外対象として処理）
        if token.upper().startswith('NOT '):
            not_word = token[4:].strip()
            if not_word:
                # trigramトークナイザーでは部分一致が自動的にサポートされるため
                # ワイルドカードは不要
                fts_parts.append(f'NOT {not_word}')
            prev_was_operator = False
            i += 1
            continue
        
        # フレーズ検索（既に引用符で囲まれている）
        if token.startswith('"') and not token.startswith('"""'):
            fts_parts.append(token)
            prev_was_operator = False
            i += 1
            continue
        
        # 厳密フレーズ検索（既に三重引用符で囲まれている）
        if token.startswith('"""') and token.endswith('"""'):
            fts_parts.append(token)
            prev_was_operator = False
            i += 1
            continue
        
        # 通常の語
        if token:
            # trigramトークナイザーでは部分一致が自動的にサポートされるため
            # ワイルドカードは不要。語をそのまま使用する。
            # これにより「日立」で「株式会社日立」「日立製作所」両方にマッチ
            fts_parts.append(token)
            prev_was_operator = False
        i += 1
    
    # トークンを結合（スペース区切りは自動的にANDとして扱われる）
    fts_query = ' '.join(fts_parts)
    
    # 連続する空白を1つに
    fts_query = re.sub(r'\s+', ' ', fts_query).strip()
    
    logger.debug(f"Parsed query '{query}' -> FTS5 query '{fts_query}'")
    return fts_query

# --- 日付フィルター処理 ---
def get_date_range(filter_type: str):
    """
    日付フィルターから日付範囲を取得します。
    
    Args:
        filter_type: フィルタータイプ（today, this_week, this_month, this_year, year:YYYY）
    
    Returns:
        tuple: (start_timestamp, end_timestamp) または (None, None)
    """
    if not filter_type:
        return None, None
    
    now = datetime.now()
    
    if filter_type == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start.timestamp(), now.timestamp()
    elif filter_type == "this_week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return start.timestamp(), now.timestamp()
    elif filter_type == "this_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return start.timestamp(), now.timestamp()
    elif filter_type == "this_year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start.timestamp(), now.timestamp()
    elif filter_type.startswith("year:"):
        try:
            year = int(filter_type.split(":")[1])
            start = datetime(year, 1, 1)
            end = datetime(year, 12, 31, 23, 59, 59)
            return start.timestamp(), end.timestamp()
        except (ValueError, IndexError):
            logger.warning(f"無効な年指定: {filter_type}")
            return None, None
    
    return None, None

# --- ルート定義 ---

@app.get("/settings", response_class=HTMLResponse)
async def show_settings(request: Request, message: str = Query(None)):
    indexes = get_all_index_configs()
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "indexes": indexes,
        "common_extensions": COMMON_EXTENSIONS,
        "message": message
    })

@app.post("/add_index", response_class=RedirectResponse)
async def add_index(request: Request, name: str = Form(...), target_directory: str = Form(...), selected_extensions: list[str] = Form(None, alias="allowed_extensions")):
    allowed_extensions_str = ",".join(selected_extensions) if selected_extensions else ""
    try:
        index_id = add_index_config(name, target_directory, allowed_extensions_str)
        if index_id == -1:
            return RedirectResponse(url="/settings?message=Error: Index name already exists!", status_code=303)
        return RedirectResponse(url="/settings?message=Index added successfully!", status_code=303)
    except Exception as e:
        logger.error(f"インデックスの追加中にエラーが発生しました: {e}", exc_info=True)
        return RedirectResponse(url=f"/settings?message=Error adding index: {e}", status_code=303)

@app.post("/delete_index/{index_id}", response_class=RedirectResponse)
async def delete_index(index_id: int):
    try:
        if delete_index_config(index_id):
            return RedirectResponse(url="/settings?message=Index deleted successfully!", status_code=303)
        else:
            return RedirectResponse(url="/settings?message=Error: Index not found!", status_code=303)
    except Exception as e:
        logger.error(f"インデックスの削除中にエラーが発生しました: {e}", exc_info=True)
        return RedirectResponse(url=f"/settings?message=Error deleting index: {e}", status_code=303)

@app.get("/trigger_index_for_id/{index_id}")
async def trigger_index_for_id(index_id: int):
    from indexer import index_files # 遅延インポート
    index_config = get_index_config_by_id(index_id)
    if not index_config:
        return RedirectResponse(url="/settings?message=Error: Index not found!", status_code=303)
    
    target_directory = index_config['target_directory']
    allowed_extensions_str = index_config['allowed_extensions']
    allowed_extensions = [ext.strip() for ext in allowed_extensions_str.split(',') if ext.strip()]
    db_path = index_config['db_path']

    if not target_directory:
        return RedirectResponse(url="/settings?message=Error: Target directory not set for this index!", status_code=303)
    
    conn = None
    try:
        conn = get_index_db_connection(db_path)
        set_indexing_stop_requested(conn, db_path, False)
        update_index_status(index_id, 'running') # メタDBのステータスを更新
        threading.Thread(target=index_files, args=(index_id, target_directory, allowed_extensions, db_path)).start()
        update_indexing_status(conn, db_path, "started", 0, 0, time.time(), 0) # 個別DBのステータスを更新
    finally:
        if conn:
            conn.close()

    return RedirectResponse(url=f"/settings?message=Indexing for '{index_config['name']}' started in background!", status_code=303)

@app.get("/stop_indexing_for_id/{index_id}")
async def stop_indexing_for_id(index_id: int):
    index_config = get_index_config_by_id(index_id)
    if not index_config:
        return RedirectResponse(url="/settings?message=Error: Index not found!", status_code=303)
    
    db_path = index_config['db_path']
    conn = None
    try:
        conn = get_index_db_connection(db_path)
        set_indexing_stop_requested(conn, db_path, True)
        update_index_status(index_id, 'stopping') # メタDBのステータスを更新
    finally:
        if conn:
            conn.close()
    return RedirectResponse(url=f"/settings?message=Indexing stop requested for '{index_config['name']}'!", status_code=303)

@app.get("/indexing_status_for_id/{index_id}")
async def get_indexing_status_for_id(index_id: int):
    index_config = get_index_config_by_id(index_id)
    if not index_config:
        return JSONResponse({"status": "error", "message": "Index not found"}, status_code=404)

    db_path = index_config['db_path']
    conn = None
    try:
        conn = get_index_db_connection(db_path)
        status = get_indexing_status(conn, db_path)
        stop_requested = is_indexing_stop_requested(conn, db_path)
    finally:
        if conn:
            conn.close()
    
    if status:
        current_time = time.time()
        elapsed_time = current_time - status['start_time'] if status['start_time'] else 0
        remaining_time = status['estimated_end_time'] - current_time if status['estimated_end_time'] else 0
        
        # メタDBのステータスも考慮
        meta_status = index_config['status']

        return JSONResponse({
            "status": status['status'],
            "meta_status": meta_status, # メタDBのステータスも返す
            "total_files": status['total_files'],
            "processed_files": status['processed_files'],
            "elapsed_time": round(elapsed_time, 2),
            "remaining_time": round(remaining_time, 2) if remaining_time > 0 else 0,
            "stop_requested": stop_requested
        })
    return JSONResponse({"status": "not_started", "meta_status": index_config['status'], "stop_requested": stop_requested})

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    indexes = get_all_index_configs()
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "results": [], 
        "indexes": indexes, 
        "selected_index_id": None,
        "show_advanced": None,
        "common_extensions": COMMON_EXTENSIONS
    })

@app.get("/search", response_class=HTMLResponse)
async def search_files(
    request: Request, 
    q: str = Query(None), 
    index_id: Optional[str] = Query(None),
    file_type: List[str] = Query(None),
    modified_date_filter: str = Query(None),
    created_date_filter: str = Query(None),
    modified_date_filter_year: str = Query(None),
    created_date_filter_year: str = Query(None),
    modified_date_filter_select: str = Query(None),
    created_date_filter_select: str = Query(None),
    show_advanced: str = Query(None)
):
    results = []
    indexes = get_all_index_configs()
    selected_index_config = None
    
    # index_idが空文字列の場合はNoneに変換
    if index_id == "":
        index_id = None
    elif index_id:
        try:
            index_id = int(index_id)
        except (ValueError, TypeError):
            index_id = None
    
    # セレクトボックスからの値を処理
    if modified_date_filter_select and modified_date_filter_select != 'year:custom':
        modified_date_filter = modified_date_filter_select
    if created_date_filter_select and created_date_filter_select != 'year:custom':
        created_date_filter = created_date_filter_select
    
    # 年指定の処理
    if modified_date_filter_year:
        modified_date_filter = f"year:{modified_date_filter_year}"
    if created_date_filter_year:
        created_date_filter = f"year:{created_date_filter_year}"
    
    # file_typeをリスト形式に正規化（Noneの場合は空リスト、文字列の場合はリストに変換）
    if file_type is None:
        file_type_list = []
    elif isinstance(file_type, str):
        file_type_list = [file_type] if file_type else []
    else:
        file_type_list = [ft for ft in file_type if ft] if file_type else []
    
    # フィルターが設定されている場合は自動的に詳細検索を表示
    if show_advanced is None and (file_type_list or modified_date_filter or created_date_filter):
        show_advanced = "1"
    
    logger.debug(f"Search filters - file_type: {file_type_list}, modified: {modified_date_filter}, created: {created_date_filter}, show_advanced: {show_advanced}")
    
    # 詳細検索パネルが開いている状態で、ファイル種別が選択されていない場合はエラー
    if show_advanced and not file_type_list:
        # 詳細検索パネルが開いているが、ファイル種別が選択されていない場合
        if index_id:
            selected_index_config = get_index_config_by_id(index_id)
            if not selected_index_config:
                return templates.TemplateResponse("index.html", {
                    "request": request, 
                    "results": [], 
                    "indexes": indexes, 
                    "selected_index_id": None, 
                    "query": q,
                    "file_type": file_type_list,
                    "modified_date_filter": modified_date_filter,
                    "created_date_filter": created_date_filter,
                    "show_advanced": show_advanced,
                    "common_extensions": COMMON_EXTENSIONS,
                    "message": "Error: Selected index not found!"
                })
        else:
            selected_index_config = None
        
        return templates.TemplateResponse("index.html", {
            "request": request,
            "results": [],
            "indexes": indexes,
            "selected_index_id": index_id,
            "query": q,
            "file_type": file_type_list,
            "modified_date_filter": modified_date_filter,
            "created_date_filter": created_date_filter,
            "show_advanced": show_advanced,
            "common_extensions": COMMON_EXTENSIONS,
            "message": "ファイル種別を選択してください。"
        })
    
    if index_id:
        selected_index_config = get_index_config_by_id(index_id)
        if not selected_index_config:
            return templates.TemplateResponse("index.html", {
                "request": request, 
                "results": [], 
                "indexes": indexes, 
                "selected_index_id": None, 
                "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": "Error: Selected index not found!"
            })
    else:
        selected_index_config = None

    if q and selected_index_config:
        db_path = selected_index_config['db_path']
        conn = get_index_db_connection(db_path)
        try:
            # 検索クエリをパースしてFTS5クエリに変換
            fts_query = parse_search_query(q)
            if not fts_query:
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": "検索クエリが空です。有効なキーワードを入力してください。"
                })
            
            logger.debug(f"Original query: '{q}'")
            logger.debug(f"Parsed FTS5 query: '{fts_query}'")
            
            # クエリが空でないことを確認
            if not fts_query or not fts_query.strip():
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": "検索クエリが空です。有効なキーワードを入力してください。"
                })
            
            # フィルター条件を構築
            filter_conditions = []
            filter_params = []
            
            # ファイル種別フィルター（複数選択対応）
            if file_type_list:
                # 空文字列を除外
                file_types = [ft.lower() for ft in file_type_list if ft and ft.strip()]
                if file_types:
                    placeholders = ','.join(['?' for _ in file_types])
                    filter_conditions.append(f"files.file_type IN ({placeholders})")
                    filter_params.extend(file_types)
                    logger.debug(f"File type filter: {file_types}")
            
            # 変更日時フィルター
            if modified_date_filter:
                start_ts, end_ts = get_date_range(modified_date_filter)
                if start_ts is not None and end_ts is not None:
                    filter_conditions.append("files.modified_date IS NOT NULL AND files.modified_date >= ? AND files.modified_date <= ?")
                    filter_params.extend([start_ts, end_ts])
                    logger.debug(f"Modified date filter: {modified_date_filter} -> {start_ts} to {end_ts}")
            
            # 作成日時フィルター
            if created_date_filter:
                start_ts, end_ts = get_date_range(created_date_filter)
                if start_ts is not None and end_ts is not None:
                    filter_conditions.append("files.created_date IS NOT NULL AND files.created_date >= ? AND files.created_date <= ?")
                    filter_params.extend([start_ts, end_ts])
                    logger.debug(f"Created date filter: {created_date_filter} -> {start_ts} to {end_ts}")
            
            # 検索語が2文字以下かどうかを判定（trigramは3文字以上が必要）
            # 空白や演算子を除いた実際の検索語の長さをチェック
            search_terms = [term for term in q.strip().split() if term.upper() not in ['OR', 'AND'] and not term.startswith('-')]
            use_like_search = any(len(term.strip('"')) <= 2 for term in search_terms)
            
            if use_like_search:
                # 2文字以下の検索語が含まれる場合はLIKE検索を使用
                logger.debug(f"Using LIKE search for short query: '{q}'")
                like_conditions = []
                like_params = []
                for term in search_terms:
                    clean_term = term.strip('"')
                    like_conditions.append("files.content LIKE ?")
                    like_params.append(f"%{clean_term}%")
                
                # すべての条件を結合
                all_conditions = like_conditions + filter_conditions
                where_clause = " AND ".join(all_conditions) if all_conditions else "1=1"
                all_params = like_params + filter_params
                
                logger.debug(f"LIKE search WHERE clause: {where_clause}")
                logger.debug(f"LIKE search params: {all_params}")
                
                cursor = conn.execute(f"""
                    SELECT 
                        files.path,
                        files.modified_date,
                        files.created_date,
                        substr(files.content, 1, 200) as snippet
                    FROM files
                    WHERE {where_clause}
                """, all_params)
            else:
                # 3文字以上の場合はtrigram FTS5検索を使用
                logger.debug(f"Using FTS5 trigram search for query: '{fts_query}'")
                
                # FTS5検索とfilesテーブルをJOINしてフィルターを適用
                # content-syncを使用しない独立したテーブルなので、pathでJOINする
                fts_join = "INNER JOIN files ON files_fts.path = files.path"
                fts_where = "files_fts MATCH ?"
                fts_params = [fts_query]
                
                # フィルター条件がある場合はWHERE句に追加
                if filter_conditions:
                    fts_where += " AND " + " AND ".join(filter_conditions)
                    fts_params.extend(filter_params)
                    logger.debug(f"FTS5 search with filters - WHERE: {fts_where}, JOIN: {fts_join}")
                    logger.debug(f"FTS5 search params: {fts_params}")
                else:
                    logger.debug(f"FTS5 search without filters - WHERE: {fts_where}")
                
                cursor = conn.execute(f"""
                    SELECT 
                        files.path,
                        files.modified_date,
                        files.created_date,
                        snippet(files_fts, 1, '<b>', '</b>', '...', 100) as snippet
                    FROM files_fts
                    {fts_join}
                    WHERE {fts_where}
                    ORDER BY rank
                """, fts_params)
            
            fetched_rows = cursor.fetchall()
            for row in fetched_rows:
                # スニペットを200文字に制限
                snippet_text = row["snippet"] if row["snippet"] else ""
                if len(snippet_text) > 200:
                    snippet_text = snippet_text[:200] + "..."
                
                # タイムスタンプを人間が読める形式に変換
                def format_timestamp(ts):
                    if ts is None:
                        return ""
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
                    except (OSError, OverflowError, ValueError):
                        return ""

                modified_str = format_timestamp(row["modified_date"])
                created_str = format_timestamp(row["created_date"])

                results.append({
                    "path": row["path"],
                    "modified_date": modified_str,
                    "created_date": created_str,
                    "snippets": [{"text": snippet_text}],
                })
        except sqlite3.OperationalError as e:
            logger.error(f"Search query failed on {db_path}: {e}", exc_info=True)
            error_msg = str(e)
            # FTSテーブルが存在しない場合のエラーハンドリング
            if "no such table: files_fts" in error_msg:
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": "エラー: インデックスデータベースが初期化されていないか、破損しています。再インデックスを作成してください。"
                })
            # 構文エラーの場合
            elif "malformed" in error_msg.lower() or "syntax" in error_msg.lower():
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": f"検索クエリの構文エラー: クエリを確認してください。例: 'python tutorial', 'python OR tutorial', 'python -tutorial'"
                })
            else:
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": f"検索中にエラーが発生しました: {error_msg}"
                })
        except Exception as e:
            logger.error(f"Unexpected error during search on {db_path}: {e}", exc_info=True)
            return templates.TemplateResponse("index.html", {
                "request": request,
                "results": [],
                "indexes": indexes,
                "selected_index_id": index_id,
                "query": q,
                "file_type": file_type_list,
                "modified_date_filter": modified_date_filter,
                "created_date_filter": created_date_filter,
                "show_advanced": show_advanced,
                "common_extensions": COMMON_EXTENSIONS,
                "message": f"予期しないエラーが発生しました: {str(e)}"
            })
        finally:
            conn.close()
    elif q and not selected_index_config:
        return templates.TemplateResponse("index.html", {
            "request": request, 
            "results": [], 
            "indexes": indexes, 
            "selected_index_id": None, 
            "query": q,
            "file_type": file_type_list,
            "modified_date_filter": modified_date_filter,
            "created_date_filter": created_date_filter,
            "show_advanced": show_advanced,
            "message": "Please select an index to search."
        })

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "results": results, 
        "query": q, 
        "indexes": indexes, 
        "selected_index_id": index_id,
        "file_type": file_type_list,
        "modified_date_filter": modified_date_filter,
        "created_date_filter": created_date_filter,
        "show_advanced": show_advanced,
        "common_extensions": COMMON_EXTENSIONS
    })

@app.post("/export")
async def export_documents(
    index_id: int = Form(...),
    q: str = Form(...),
    max_chars: int = Form(100000),
    selected_paths: str = Form(...),
    file_type: str = Form(None),
    modified_date_filter: str = Form(None),
    created_date_filter: str = Form(None),
    modified_date_filter_year: str = Form(None),
    created_date_filter_year: str = Form(None),
    modified_date_filter_select: str = Form(None),
    created_date_filter_select: str = Form(None)
):
    """
    選択されたドキュメントをMarkdown形式でエクスポートします。
    文字数制限を超える場合はZIPファイルで分割出力します。
    """
    from fastapi.responses import PlainTextResponse, Response
    import json
    import zipfile
    import io
    
    index_config = get_index_config_by_id(index_id)
    if not index_config:
        return PlainTextResponse("Error: Index not found", status_code=404)
    
    # 選択されたパスをパース
    try:
        paths = json.loads(selected_paths)
        if not paths:
            return PlainTextResponse("Error: No documents selected", status_code=400)
    except json.JSONDecodeError:
        return PlainTextResponse("Error: Invalid selected_paths format", status_code=400)
    
    db_path = index_config['db_path']
    conn = get_index_db_connection(db_path)
    
    try:
        # 選択されたドキュメントを取得
        placeholders = ','.join(['?' for _ in paths])
        cursor = conn.execute(f"""
            SELECT path, content, modified_date, created_date, file_type
            FROM files
            WHERE path IN ({placeholders})
        """, paths)
        
        rows = cursor.fetchall()
        
        def format_timestamp(ts):
            if ts is None:
                return "不明"
            try:
                return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
            except (OSError, OverflowError, ValueError):
                return "不明"
        
        def create_document_markdown(doc_num, row, total_docs):
            """1つのドキュメントのMarkdownを生成"""
            path = row['path']
            content = row['content'] or ""
            modified_date = format_timestamp(row['modified_date'])
            created_date = format_timestamp(row['created_date'])
            file_type_val = row['file_type'] or "不明"
            filename = os.path.basename(path)
            
            lines = []
            lines.append(f"## ドキュメント {doc_num}/{total_docs}: {filename}")
            lines.append(f"")
            lines.append(f"- **ファイルパス:** {path}")
            lines.append(f"- **ファイル種別:** {file_type_val}")
            lines.append(f"- **作成日時:** {created_date}")
            lines.append(f"- **変更日時:** {modified_date}")
            lines.append(f"")
            lines.append(f"### 本文")
            lines.append(f"")
            lines.append(f"```")
            lines.append(content)
            lines.append(f"```")
            lines.append(f"")
            lines.append(f"---")
            lines.append(f"")
            return "\n".join(lines)
        
        def create_header(part_num=None, total_parts=None):
            """ヘッダーを生成"""
            lines = []
            lines.append(f"# 検索結果エクスポート")
            if part_num and total_parts:
                lines.append(f"## パート {part_num}/{total_parts}")
            lines.append(f"")
            lines.append(f"**検索クエリ:** {q}")
            lines.append(f"**検索対象インデックス:** {index_config['name']}")
            lines.append(f"**選択ドキュメント数:** {len(rows)}件")
            lines.append(f"**最大文字数設定:** {max_chars:,}文字/ファイル")
            lines.append(f"")
            lines.append(f"---")
            lines.append(f"")
            return "\n".join(lines)
        
        # ドキュメントを文字数制限に基づいて分割
        all_docs = []
        total_docs = len(rows)
        for i, row in enumerate(rows, 1):
            doc_md = create_document_markdown(i, row, total_docs)
            all_docs.append((row['path'], doc_md))
        
        # ファイルに分割
        files_content = []
        current_content = []
        current_chars = 0
        header_chars = len(create_header(1, 1))
        
        for path, doc_md in all_docs:
            doc_chars = len(doc_md)
            
            # 現在のファイルに追加できるか確認
            if current_chars + doc_chars + header_chars > max_chars and current_content:
                # 現在のファイルを保存し、新しいファイルを開始
                files_content.append(current_content)
                current_content = [(path, doc_md)]
                current_chars = doc_chars
            else:
                current_content.append((path, doc_md))
                current_chars += doc_chars
        
        # 最後のファイルを追加
        if current_content:
            files_content.append(current_content)
        
        # ファイル名のベースを生成（ASCIIのみに制限）
        def sanitize_filename(text):
            # 非ASCII文字を削除し、特殊文字を置換
            ascii_only = ''.join(c if ord(c) < 128 else '_' for c in text)
            safe = re.sub(r'[\\/:*?"<>|]', '_', ascii_only)
            return safe[:30] or 'export'
        
        safe_query = sanitize_filename(q)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if len(files_content) == 1:
            # 単一ファイルの場合
            header = create_header()
            body = "\n".join([doc_md for _, doc_md in files_content[0]])
            markdown_text = header + body
            
            filename = f"export_{safe_query}_{timestamp}.md"
            
            return Response(
                content=markdown_text.encode('utf-8'),
                media_type="text/markdown; charset=utf-8",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}"
                }
            )
        else:
            # 複数ファイルの場合はZIPで出力
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                total_parts = len(files_content)
                for part_num, docs in enumerate(files_content, 1):
                    header = create_header(part_num, total_parts)
                    body = "\n".join([doc_md for _, doc_md in docs])
                    markdown_text = header + body
                    
                    part_filename = f"export_{safe_query}_{timestamp}_part{part_num:02d}.md"
                    zip_file.writestr(part_filename, markdown_text.encode('utf-8'))
            
            zip_buffer.seek(0)
            zip_filename = f"export_{safe_query}_{timestamp}.zip"
            
            return Response(
                content=zip_buffer.getvalue(),
                media_type="application/zip",
                headers={
                    "Content-Disposition": f"attachment; filename={zip_filename}"
                }
            )
        
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)
    finally:
        conn.close()

@app.get("/open")
async def open_file(path: str):
    if not os.path.exists(path):
        return {"error": "File not found"}
    try:
        os.startfile(path)
        return {"status": "success", "path": path}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
