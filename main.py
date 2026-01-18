from fastapi import FastAPI, Request, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import sqlite3
import re
import os
import threading
import time
import logging
from datetime import datetime

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
    ".txt", ".md", ".py", ".html", ".css", ".js", ".json", ".xml", ".csv",
    ".c", ".cpp", ".h", ".java", ".go", ".php", ".rb", ".ts", ".sh", ".bat",
    ".pdf", ".xlsx", ".docx", ".pptx", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg"
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
                # NOT検索では、ワイルドカード（*）を使用して部分一致を可能にする
                # これにより、「穴埋め」で「穴埋め式」も除外できる
                # FTS5では、語をそのまま使用し、*でプレフィックスマッチを行う
                fts_parts.append(f'NOT {not_word}*')
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
            # 前のトークンがOR演算子の場合は、語をそのまま使用
            # そうでない場合は、ワイルドカード付きで使用（部分一致を可能にする）
            if prev_was_operator or (i > 0 and tokens[i-1].upper() == 'OR'):
                # OR検索の場合は、語をそのまま使用
                fts_parts.append(token)
            else:
                # AND検索の場合は、ワイルドカードを付けて部分一致を可能にする
                # これにより、「穴埋め」で「穴埋め式」も検索できる
                # 除外ワードと同様の形式で一貫性を保つ
                fts_parts.append(f'{token}*')
            prev_was_operator = False
        i += 1
    
    # トークンを結合（スペース区切りは自動的にANDとして扱われる）
    fts_query = ' '.join(fts_parts)
    
    # 連続する空白を1つに
    fts_query = re.sub(r'\s+', ' ', fts_query).strip()
    
    logger.debug(f"Parsed query '{query}' -> FTS5 query '{fts_query}'")
    return fts_query

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
    return templates.TemplateResponse("index.html", {"request": request, "results": [], "indexes": indexes, "selected_index_id": None})

@app.get("/search", response_class=HTMLResponse)
async def search_files(request: Request, q: str = Query(None), index_id: int = Query(None)):
    results = []
    indexes = get_all_index_configs()
    selected_index_config = None
    
    if index_id:
        selected_index_config = get_index_config_by_id(index_id)
        if not selected_index_config:
            return templates.TemplateResponse("index.html", {"request": request, "results": [], "indexes": indexes, "selected_index_id": None, "query": q, "message": "Error: Selected index not found!"})

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
                    "message": "検索クエリが空です。有効なキーワードを入力してください。"
                })
            
            cursor = conn.execute("""
                SELECT path, snippet(files_fts, 1, '<b>', '</b>', '...', 100)
                FROM files_fts
                WHERE files_fts MATCH ?
                ORDER BY rank
                LIMIT 50
            """, (fts_query,))
            fetched_rows = cursor.fetchall()
            for row in fetched_rows:
                # スニペットを200文字に制限
                snippet_text = row[1] if row[1] else ""
                if len(snippet_text) > 200:
                    snippet_text = snippet_text[:200] + "..."
                results.append({"path": row['path'], "snippets": [{"text": snippet_text}]})
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
                    "message": f"検索クエリの構文エラー: クエリを確認してください。例: 'python tutorial', 'python OR tutorial', 'python -tutorial'"
                })
            else:
                return templates.TemplateResponse("index.html", {
                    "request": request,
                    "results": [],
                    "indexes": indexes,
                    "selected_index_id": index_id,
                    "query": q,
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
                "message": f"予期しないエラーが発生しました: {str(e)}"
            })
        finally:
            conn.close()
    elif q and not selected_index_config:
        return templates.TemplateResponse("index.html", {"request": request, "results": [], "indexes": indexes, "selected_index_id": None, "query": q, "message": "Please select an index to search."})

    return templates.TemplateResponse("index.html", {"request": request, "results": results, "query": q, "indexes": indexes, "selected_index_id": index_id})

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
