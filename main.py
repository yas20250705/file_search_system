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

# --- ロギング設定 ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- データベースの初期化 ---
# アプリケーション起動時に、必ずデータベースを最新のスキーマで再作成する
from database import create_tables, get_db_connection, set_setting, get_setting, add_directory_to_history, get_directory_history, update_indexing_status, get_indexing_status, set_indexing_stop_requested, is_indexing_stop_requested

logger.info("アプリケーションの起動プロセスを開始します。")
try:
    create_tables() # ここでデータベースが確実に再作成される
    logger.info("データベースの初期化が正常に完了しました。")
except Exception as e:
    logger.critical(f"データベースの初期化中に致命的なエラーが発生しました: {e}", exc_info=True)
    # アプリケーションを続行できないため、ここで終了する
    exit(1)

# --- FastAPIアプリケーションのセットアップ ---
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # ステータスをリセット
    update_indexing_status("not_started")
    set_indexing_stop_requested(False)
    logger.info("FastAPIアプリケーションの起動イベントが完了しました。")

COMMON_EXTENSIONS = [
    ".txt", ".md", ".py", ".html", ".css", ".js", ".json", ".xml", ".csv",
    ".c", ".cpp", ".h", ".java", ".go", ".php", ".rb", ".ts", ".sh", ".bat",
    ".pdf", ".xlsx", ".docx", ".pptx", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg"
]

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- ルート定義 (ここから下は変更なし) ---

@app.get("/settings", response_class=HTMLResponse)
async def show_settings(request: Request):
    target_directory = get_setting('TARGET_DIRECTORY')
    allowed_extensions_str = get_setting('ALLOWED_EXTENSIONS')
    allowed_extensions_list = [ext.strip() for ext in allowed_extensions_str.split(',') if ext.strip()] if allowed_extensions_str else []
    directory_history = get_directory_history()
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "target_directory": target_directory,
        "common_extensions": COMMON_EXTENSIONS,
        "allowed_extensions": allowed_extensions_list,
        "directory_history": directory_history
    })

@app.post("/settings", response_class=HTMLResponse)
async def update_settings(request: Request, target_directory: str = Form(...), selected_extensions: list[str] = Form(None, alias="allowed_extensions")):
    set_setting('TARGET_DIRECTORY', target_directory)
    add_directory_to_history(target_directory)
    allowed_extensions_to_save = ",".join(selected_extensions) if selected_extensions else ""
    set_setting('ALLOWED_EXTENSIONS', allowed_extensions_to_save)
    return RedirectResponse(url="/settings?message=Settings updated successfully!", status_code=303)

@app.get("/index_now")
async def trigger_index():
    from indexer import index_files # 遅延インポート
    target_directory = get_setting('TARGET_DIRECTORY')
    allowed_extensions_str = get_setting('ALLOWED_EXTENSIONS')
    allowed_extensions = [ext.strip() for ext in allowed_extensions_str.split(',') if ext.strip()]
    if not target_directory:
        return RedirectResponse(url="/settings?message=Error: TARGET_DIRECTORY is not set!", status_code=303)
    set_indexing_stop_requested(False)
    threading.Thread(target=index_files, args=(target_directory, allowed_extensions)).start()
    update_indexing_status("started", 0, 0, time.time(), 0)
    return RedirectResponse(url="/settings?message=Indexing started in background!", status_code=303)

@app.get("/stop_indexing")
async def stop_indexing():
    set_indexing_stop_requested(True)
    return RedirectResponse(url="/settings?message=Indexing stop requested!", status_code=303)

@app.get("/indexing_status")
async def get_indexing_status_api():
    status = get_indexing_status()
    stop_requested = is_indexing_stop_requested()
    if status:
        current_time = time.time()
        elapsed_time = current_time - status['start_time'] if status['start_time'] else 0
        remaining_time = status['estimated_end_time'] - current_time if status['estimated_end_time'] else 0
        return JSONResponse({
            "status": status['status'],
            "total_files": status['total_files'],
            "processed_files": status['processed_files'],
            "elapsed_time": round(elapsed_time, 2),
            "remaining_time": round(remaining_time, 2) if remaining_time > 0 else 0,
            "stop_requested": stop_requested
        })
    return JSONResponse({"status": "not_started", "stop_requested": stop_requested})

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "results": []})

@app.get("/search", response_class=HTMLResponse)
async def search_files(request: Request, q: str = Query(None)):
    results = []
    if q:
        conn = get_db_connection()
        try:
            fts_query = ' '.join([f'"{term}"*' for term in q.split()])
            logger.debug(f"Executing FTS5 query: {fts_query}")
            cursor = conn.execute("""
                SELECT path, snippet(files_fts, 1, '<b>', '</b>', '...', 15)
                FROM files_fts
                WHERE files_fts MATCH ?
                ORDER BY rank
                LIMIT 50
            """, (fts_query,))
            fetched_rows = cursor.fetchall()
            for row in fetched_rows:
                results.append({"path": row['path'], "snippets": [{"text": row[1]}]})
        except sqlite3.OperationalError as e:
            logger.error(f"Search query failed: {e}", exc_info=True)
        finally:
            conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "results": results, "query": q})

@app.get("/open")
async def open_file(path: str):
    if not os.path.exists(path):
        return {"error": "File not found"}
    try:
        os.startfile(path)
        return {"status": "success", "path": path}
    except Exception as e:
        return {"error": str(e)}