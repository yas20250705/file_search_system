
import sqlite3
import os
import threading
import logging

DATABASE_NAME = "file_index.db"
db_lock = threading.Lock()
logger = logging.getLogger(__name__)

def get_db_connection():
    """データベース接続を取得します。"""
    # WALモードではタイムアウトを長くすることが推奨される
    conn = sqlite3.connect(DATABASE_NAME, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    """
    データベースファイルとテーブルを（再）作成し、WALモードを有効にします。
    """
    with db_lock:
        if os.path.exists(DATABASE_NAME):
            os.remove(DATABASE_NAME)
            logger.info(f"既存のデータベースファイル {DATABASE_NAME} を削除しました。")

        conn = get_db_connection()
        try:
            # ジャーナルモードをWALに設定して並行処理性能を向上 (最重要)
            conn.execute("PRAGMA journal_mode = WAL;")
            logger.info("データベースのジャーナルモードをWALに設定しました。")

            logger.info("新しいデータベースとテーブルを作成します...")
            conn.execute("""
                CREATE TABLE files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path TEXT NOT NULL UNIQUE,
                    content TEXT
                )
            """)
            conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("CREATE TABLE directory_history (path TEXT PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
            conn.execute("""
                CREATE TABLE indexing_status (
                    id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    total_files INTEGER,
                    processed_files INTEGER,
                    start_time REAL,
                    estimated_end_time REAL
                );
            """)
            conn.execute("""
                CREATE VIRTUAL TABLE files_fts USING fts5(
                    path, 
                    content, 
                    content='files', 
                    content_rowid='id',
                    tokenize = 'porter unicode61',
                    detail=full
                );
            """)
            logger.info("FTS5テーブルが 'detail=full' で正常に作成されました。")
            
            # デフォルト設定
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('TARGET_DIRECTORY', r'C:\Users\yasud\OneDrive\電子書籍\研修資料'))
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('ALLOWED_EXTENSIONS', '.txt,.md,.py,.html,.css,.js,.json,.xml,.csv,.c,.cpp,.h,.java,.go,.php,.rb,.ts,.sh,.bat,.pdf,.xlsx,.docx,.pptx'))
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('indexing_stop_requested', 'False'))
            conn.execute("INSERT INTO directory_history (path) VALUES (?) ", (r'C:\Users\yasud\OneDrive\電子書籍\研修資料',))
            
            conn.commit()
            logger.info("データベーステーブルのセットアップが正常に完了しました。")
        except sqlite3.Error as e:
            logger.error(f"テーブル作成中に致命的なエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            conn.close()

# --- 以下の関数は変更なし ---

def get_setting(key: str):
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result['value'] if result else None
        finally:
            conn.close()

def set_setting(key: str, value: str):
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
        finally:
            conn.close()

def add_directory_to_history(path: str):
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO directory_history (path) VALUES (?) ", (path,))
            conn.commit()
        finally:
            conn.close()

def get_directory_history():
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT path FROM directory_history ORDER BY timestamp DESC")
            history = [row['path'] for row in cursor.fetchall()]
            return history
        finally:
            conn.close()

def update_indexing_status(status: str, total_files: int = None, processed_files: int = None, start_time: float = None, estimated_end_time: float = None):
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO indexing_status (id, status, total_files, processed_files, start_time, estimated_end_time) VALUES (?, ?, ?, ?, ?, ?)",
                         (1, status, total_files, processed_files, start_time, estimated_end_time))
            conn.commit()
        finally:
            conn.close()

def get_indexing_status():
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT status, total_files, processed_files, start_time, estimated_end_time FROM indexing_status WHERE id = 1")
            status = cursor.fetchone()
            return status
        finally:
            conn.close()

def set_indexing_stop_requested(requested: bool):
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ('indexing_stop_requested', str(requested)))
            conn.commit()
        finally:
            conn.close()

def is_indexing_stop_requested():
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.execute("SELECT value FROM settings WHERE key = ?", ('indexing_stop_requested',))
            result = cursor.fetchone()
            return result['value'] == 'True' if result else False
        finally:
            conn.close()
