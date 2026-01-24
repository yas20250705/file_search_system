import sqlite3
import os
import threading
import logging
from datetime import datetime

# メタデータベースのファイル名
META_DATABASE_NAME = "meta.db"
# インデックスデータベースを保存するディレクトリ
INDEXES_DIR = "indexes"

# ロックをメタデータベース用とインデックスデータベース用で分離
meta_db_lock = threading.Lock()
index_db_lock = threading.RLock()

logger = logging.getLogger(__name__)

def get_meta_db_connection():
    """メタデータベースへの接続を取得します。"""
    conn = sqlite3.connect(META_DATABASE_NAME, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def get_index_db_connection(db_path: str):
    """指定されたインデックスデータベースへの接続を取得します。"""
    # データベースファイルが存在しない場合は作成される
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_meta_database():
    """
    メタデータベースと必要なテーブルが存在しない場合にのみ初期化します。
    """
    with meta_db_lock: # メタデータベース用のロックを使用
        conn = get_meta_db_connection()
        try:
            conn.execute("PRAGMA journal_mode = WAL;")
            cursor = conn.cursor()

            # 'indexes' テーブルが存在するか確認
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='indexes'")
            if cursor.fetchone() is None:
                logger.info("メタデータベースのテーブルが存在しないため、作成します...")
                conn.execute("""
                    CREATE TABLE indexes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        target_directory TEXT NOT NULL,
                        allowed_extensions TEXT NOT NULL,
                        db_path TEXT NOT NULL UNIQUE,
                        last_indexed_at DATETIME,
                        status TEXT DEFAULT 'not_indexed'
                    )
                """)
                conn.commit()
                logger.info("メタデータベースのセットアップが正常に完了しました。")
            else:
                logger.info("既存のメタデータベースを使用します。")

        except sqlite3.Error as e:
            logger.error(f"メタデータベースの初期化中に致命的なエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            conn.close()

def create_index_tables(db_path: str):
    """
    指定されたインデックスデータベース内に必要なテーブルを作成します。
    テーブルが存在しない場合にのみ作成します。
    """
    with index_db_lock: # インデックスデータベース用のロックを使用
        conn = get_index_db_connection(db_path)
        try:
            conn.execute("PRAGMA journal_mode = WAL;")
            logger.info(f"インデックスデータベース '{db_path}' のジャーナルモードをWALに設定しました。")

            cursor = conn.cursor()

            # files テーブルが存在するか確認し、なければ作成
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                logger.info(f"インデックスデータベース '{db_path}' にテーブルを作成します...")
                conn.execute("""
                    CREATE TABLE files (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        path TEXT NOT NULL UNIQUE,
                        content TEXT,
                        file_type TEXT,
                        modified_date REAL,
                        created_date REAL
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
                # FTS5テーブルをtrigramトークナイザーで作成（content-syncを使用しない）
                # content-syncとtrigramの組み合わせは問題を引き起こすため、独立したテーブルを使用
                conn.execute("""
                    CREATE VIRTUAL TABLE files_fts USING fts5(
                        path, 
                        content,
                        tokenize = 'trigram'
                    );
                """)
                logger.info(f"インデックスデータベース '{db_path}' にFTS5テーブルが正常に作成されました。")
                
                # デフォルト設定 (インデックスデータベースごと)
                conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('indexing_stop_requested', 'False'))
                
                conn.commit()
                logger.info(f"インデックスデータベース '{db_path}' のテーブルセットアップが正常に完了しました。")
            else:
                # 既存テーブルにカラムを追加（存在しない場合のみ）
                cursor.execute("PRAGMA table_info(files)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if 'file_type' not in columns:
                    logger.info(f"インデックスデータベース '{db_path}' のfilesテーブルにfile_typeカラムを追加します...")
                    conn.execute("ALTER TABLE files ADD COLUMN file_type TEXT")
                
                if 'modified_date' not in columns:
                    logger.info(f"インデックスデータベース '{db_path}' のfilesテーブルにmodified_dateカラムを追加します...")
                    conn.execute("ALTER TABLE files ADD COLUMN modified_date REAL")
                
                if 'created_date' not in columns:
                    logger.info(f"インデックスデータベース '{db_path}' のfilesテーブルにcreated_dateカラムを追加します...")
                    conn.execute("ALTER TABLE files ADD COLUMN created_date REAL")
                
                # FTS5テーブルが存在しない場合は作成
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
                fts_exists = cursor.fetchone() is not None
                
                if not fts_exists:
                    logger.warning(f"インデックスデータベース '{db_path}' のFTS5テーブルが存在しません。作成します。")
                    # FTS5テーブルをtrigramトークナイザーで作成（content-syncを使用しない）
                    conn.execute("""
                        CREATE VIRTUAL TABLE files_fts USING fts5(
                            path, 
                            content,
                            tokenize = 'trigram'
                        );
                    """)
                    logger.info(f"インデックスデータベース '{db_path}' のFTS5テーブルを作成しました。")
                
                conn.commit()
                logger.info(f"インデックスデータベース '{db_path}' のテーブルは既に存在します。")

        except sqlite3.Error as e:
            logger.error(f"インデックスデータベース '{db_path}' のテーブル作成中に致命的なエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            conn.close()

# --- メタデータベース操作関数 ---

def add_index_config(name: str, target_directory: str, allowed_extensions: str) -> int:
    """新しいインデックス設定をメタデータベースに追加し、対応するDBファイルを作成します。""" 
    # メタデータベースのロック内でメタDBへの書き込みを行い、その後インデックスDBの作成を行う
    with meta_db_lock: 
        meta_conn = get_meta_db_connection()
        try:
            # 新しいDBファイルのパスを生成
            db_filename = f"index_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(name)}.db"
            db_path = os.path.join(INDEXES_DIR, db_filename)
            
            meta_conn.execute("""
                INSERT INTO indexes (name, target_directory, allowed_extensions, db_path, last_indexed_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, target_directory, allowed_extensions, db_path, None, 'not_indexed'))
            meta_conn.commit()
            index_id = meta_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            logger.info(f"新しいインデックス設定を追加しました: ID={index_id}, Name='{name}', DB Path='{db_path}'")
        except sqlite3.IntegrityError:
            logger.warning(f"インデックス名 '{name}' は既に存在します。")
            return -1 # または適切なエラーコード
        except sqlite3.Error as e:
            logger.error(f"インデックス設定の追加中にエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            meta_conn.close()

    # メタデータベースのロックを解放してから、インデックスデータベースのテーブルを作成
    # これによりデッドロックを回避
    try:
        create_index_tables(db_path)
        return index_id
    except Exception as e:
        logger.error(f"インデックスデータベースのテーブル作成中にエラーが発生しました: {e}", exc_info=True)
        # エラーが発生した場合は、メタデータベースからエントリを削除するなどのロールバック処理も検討
        with meta_db_lock:
            meta_conn = get_meta_db_connection()
            try:
                meta_conn.execute("DELETE FROM indexes WHERE id = ?", (index_id,))
                meta_conn.commit()
                logger.warning(f"インデックスID {index_id} のテーブル作成に失敗したため、メタデータベースからエントリを削除しました。")
            except Exception as rollback_e:
                logger.error(f"ロールバック中にエラーが発生しました: {rollback_e}")
            finally:
                meta_conn.close()
        raise # 元のエラーを再スロー

def get_all_index_configs():
    """すべてのインデックス設定をメタデータベースから取得します。"""
    with meta_db_lock: # メタデータベース用のロックを使用
        meta_conn = get_meta_db_connection()
        try:
            cursor = meta_conn.execute("SELECT id, name, target_directory, allowed_extensions, db_path, last_indexed_at, status FROM indexes ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
        finally:
            meta_conn.close()

def get_index_config_by_id(index_id: int):
    """指定されたIDのインデックス設定をメタデータベースから取得します。"""
    with meta_db_lock: # メタデータベース用のロックを使用
        meta_conn = get_meta_db_connection()
        try:
            cursor = meta_conn.execute("""
                SELECT id, name, target_directory, allowed_extensions, db_path, last_indexed_at, status
                FROM indexes WHERE id = ?
            """, (index_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
        finally:
            meta_conn.close()

def delete_index_config(index_id: int):
    """指定されたIDのインデックス設定と、関連するDBファイルを削除します。"""
    with meta_db_lock: # メタデータベース用のロックを使用
        meta_conn = get_meta_db_connection()
        try:
            # 関連するDBファイルのパスを取得
            cursor = meta_conn.execute("SELECT db_path FROM indexes WHERE id = ?", (index_id,))
            result = cursor.fetchone()
            if result:
                db_path = result['db_path']
                
                # メタデータベースからエントリを削除
                meta_conn.execute("DELETE FROM indexes WHERE id = ?", (index_id,))
                meta_conn.commit()
                logger.info(f"メタデータベースからインデックスID {index_id} を削除しました。")

                # 関連するDBファイルを削除
                if os.path.exists(db_path):
                    os.remove(db_path)
                    logger.info(f"関連するインデックスデータベースファイル '{db_path}' を削除しました。")
                else:
                    logger.warning(f"関連するインデックスデータベースファイル '{db_path}' が見つかりませんでした。")
                return True
            else:
                logger.warning(f"インデックスID {index_id} が見つかりませんでした。")
                return False
        except sqlite3.Error as e:
            logger.error(f"インデックス設定の削除中にエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            meta_conn.close()

def update_index_status(index_id: int, status: str, last_indexed_at: datetime = None):
    """メタデータベース内のインデックスのステータスと最終インデックス作成日時を更新します。"""
    with meta_db_lock: # メタデータベース用のロックを使用
        meta_conn = get_meta_db_connection()
        try:
            if last_indexed_at:
                meta_conn.execute("""
                    UPDATE indexes SET status = ?, last_indexed_at = ? WHERE id = ?
                """, (status, last_indexed_at.isoformat(), index_id))
            else:
                meta_conn.execute("""
                    UPDATE indexes SET status = ? WHERE id = ?
                """, (status, index_id))
            meta_conn.commit()
            logger.info(f"インデックスID {index_id} のステータスを '{status}' に更新しました。")
        except sqlite3.Error as e:
            logger.error(f"インデックスステータスの更新中にエラーが発生しました: {e}", exc_info=True)
            raise
        finally:
            meta_conn.close()

# --- 個別インデックスデータベース操作関数 (db_pathを引数に追加) ---

def get_setting(db_path: str, key: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        conn = get_index_db_connection(db_path)
        try:
            cursor = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result['value'] if result else None
        finally:
            conn.close()

def set_setting(db_path: str, key: str, value: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        conn = get_index_db_connection(db_path)
        try:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
        finally:
            conn.close()

def add_directory_to_history(db_path: str, path: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        conn = get_index_db_connection(db_path)
        try:
            conn.execute("INSERT OR REPLACE INTO directory_history (path) VALUES (?) ", (path,))
            conn.commit()
        finally:
            conn.close()

def get_directory_history(db_path: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        conn = get_index_db_connection(db_path)
        try:
            cursor = conn.execute("SELECT path FROM directory_history ORDER BY timestamp DESC")
            history = [row['path'] for row in cursor.fetchall()]
            return history
        finally:
            conn.close()

def update_indexing_status(conn, db_path: str, status: str, total_files: int = None, processed_files: int = None, start_time: float = None, estimated_end_time: float = None):
    logger.debug(f"DB: update_indexing_status called for {db_path} with status={status}, total_files={total_files}, processed_files={processed_files}")
    with index_db_lock: # インデックスデータベース用のロックを使用
        try:
            conn.execute("INSERT OR REPLACE INTO indexing_status (id, status, total_files, processed_files, start_time, estimated_end_time) VALUES (?, ?, ?, ?, ?, ?)",
                         (1, status, total_files, processed_files, start_time, estimated_end_time))
            conn.commit()
        finally:
            pass # 接続は呼び出し元で閉じる

def get_indexing_status(conn, db_path: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        try:
            cursor = conn.execute("SELECT status, total_files, processed_files, start_time, estimated_end_time FROM indexing_status WHERE id = 1")
            status = cursor.fetchone()
            logger.debug(f"DB: get_indexing_status for {db_path} returned: {status}")
            return status
        finally:
            pass # 接続は呼び出し元で閉じる

def set_indexing_stop_requested(conn, db_path: str, requested: bool):
    with index_db_lock: # インデックスデータベース用のロックを使用
        try:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ('indexing_stop_requested', str(requested)))
            conn.commit()
        finally:
            pass # 接続は呼び出し元で閉じる

def is_indexing_stop_requested(conn, db_path: str):
    with index_db_lock: # インデックスデータベース用のロックを使用
        try:
            cursor = conn.execute("SELECT value FROM settings WHERE key = ?", ('indexing_stop_requested',))
            result = cursor.fetchone()
            return result['value'] == 'True' if result else False
        finally:
            pass # 接続は呼び出し元で閉じる