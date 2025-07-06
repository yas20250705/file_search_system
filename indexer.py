import os
import sqlite3
import time
import logging
import fitz  # PyMuPDF
from openpyxl import load_workbook
from docx import Document
from pptx import Presentation
from datetime import datetime

from database import get_index_db_connection, update_indexing_status, is_indexing_stop_requested, set_indexing_stop_requested, index_db_lock, update_index_status

logger = logging.getLogger(__name__)

# --- Text Extraction Functions ---

def extract_text_from_pdf(file_path):
    logger.debug(f"PDF抽出開始: {file_path}")
    try:
        with fitz.open(file_path) as doc:
            logger.debug(f"PDFファイルオープン成功: {file_path}")
            text = "".join(page.get_text() for page in doc)
        logger.debug(f"PDF抽出完了: {file_path}")
        return text
    except Exception as e:
        logger.error(f"PDFファイルからのテキスト抽出エラー ({file_path}): {e}", exc_info=True)
        return ""

def extract_text_from_excel(file_path):
    try:
        workbook = load_workbook(filename=file_path, read_only=True)
        text = []
        for sheet in workbook.worksheets:
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.value:
                        text.append(str(cell.value))
        return " ".join(text)
    except Exception as e:
        logger.error(f"Excelファイルからのテキスト抽出エラー ({file_path}): {e}")
        return ""

def extract_text_from_word(file_path):
    try:
        doc = Document(file_path)
        text = [p.text for p in doc.paragraphs]
        return "\n".join(text)
    except Exception as e:
        logger.error(f"Wordファイルからのテキスト抽出エラー ({file_path}): {e}")
        return ""

def extract_text_from_powerpoint(file_path):
    try:
        prs = Presentation(file_path)
        text = []
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text"): # テキストを持つシェイプのみ
                    text.append(shape.text)
        return "\n".join(text)
    except Exception as e:
        logger.error(f"PowerPointファイルからのテキスト抽出エラー ({file_path}): {e}")
        return ""

def extract_text_from_plain(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        logger.error(f"テキストファイルからの読み込みエラー ({file_path}): {e}")
        return ""

# --- Main Indexing Logic ---

def index_files(index_id: int, target_directory: str, allowed_extensions: list[str], db_path: str):
    logger.info(f"インデックスID {index_id} ('{target_directory}') のインデックス作成を開始します...")
    start_time = time.time()
    
    # メタデータベースのステータスを更新
    update_index_status(index_id, 'running')

    conn = None # 接続を初期化
    try:
        conn = get_index_db_connection(db_path)
        cursor = conn.cursor()

        cursor = conn.cursor()

        # 既存のデータをクリア
        cursor.execute("DELETE FROM files")
        cursor.execute("DELETE FROM files_fts")
        conn.commit()
        logger.info(f"インデックスID {index_id} の既存データをクリアしました。")

        files_to_index = []
        for root, _, files in os.walk(target_directory):
            for file in files:
                logger.debug(f"Indexer: Found file: {os.path.join(root, file)}")
                if any(file.endswith(ext) for ext in allowed_extensions):
                    files_to_index.append(os.path.join(root, file))
        
        logger.debug(f"Indexer: Files to index after filtering: {files_to_index}")
        total_files = len(files_to_index)
        logger.info(f"インデックスID {index_id} の対象ファイル数: {total_files}")
        
        update_indexing_status(conn, db_path, "started", total_files, 0, start_time, 0) # 個別DBのステータスを更新

        if total_files == 0:
            logger.info(f"インデックスID {index_id} の対象ファイルがありません。インデックス作成を完了します。")
            with index_db_lock:
                update_indexing_status(conn, db_path, "completed", 0, 0, start_time, time.time()) # 個別DBのステータスを更新
            update_index_status(index_id, 'completed', datetime.now())
            return # 関数を終了

        logger.debug(f"Indexer: Starting file processing loop for {total_files} files.")
        for i, file_path in enumerate(files_to_index):
            if is_indexing_stop_requested(conn, db_path):
                logger.info(f"インデックスID {index_id} のインデックス作成がユーザーによって中止されました。")
                update_indexing_status(conn, db_path, "stopped", total_files, i, start_time, time.time()) # 個別DBのステータスを更新
                update_index_status(index_id, 'stopped') # メタDBのステータスを更新
                break

            ext = os.path.splitext(file_path)[1].lower()
            content = ""
            logger.debug(f"Indexer: Extracting text from {file_path}")
            if ext == '.pdf':
                content = extract_text_from_pdf(file_path)
            elif ext in ['.xlsx', '.xls']:
                content = extract_text_from_excel(file_path)
            elif ext == '.docx':
                content = extract_text_from_word(file_path)
            elif ext == '.pptx':
                content = extract_text_from_powerpoint(file_path)
            else:
                content = extract_text_from_plain(file_path)
            logger.debug(f"Indexer: Finished extracting text from {file_path}")

            if content:
                try:
                        # 1. `files`テーブルに挿入
                        cursor.execute("INSERT OR REPLACE INTO files (path, content) VALUES (?, ?)", (file_path, content))
                        last_row_id = cursor.lastrowid
                        
                        # 2. `files_fts`テーブルに、`files`テーブルのrowidを使って挿入
                        #    これにより、2つのテーブルが正しく同期される
                        cursor.execute("INSERT INTO files_fts (rowid, path, content) VALUES (?, ?, ?)", (last_row_id, file_path, content))
                        
                except sqlite3.Error as e:
                    logger.error(f"インデックスID {index_id} のデータベース挿入エラー ({file_path}): {e}")

            # 進捗を更新
            current_processed_files = i + 1
            logger.debug(f"Indexer: Calling update_indexing_status for index {index_id} with processed_files={current_processed_files}/{total_files}")
            update_indexing_status(conn, db_path, "running", total_files, current_processed_files, start_time, 0) # 個別DBのステータスを更新

            if current_processed_files % 10 == 0:
                conn.commit() # 10ファイルごとにコミット
                logger.info(f"インデックスID {index_id} の進捗: {current_processed_files}/{total_files}")

        conn.commit() # 最終コミット
        
        if not is_indexing_stop_requested(conn, db_path): # 中止されていない場合のみ完了ステータス
            logger.info(f"インデックスID {index_id} のインデックス作成が完了しました。")
            update_indexing_status(conn, db_path, "completed", total_files, total_files, start_time, time.time()) # 個別DBのステータスを更新
            update_index_status(index_id, 'completed', datetime.now()) # メタDBのステータスと最終インデックス日時を更新
        else:
            logger.info(f"インデックスID {index_id} のインデックス作成は中止されました。")
            update_indexing_status(conn, db_path, "stopped", total_files, i, start_time, time.time()) # 個別DBのステータスを更新
            update_index_status(index_id, 'stopped') # メタDBのステータスを更新

    except Exception as e:
        logger.error(f"インデックスID {index_id} のインデックス作成中に予期せぬエラーが発生しました: {e}", exc_info=True)
        update_indexing_status(conn, db_path, "failed", total_files, i, start_time, time.time()) # 個別DBのステータスを更新
        update_index_status(index_id, 'failed') # メタDBのステータスを更新
    finally:
        if conn:
            conn.close()