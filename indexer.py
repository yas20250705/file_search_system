import os
import sqlite3
import time
import logging
import fitz  # PyMuPDF
from openpyxl import load_workbook
from docx import Document
from pptx import Presentation
from database import get_db_connection, get_setting, update_indexing_status, is_indexing_stop_requested, set_indexing_stop_requested, db_lock

logger = logging.getLogger(__name__)

# --- Text Extraction Functions ---

def extract_text_from_pdf(file_path):
    try:
        with fitz.open(file_path) as doc:
            text = "".join(page.get_text() for page in doc)
        return text
    except Exception as e:
        logger.error(f"PDFファイルからのテキスト抽出エラー ({file_path}): {e}")
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
                if hasattr(shape, "text"):
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

def index_files(target_directory, allowed_extensions):
    logger.info(f"'{target_directory}' のインデックス作成を開始します...")
    start_time = time.time()
    
    files_to_index = []
    for root, _, files in os.walk(target_directory):
        for file in files:
            if any(file.endswith(ext) for ext in allowed_extensions):
                files_to_index.append(os.path.join(root, file))
    
    total_files = len(files_to_index)
    logger.info(f"インデックス対象ファイル数: {total_files}")
    update_indexing_status("started", total_files, 0, start_time)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for i, file_path in enumerate(files_to_index):
            if is_indexing_stop_requested():
                logger.info("インデックス作成がユーザーによって中止されました。")
                update_indexing_status("stopped", total_files, i, start_time)
                break

            ext = os.path.splitext(file_path)[1].lower()
            content = ""
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

            if content:
                with db_lock:
                    try:
                        # 1. `files`テーブルに挿入
                        cursor.execute("INSERT OR REPLACE INTO files (path, content) VALUES (?, ?)", (file_path, content))
                        last_row_id = cursor.lastrowid
                        
                        # 2. `files_fts`テーブルに、`files`テーブルのrowidを使って挿入
                        #    これにより、2つのテーブルが正しく同期される
                        cursor.execute("INSERT INTO files_fts (rowid, path, content) VALUES (?, ?, ?)", (last_row_id, file_path, content))
                        
                    except sqlite3.Error as e:
                        logger.error(f"データベース挿入エラー ({file_path}): {e}")

            if (i + 1) % 10 == 0:
                conn.commit() # 10ファイルごとにコミット
                logger.info(f"進捗: {i + 1}/{total_files}")
                update_indexing_status("running", total_files, i + 1, start_time)

        conn.commit() # 最終コミット
        logger.info("インデックス作成が完了しました。")
        update_indexing_status("completed", total_files, total_files, start_time)

    except Exception as e:
        logger.error(f"インデックス作成中に予期せぬエラーが発生しました: {e}", exc_info=True)
        update_indexing_status("failed", total_files, i, start_time)
    finally:
        conn.close()
