@echo off
REM --- ファイル検索アプリケーション 起動バッチファイル --- 

echo Python仮想環境を有効化します...
call .\myenv\Scripts\activate

if %errorlevel% neq 0 (
    echo 仮想環境の有効化に失敗しました。
    pause
    exit /b
)

echo アプリケーションサーバーを起動します...
echo ブラウザで http://127.0.0.1:8000 を手動で開いてください。

uvicorn main:app --reload

pause
