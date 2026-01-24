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
echo 数秒後にブラウザが自動的に開きます...

REM サーバーを別ウィンドウで起動
start "File Search Server" cmd /k "uvicorn main:app --reload"

REM サーバーが起動するまで少し待つ
timeout /t 3 /nobreak >nul

REM ブラウザを開く
start http://127.0.0.1:8000

echo.
echo サーバーは別ウィンドウで実行中です。
echo ブラウザが開かない場合は、手動で http://127.0.0.1:8000 を開いてください。
pause

pause
