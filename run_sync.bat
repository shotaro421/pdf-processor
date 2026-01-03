@echo off
chcp 65001 > nul
title PDF Processor - Auto Sync

echo ============================================
echo   PDF Processor - ワンクリック同期ツール
echo ============================================
echo.

REM フォルダの確認・作成
if not exist "%USERPROFILE%\pdf-processor-local\input" mkdir "%USERPROFILE%\pdf-processor-local\input"
if not exist "%USERPROFILE%\pdf-processor-local\output" mkdir "%USERPROFILE%\pdf-processor-local\output"

echo [INFO] 入力フォルダ: %USERPROFILE%\pdf-processor-local\input
echo [INFO] 出力フォルダ: %USERPROFILE%\pdf-processor-local\output
echo.

REM 入力フォルダにPDFがあるか確認
dir /b "%USERPROFILE%\pdf-processor-local\input\*.pdf" > nul 2>&1
if errorlevel 1 (
    echo [警告] 入力フォルダにPDFがありません。
    echo        %USERPROFILE%\pdf-processor-local\input にPDFを入れてください。
    echo.
    echo 入力フォルダを開きますか？ (Y/N)
    choice /c YN /n
    if errorlevel 2 goto end
    start "" "%USERPROFILE%\pdf-processor-local\input"
    goto end
)

echo [INFO] PDFを検出しました。処理を開始します...
echo.

REM Python sync toolを実行
cd /d "%~dp0"
python tools/sync.py sync

echo.
echo ============================================
echo   処理完了！
echo   出力フォルダ: %USERPROFILE%\pdf-processor-local\output
echo ============================================
echo.
echo 出力フォルダを開きますか？ (Y/N)
choice /c YN /n
if errorlevel 2 goto end
start "" "%USERPROFILE%\pdf-processor-local\output"

:end
echo.
pause
