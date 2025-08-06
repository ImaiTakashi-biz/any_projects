import openpyxl
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time
import webbrowser
import pyautogui as pg
import smtplib
from email.mime.text import MIMEText
import traceback
import os
import sys

# 通知アカウント
# ・takada@araiseimitsu.onmicrosoft.com
# ・imai@araiseimitsu.onmicrosoft.com
# ・n.kizaki@araiseimitsu.onmicrosoft.com

# --- メール通知用の設定 ---
# これらの設定値は、ご自身の環境に合わせて変更してください。
# パスワードを直接コードに書くことは推奨しません。
EMAIL_SENDER = "imai@araiseimitsu.onmicrosoft.com"
EMAIL_PASSWORD = "Arai267786"
EMAIL_RECEIVERS = [
    "takada@araiseimitsu.onmicrosoft.com",
    "imai@araiseimitsu.onmicrosoft.com",
    "n.kizaki@araiseimitsu.onmicrosoft.com"
]
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

def send_error_email(error_info):
    """
    エラー発生時に指定されたアカウントへメールを送信する関数
    """
    try:
        # プログラム名とファイルパスを取得
        program_name = os.path.basename(sys.argv[0])
        file_path = os.path.abspath(sys.argv[0])
        
        # 件名にプログラム名を追記
        subject = f"【エラー通知】{program_name} 実行中にエラーが発生しました"
        
        # 本文にプログラム名とファイルパスを追記
        body = f"""
お疲れ様です。

Pythonスクリプトの実行中にエラーが発生しました。
下記に詳細を記載します。

---
プログラム名: {program_name}

ファイルパス: {file_path}

日時: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

エラー詳細:
{error_info}
---

お手数ですが、ご確認をお願いします。
"""
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(EMAIL_RECEIVERS)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVERS, msg.as_string())
        print("エラー通知メールを送信しました。")

    except Exception as e:
        print(f"メール送信中にエラーが発生しました: {e}")

# --- メイン処理 ---
try:
    webbrowser.open(r"\\192.168.1.200\共有\製造課\加工進行表\セット予定表参照.xlsx")
    pg.sleep(10)
    pg.leftClick(470, 235)
    pg.sleep(10)
    pg.hotkey("ctrl","s")
    pg.sleep(5)
    pg.hotkey("alt","f4")
    pg.sleep(5)

    # セット日データ取得
    wb = openpyxl.load_workbook(r"\\192.168.1.200\共有\製造課\加工進行表\セット予定表参照.xlsx", data_only=True)
    ws = wb["セット予定"]

    set_list = []
    for row in range(2, 71):
        value1 = ws.cell(row, 35).value
        set_list.append(value1)
    wb.close()
    print(set_list)


    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('aptest-384703-24764f69b34f.json', scope)
    client = gspread.authorize(creds)

    # 各シートキー 製造管理表シートキーをコピー
    # ws = client.open("name").sheet1 #スプレッドシートの名前
    ws_key = client.open_by_key("1FonGNX3czFYxV4ZSfEoMUXgemVSj6bZLJib-4N2BdX0") #スプレッドシートのkey
    sh_key = ws_key.worksheet("シート1")
    cell_value = sh_key.acell('C4').value

    # 製造管理表を開く
    ws = client.open_by_key(cell_value)
    sh = ws.worksheet("検証")

    # セル範囲A8:A79の値をクリア
    cell_range = 'A8:A79'
    sh.batch_update([{
        'range': cell_range,
        'values': [['' for _ in range(1)] for _ in range(72)]
    }])

    # セットデータ書き込み
    for row, value1 in enumerate(set_list, start=8):
        if isinstance(value1, datetime):
            value1 = value1.strftime("%Y/%m/%d")
        sh.update_cell(row, 1, value1)
        time.sleep(1)

    # A列のセルの表示形式を日付に変更
    a_column_range = "A:A"
    sh.format(a_column_range, {"numberFormat": {"type": "DATE", "pattern": "yyyy/m/d"}})
    
    print("完了しました。")
    
except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise