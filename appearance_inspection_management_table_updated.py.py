import xlrd
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import pyodbc as pyo
import smtplib
from email.mime.text import MIMEText
import traceback
import os
import sys
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()

# 通知アカウント
# ・takada@araiseimitsu.onmicrosoft.com
# ・imai@araiseimitsu.onmicrosoft.com
# ・n.kizaki@araiseimitsu.onmicrosoft.com

# --- メール通知用の設定 ---
# これらの設定値は、.envファイルから読み込まれます。
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVERS = os.getenv("EMAIL_RECEIVERS", "").split(",") if os.getenv("EMAIL_RECEIVERS") else []
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Google API設定の存在確認
if not GOOGLE_SERVICE_ACCOUNT_KEY_FILE:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY_FILE が .env ファイルに設定されていません")

# Google API認証ファイルパスの解決（環境非依存）
def resolve_google_api_key_file(filename):
    """
    Google API認証ファイルのパスを環境非依存で解決する
    """
    # 1. 現在の作業ディレクトリからの相対パス
    if os.path.exists(filename):
        return os.path.abspath(filename)
    
    # 2. スクリプトと同じディレクトリ
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_relative_path = os.path.join(script_dir, filename)
    if os.path.exists(script_relative_path):
        return script_relative_path
    
    # 3. ファイルが見つからない場合
    raise FileNotFoundError(
        f"Google API認証ファイルが見つかりません: {filename}\n"
        f"検索パス:\n"
        f"  - 現在の作業ディレクトリ: {os.path.abspath(filename)}\n"
        f"  - スクリプトディレクトリ: {script_relative_path}"
    )

# Google API認証ファイルパスを解決
RESOLVED_GOOGLE_API_KEY_FILE = resolve_google_api_key_file(GOOGLE_SERVICE_ACCOUNT_KEY_FILE)

# メール設定の存在確認
if not EMAIL_SENDER:
    raise ValueError("EMAIL_SENDER が .env ファイルに設定されていません")
if not EMAIL_PASSWORD:
    raise ValueError("EMAIL_PASSWORD が .env ファイルに設定されていません")
if not EMAIL_RECEIVERS:
    raise ValueError("EMAIL_RECEIVERS が .env ファイルに設定されていません")

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

日時: {datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

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
    # 製品マスター
    wb = xlrd.open_workbook(r"\\192.168.1.200\共有\生産管理課\製品マスター.xls")
    ws = wb["製品マスター"]

    # A列が空になった行番号を取得
    empty_row_indices = [row_index for row_index in range(ws.nrows) if ws.cell_value(row_index, 0) == '']

    # データ取得
    hinban_data = []
    for _ in range(1, empty_row_indices[1]):
    
        value1 = ws.cell(_, 0).value
        hinban_data.append(value1)
    
    # 不具合情報検索_Accessデータ取得
    for driver in pyo.drivers():
        if driver.startswith('Microsoft Access Driver'):
            print(driver)
    conn_str = (
        r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=\\192.168.1.200\共有\品質保証課\外観検査記録\不具合情報検索.accdb'
        )
    conn = pyo.connect(conn_str)
    cur = conn.cursor()
    for table in cur.tables(tableType='TABLE'):
        print(table.table_name)
    sql = "select * from t_不具合情報"
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    conn.close()

    # 本日の日付
    today = datetime.datetime.now()
    # 3年前の日付
    three_years_ago = today - datetime.timedelta(days=365*3)
    # Noneを含む行を除外して、3番目の要素が3年前以降の日付である行のみを取得する
    filtered_data = [row for row in data if row[3] is not None and row[3] >= (datetime.datetime.now() - datetime.timedelta(days=365*3))]
    # データ整形
    extracted_data = [(row[2], row[10], row[11]) for row in filtered_data]

    result = []
    for hinban in hinban_data:
        total_value1 = 0
        total_value2 = 0
        for item in extracted_data:
            if item[0] == hinban:
                total_value1 += item[1]
                total_value2 += item[2]
        result.append([hinban, total_value1, total_value2])

    # QR管理_Accessデータ取得
    for driver in pyo.drivers():
        if driver.startswith('Microsoft Access Driver'):
            print(driver)
    conn_str = (
        r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=\\192.168.1.200\共有\QRシステム\Access\QR管理.accdb'
        )
    conn = pyo.connect(conn_str)
    cur = conn.cursor()
    for table in cur.tables(tableType='TABLE'):
        print(table.table_name)
    sql = "select * from t_現品票履歴"
    cur.execute(sql)
    id_data = cur.fetchall()
    cur.close()
    conn.close()
    filtered_id_data = [row for row in id_data if row[10] not in ['完了']]

    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(RESOLVED_GOOGLE_API_KEY_FILE, scope)
    client = gspread.authorize(creds)

    # 外観検査管理表
    ws = client.open_by_key("1Zb9jVKffZGx5PQ6wh6CGSioiJOAx_x3DX2AS7zrCd5Y")   # スプレッドシートのkey
    sh = ws.worksheet("翌日分振分表")
    sh1 = ws.worksheet("検査時間マスター")
    sh2 = ws.worksheet("data")

    sh1.update(values=result, range_name="A2")
    sh2.clear()

    filtered_id_data_list = []
    for row in filtered_id_data:
        filtered_row = list(row)
        for i, item in enumerate(filtered_row):
            if isinstance(item, datetime.datetime):
                filtered_row[i] = item.strftime('%Y/%m/%d')   # フォーマットは適宜変更してください
        filtered_id_data_list.append(filtered_row)

    sh2.update(values=filtered_id_data_list, range_name="A1")
    
    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise