import datetime
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
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

# --- Google API設定 ---
GOOGLE_SERVICE_ACCOUNT_KEY_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_FILE")

# --- メール通知用の設定 ---
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
    # 日付設定
    now = datetime.datetime.now()
    yesterday = now - datetime.timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y%m%d")

    # ファイルパス設定
    if now.weekday() in [5, 6]:  # 土日
        file_path = fr"\\192.168.1.200\共有\製造課\MTLINKI\{yesterday_date}.csv"
    else:
        file_path = fr"\\192.168.1.200\共有\製造課\MTLINKI\Backup\{yesterday_date}.csv"

    # CSVデータ読み込み
    quantity = []
    with open(file_path, encoding="utf-8") as f:
        for row in csv.reader(f):
            quantity.append(row)

    # 対象リストと列番号のマッピング
    column_mapping = {
        "E-12": {"date_col": 39, "value_col": 40},
        "F-7": {"date_col": 46, "value_col": 47},
        "F-8": {"date_col": 53, "value_col": 54},
        "F-13": {"date_col": 67, "value_col": 68},
        "F-14": {"date_col": 74, "value_col": 75},
        "E-14": {"date_col": 81, "value_col": 82},
        "E-13": {"date_col": 88, "value_col": 89},
    }

    # target_numが変動する場合（例: 一部項目を除外）
    target_num = ["E-12", "E-14", "E-13"]  # 動的に変動する対象リスト
    # master ["E-12", "F-7", "F-8", "F-13", "F-14", "E-14", "E-13"]
    target_list = []

    # データ検索
    for num in target_num:
        for row in quantity:
            if num == row[1]:
                target_list.append((num, row[2]))
                break
        else:  # 該当がない場合
            target_list.append((num, 0))

    # 認証とスプレッドシート設定
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(RESOLVED_GOOGLE_API_KEY_FILE, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key("1Zb9jVKffZGx5PQ6wh6CGSioiJOAx_x3DX2AS7zrCd5Y").worksheet("シャフトB進捗表")

    # 行番号計算
    start_date = datetime.date(2023, 3, 1)
    delta_days = (now.date() - start_date).days
    row_num = 5 + delta_days
    today_mmdd = now.strftime("%m/%d")

    # 更新対象のデータをスプレッドシートに書き込む
    processed = set()
    for i, (num, value) in enumerate(target_list):
        if num in column_mapping:  # マッピングに存在する場合のみ処理
            date_col = column_mapping[num]["date_col"]
            value_col = column_mapping[num]["value_col"]

            # 更新処理
            sh.update_cell(row_num, date_col, today_mmdd)
            sh.update_cell(row_num, value_col, value)
            print(f"Updated {num} → Row {row_num}, Columns {date_col}/{value_col} with {today_mmdd}, {value}")
            processed.add(num)

    # 更新対象外のセルに"-"を入力する
    for num, cols in column_mapping.items():
        if num not in processed:
            sh.update_cell(row_num, cols["date_col"], today_mmdd)
            sh.update_cell(row_num, cols["value_col"], "-")
            print(f"Updated {num} → Row {row_num}, Columns {cols['date_col']}/{cols['value_col']} with {today_mmdd}, '-'")

    # 完了メッセージ
    print("スプレッドシートの更新が完了しました。")
    
except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise