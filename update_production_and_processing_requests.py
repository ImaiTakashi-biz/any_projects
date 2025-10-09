import xlrd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import datetime
import time
import smtplib
from email.mime.text import MIMEText
import traceback
import os
import sys
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()

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
    today = datetime.date.today()
    today_date = today.strftime('%Y/%m/%d')

    # 認証情報を設定（環境変数から読み込み）
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(RESOLVED_GOOGLE_API_KEY_FILE, scope)
    client = gspread.authorize(creds)

    # 洗浄・二次処理依頼
    ws = client.open_by_key("1XpKStAPNFs4go7EPsbL-_UMtq_rV5Dag_yRAf1o_ffQ") #スプレッドシートのkey
    sh = ws.worksheet("依頼一覧")
    all_values = sh.get_all_values()

    # 工程完了削除対象の行のインデックスを収集
    indexes_to_remove = []
    for i, row in enumerate(all_values):
        if row[12] == "TRUE":
            indexes_to_remove.append(i)

    # 対応完了記録へアップデート
    sh_record = ws.worksheet("対応完了品記録")

    # 完了したタスクを格納するリスト
    completed_tasks = []

    # リスト内の各行をチェック
    for task in all_values[:]:  # コピーしたリストを使ってイテレーション
        if task[12] == "TRUE":
            combined_task = task[1:10] + task[13:]
            completed_tasks.append(combined_task)  # 修正後のタスクを追加

    # 各行の最後尾にformatted_dateを追加
    for task in completed_tasks:
        task.append(today_date)

    # 対応完了品記録シートデータ取得
    all_record = sh_record.get_all_values()

    # completed_tasks を all_record に結合
    all_record.extend(completed_tasks)

    # 対応完了品記録シートデータ更新
    sh_record.update(values=all_record, range_name="A1")

    # 収集したインデックスを逆順に並べ、リストから削除
    for index in reversed(indexes_to_remove):
        del all_values[index]

    # セル範囲A3:H100を空のデータで上書き
    empty_data = [['' for _ in range(13)] for _ in range(100)]
    range_to_clear = 'A1:M100'
    sh.update(values=empty_data, range_name=range_to_clear)

    # データ書き込み
    sh.update(values=all_values, range_name="A1")
    values = sh.get("L3:M100")
    for row in values:
        for i in range(len(row)):
            if row[i] == 'TRUE':
                row[i] = True
            elif row[i] == 'FALSE':
                row[i] = False
    sh.update(values=values, range_name="L3")

    # セル範囲 A:A の値を取得し、日付形式に変換して更新する（改良版）
    print("日付データの正規化と並び替えを開始します...")

    # 全データを取得
    all_data = sh.get_all_values()

    if len(all_data) > 2:
        # ヘッダー行（1行目）と2行目を保護し、3行目以降のデータ行を取得
        header = all_data[0]
        second_row = all_data[1] if len(all_data) > 1 else []
        data_rows = all_data[2:]  # 3行目以降のデータ
        
        # 日付正規化と並び替えを同時に実行
        processed_rows = []
        invalid_date_rows = []
        
        for row in data_rows:
            if len(row) > 0 and row[0].strip():  # A列に値がある場合
                try:
                    # 日付を正規化
                    date_value = datetime.datetime.strptime(row[0], "%Y/%m/%d")
                    normalized_date = date_value.strftime("%Y/%m/%d")
                    
                    # 行のA列を正規化された日付で更新
                    row[0] = normalized_date
                    processed_rows.append((date_value, row))
                    
                except ValueError:
                    # 日付として認識できない行は別途処理
                    invalid_date_rows.append(row)
                    print(f"日付として認識できない行をスキップ: A列の値 = '{row[0]}'")
        
        # 日付昇順で並び替え
        processed_rows.sort(key=lambda x: x[0])
        
        # 並び替えたデータと無効な日付の行を結合
        sorted_data = [row for _, row in processed_rows] + invalid_date_rows
        
        # ヘッダー、2行目、並び替えたデータを結合
        final_data = [header] + [second_row] + sorted_data
        
        # スプレッドシートを更新
        sh.clear()  # シートをクリア
        sh.update(values=final_data, range_name="A1")  # 並び替えたデータを書き込み
        
        print(f"日付正規化完了: {len(processed_rows)}行")
        print(f"無効な日付行: {len(invalid_date_rows)}行")
        print("データを日付昇順で並び替えました。")
        
    else:
        print("並び替えるデータがありません。（3行目以降のデータが必要）")

    print("日付処理が完了しました。")
   
    # 各リンク先 生産支援管理表シートキーをコピー
    ws_key = client.open_by_key("184vxMHttnn6HmfCFW2uM6B94e5tAscfFCEpgXF0wKOk") #スプレッドシートのkey
    sh_key = ws_key.worksheet("シート1")
    cell_value = sh_key.acell('B4').value

    # 生産支援管理表を開く
    ws = client.open_by_url(cell_value)
    sh = ws.worksheet("管理用")
    next_day = sh.acell('P3').value
    next_sh = ws.worksheet(next_day)

    # リスト内の空の文字列をNoneに変更する
    for row in all_values:
        for i, cell in enumerate(row):
            if isinstance(cell, str) and cell.strip() == '':
                row[i] = None

    # 管理表へall_valuesリストを書き込み
    # next_sh.update('A70', all_values)

    # 材料管理用データ取得
    cell_range = sh.range('A1:N27')
    data = []
    for i in range(0, len(cell_range), 14):
        row_data = [cell.value for cell in cell_range[i:i+14]]
        data.append(row_data)

    # 空白削除対象の行のインデックスを収集
    for row in data:
        if row[4] == "":
            data.remove(row)

    # 完了削除対象の行のインデックスを収集
    indexes_to_remove = []
    for i, row in enumerate(data):
        if row[13] == "〇":
            indexes_to_remove.append(i)

    # 収集したインデックスを逆順に並べ、リストから削除
    for index in reversed(indexes_to_remove):
        del data[index]

    # dataからdata[:][13]のデータを削除する
    new_data = [[cell for idx, cell in enumerate(row) if idx != 13] for row in data]
    data = new_data

    # セル範囲A1:M27の値を空にする
    empty_values = [[''] * 13] * 27  # 空の値を持つ27行13列の二次元リストを作成
    sh.update(values=empty_values, range_name='A1')  # A1セルから指定した範囲に空の値を更新

    # dataリスト内のデータ置換
    data = [[False if cell == "FALSE" else cell for cell in row] for row in data]
    data = [[True if cell == "TRUE" else cell for cell in row] for row in data]

    # データをA1セルから書き込む
    sh.update(values=data, range_name='A1')

    # 管理表へdataリストを書き込み
    next_sh.update(values=data, range_name='AM70')

    # 製品マスター
    wb = xlrd.open_workbook(r"\\192.168.1.200\共有\生産管理課\製品マスター.xls")
    ws = wb.sheet_by_name("製品マスター") # シート名を指定
    
    # A列が空になった行番号を取得
    empty_row_indices = [row_index for row_index in range(ws.nrows) if ws.cell_value(row_index, 0) == '']

    # データ取得
    hinban_data = []
    for _ in range(1, empty_row_indices[1]):
        value1 = ws.cell(_, 0).value
        hinban_data.append(value1)

    senjou_data = []
    for _ in range(1, empty_row_indices[1]):
        value2 = ws.cell(_, 35).value
        senjou_data.append(value2)

    # 洗浄指示確認用
    ws_clean = client.open_by_key("1mXaPA36hJCsBR19bZwnPG6Rf-k7h-0CVgqdOSe7WPz4") #スプレッドシートのkey
    sh_clean = ws_clean.worksheet("🔒data")

    # データ書き込み
    sh_clean.update(values=[[data] for data in hinban_data], range_name='F5')
    sh_clean.update(values=[[data] for data in senjou_data], range_name='G5')
    
    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise