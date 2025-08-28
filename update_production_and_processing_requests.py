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
    # 本日の日付を取得
    today = datetime.date.today()
    # 表示形式を指定してフォーマット
    today_date = today.strftime('%Y/%m/%d')
    # 結果を表示
    print(today_date)

    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('aptest-384703-24764f69b34f.json', scope)
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
    completed_tasks = []
    for task in all_values[:]:  # コピーしたリストを使ってイテレーション
        if task[12] == "TRUE":
            combined_task = task[1:10] + task[13:]
            completed_tasks.append(combined_task)  # 修正後のタスクを追加

    # 各行の最後尾にformatted_dateを追加
    for task in completed_tasks:
        task.append(today_date)

    print(completed_tasks)

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

    # セル範囲 A:A の値を取得し、日付形式に変換して更新する
    values = sh.col_values(1)  # A列の値を取得
    for i in range(1, len(values) + 1):  # A列の各セルについて
        try:
            date_value = datetime.datetime.strptime(values[i - 1], "%Y/%m/%d")  # 文字列を日付オブジェクトに変換
            sh.update_cell(i, 1, date_value.strftime("%Y/%m/%d"))  # 日付を指定した形式でセルに書き込む
            time.sleep(1)  # 1秒の待ち時間を設ける
            print(f"Cell A{i} updated successfully.")
        except ValueError:
            print(f"Skipping non-date value at Cell A{i}.")

    print("Process completed.")


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
    ws = client.open_by_key("1mXaPA36hJCsBR19bZwnPG6Rf-k7h-0CVgqdOSe7WPz4") #スプレッドシートのkey
    sh = ws.worksheet("🔒data")

    # データ書き込み
    sh.update(values=[[data] for data in hinban_data], range_name='F5')
    sh.update(values=[[data] for data in senjou_data], range_name='G5')
    
    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise