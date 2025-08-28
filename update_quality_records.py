import xlrd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import pyodbc as pyo
import datetime
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

    # 製品マスター
    wb = xlrd.open_workbook(r"\\192.168.1.200\共有\生産管理課\製品マスター.xls")
    ws = wb.sheet_by_name("製品マスター") #シート名を指定
    
    # A列が空になった行番号を取得
    empty_row_indices = [row_index for row_index in range(ws.nrows) if ws.cell_value(row_index, 0) == '']

    # データ取得
    hinban_data = []
    for _ in range(1, empty_row_indices[1]):
        data = []
        for col in range(0, 4):
            value1 = ws.cell(_, col).value
            data.append(value1)
        hinban_data.append(data)

    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('aptest-384703-24764f69b34f.json', scope)
    client = gspread.authorize(creds)

    # 品質管理記録
    ws = client.open_by_key("1se9Gvgd5dsk3hLAaLQ4C1M_yBsQpLBkZxWmHxEigrf0")  # スプレッドシートのkey
    sh_master = ws.worksheet("製品マスター")
    sh_manage = ws.worksheet("品質管理表")
    sh_record = ws.worksheet("対応完了品記録")

    # 製品マスター更新
    sh_master.update(values=hinban_data, range_name="A2")

    # 品質管理表シートデータ取得
    all_manage = sh_manage.get_all_values()

    # 完了したタスクを格納するリスト
    completed_tasks = []

    # リスト内の各行をチェック
    for task in all_manage[:]:  # コピーしたリストを使ってイテレーション
        if task[19] == "済":
            completed_tasks.append(task[1:])  # [1] 番目以降のデータを追加
            all_manage.remove(task)

    # 各行の最後尾にformatted_dateを追加
    for task in completed_tasks:
        task.append(today_date)

    # 対応完了品記録シートデータ取得
    all_record = sh_record.get_all_values()

    # completed_tasks を all_record に結合
    all_record.extend(completed_tasks)

    # all_manage の最初の行を削除
    if all_manage:
        all_manage.pop(0)

    # 更新された all_manage の再構築
    first_part = [task[0:7] for task in all_manage]
    second_part = [task[9:15] for task in all_manage]
    third_part = [task[16:20] for task in all_manage]

    # それぞれに10行の空白行を追加
    for _ in range(10):
        first_part.append([""] * 7)
        second_part.append([""] * 6)
        third_part.append([""] * 4)

    # 結果の表示
    print("first_part:", first_part)
    print("second_part:", second_part)
    print("third_part:", third_part)
    print("結合された all_record:", all_record)

    # 品質管理表シートデータ更新
    sh_manage.update(values=first_part, range_name="A2")
    sh_manage.update(values=second_part, range_name="J2")
    sh_manage.update(values=third_part, range_name="Q2")


    # 対応完了品記録シートデータ更新
    sh_record.update(values=all_record, range_name="A1")


    # セルの表示形式を日付に変更する関数
    def set_date_format(spreadsheet, sheet_id, cell_ranges):
        requests = []
        for cell_range in cell_ranges:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": cell_range['startRowIndex'],
                        "endRowIndex": cell_range['endRowIndex'],
                        "startColumnIndex": cell_range['startColumnIndex'],
                        "endColumnIndex": cell_range['endColumnIndex']
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "yyyy/mm/dd"  # 必要に応じてパターンを変更
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
        spreadsheet.batch_update({"requests": requests})

    # セル範囲を指定して表示形式を変更
    cell_ranges = [
        {'startRowIndex': 1, 'endRowIndex': 100, 'startColumnIndex': 2, 'endColumnIndex': 3},  # C2:C100
        {'startRowIndex': 1, 'endRowIndex': 100, 'startColumnIndex': 5, 'endColumnIndex': 6},  # F2:F100
        {'startRowIndex': 1, 'endRowIndex': 100, 'startColumnIndex': 10, 'endColumnIndex': 11}  # K2:K100
    ]

    # ワークシートのシートIDを取得
    sheet_id = sh_manage._properties['sheetId']

    # 表示形式を変更
    set_date_format(ws, sheet_id, cell_ranges)
    
    print("処理が完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise