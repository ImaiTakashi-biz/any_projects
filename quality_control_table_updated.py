import openpyxl
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import pyodbc as pyo
import time
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
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVERS = os.getenv("EMAIL_RECEIVERS", "").split(",") if os.getenv("EMAIL_RECEIVERS") else []
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

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
    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('aptest-384703-24764f69b34f.json', scope)
    client = gspread.authorize(creds)

    # 品証管理表
    ws = client.open_by_key("1jk__1dhS_8jTDah63MH3vKgn2MbSpx4t5wZSrwSYkHo") #スプレッドシートのkey
    sh_pic = ws.worksheet("出荷検査担当")
    sh_next = ws.worksheet("翌日分振分用")

    # 工程内データ読込み、書込み A2:I36
    cell_range = 'A2:I36'
    data1 = sh_pic.get(cell_range)
    for row in data1:
        if len(row) > 8 and row[8] == "TRUE":
            row[:7] = [""] * 7
            row[7:9] = ["FALSE"] * 2
    sh_pic.update(values=data1, range_name="A39")
    values = sh_pic.get("H2:I36")
    new_values = [["FALSE", "FALSE"] for _ in range(len(values))]
    sh_pic.update(values=new_values, range_name="H2")
    values = sh_pic.get("H2:I73")
    for row in values:
        for i in range(len(row)):
            if row[i] == 'TRUE':
                row[i] = True
            elif row[i] == 'FALSE':
                row[i] = False
    sh_pic.update(values=values, range_name="H2")

    # 工程内データ読込み、書込み O2:W36
    cell_range = 'O2:W36'
    data2 = sh_pic.get(cell_range)
    for row in data2:
        if len(row) > 8 and row[8] == "TRUE":
            row[:7] = [""] * 7
            row[7:9] = ["FALSE"] * 2
    sh_pic.update(values=data2, range_name="O39")
    values = sh_pic.get("V2:W36")
    new_values = [["FALSE", "FALSE"] for _ in range(len(values))]
    sh_pic.update(values=new_values, range_name="V2")
    values = sh_pic.get("V2:W73")
    for row in values:
        for i in range(len(row)):
            if row[i] == 'TRUE':
                row[i] = True
            elif row[i] == 'FALSE':
                row[i] = False
    sh_pic.update(values=values, range_name="V2")

    # 外注当日出荷品・メッキ上がり品等データ読込み、書込み AG3:AM22
    cell_range = 'AG3:AM22'
    data3 = sh_pic.get(cell_range)
    data3 = [row for row in data3 if row]
    data3 = [row for row in data3 if len(row) > 6 and row[6] != 'TRUE']
    sh_pic.update(values=data3, range_name="AG3")
    cell_range = 'AM3:AM22'
    data4 = sh_pic.get(cell_range)
    for row_index, row in enumerate(data4):
        for col_index, cell_value in enumerate(row):
            if cell_value == 'FALSE':
                data4[row_index][col_index] = False
    sh_pic.update(values=data4, range_name=cell_range)

    # 在庫洗浄品・二次工程完了品
    cell_range = 'AM24:AM47'
    data5 = sh_pic.get(cell_range)
    for row_index, row in enumerate(data5):
        for col_index, cell_value in enumerate(row):
            if cell_value == 'TRUE':
                data5[row_index][col_index] = False
            elif cell_value == 'FALSE':
                data5[row_index][col_index] = False
    sh_pic.update(values=data5, range_name=cell_range)

    # 現品票印刷Accessデータ取得
    for driver in pyo.drivers():
        if driver.startswith('Microsoft Access Driver'):
            print(driver)
    conn_str = (
        r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=\\192.168.1.200\共有\生産管理課\現品票印刷.accdb'
        )
    conn = pyo.connect(conn_str)
    cur = conn.cursor()
    for table in cur.tables(tableType='TABLE'):
        print(table.table_name)
    sql = "select * from t_現品票履歴"
    cur.execute(sql)
    table = cur.fetchall()
    cur.close()
    conn.close()

    today_data = []
    target_col = [26, 0, 1, 2, 3, 4, 5, 6, 9, 25, 27]
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    for _ in table:
        data = []
        if len(_) > 27 and 'E' in _[0] and _[27] == today:
            for col in target_col:
                value1 = _[col]
                if isinstance(value1, datetime.datetime):
                    value1 = datetime.datetime.strftime(value1, "%Y/%m/%d")
                data.append(value1)
            today_data.append(data)

    # データ整理
    sh_data = ws.worksheet("データ")

    cell_range = 'A16:O70'
    data6 = sh_data.get(cell_range)
    indexes_to_remove = []
    for i, row in enumerate(data6):
        if len(row) == 0 or (len(row) > 0 and row[0] == ""):
            indexes_to_remove.append(i)
        elif len(row) > 12 and row[12] == "TRUE":
            indexes_to_remove.append(i)
    for index in reversed(indexes_to_remove):
        del data6[index]

    # リスト編集、結合
    name_list = []
    for _ in data6:
        if len(_) > 14:
            value2 = _[14]
        else:
            value2 = None
        name_list.append(value2)

    for row in data6:
        if len(row) > 12:
            del row[12:15]
    combined_list = data6 + today_data

    cell_list = sh_data.range('A16:L70')
    for cell in cell_list:
        cell.value = ''
    sh_data.update_cells(cell_list)
    sh_data.update(values=combined_list, range_name="A16")

    cell_range = 'AE3:AE57'
    data8 = sh_pic.get(cell_range)
    for row_index, row in enumerate(data8):
        for col_index, cell_value in enumerate(row):
            if cell_value == 'TRUE':
                data8[row_index][col_index] = False
            elif cell_value == 'FALSE':
                data8[row_index][col_index] = False
    sh_pic.update(values=data8, range_name=cell_range)

    cell_range = 'AD3:AD57'
    sh_pic.batch_clear([cell_range])
    for i in range(len(name_list)):
        sh_pic.update_cell(3 + i, 30, name_list[i])


    # 外注当日出荷品・メッキ上がり品等データ_数式設定 AJ3:AK22
    start_row, end_row = 3, 22
    aj_formulas = [f"=XLOOKUP($AI{row},'製品マスタ'!$B:$B,'製品マスタ'!$C:$C,\"\",FALSE)" for row in range(start_row, end_row + 1)]
    ak_formulas = [f"=XLOOKUP($AI{row},'製品マスタ'!$B:$B,'製品マスタ'!$A:$A,\"\",FALSE)" for row in range(start_row, end_row + 1)]

    # 数式をセル範囲に入力
    def set_formulas_in_batches(sh_pic, start_cell, formulas):
        for i in range(0, len(formulas), 5):  # 5件ずつ書き込み（API制限対策）
            end_row = start_row + i + len(formulas[i:i+5]) - 1
            cell_range = f"{start_cell}{start_row + i}:{start_cell}{end_row}"
            formulas_batch = [[formula] for formula in formulas[i:i+5]]
            sh_pic.update(range_name=cell_range, values=formulas_batch, value_input_option='USER_ENTERED')
            time.sleep(1)  # API呼び出し制限を避けるためのウェイト

    # AJ列とAK列の数式をバッチ処理で入力
    set_formulas_in_batches(sh_pic, 'AJ', aj_formulas)
    set_formulas_in_batches(sh_pic, 'AK', ak_formulas)

    # セル範囲AL24:AL47のデータをクリア
    cell_range_clear = 'AL24:AL47'
    empty_data = [[""] * len(data5[0]) for _ in range(len(data5))]
    sh_pic.update(values=empty_data, range_name=cell_range_clear)

    # F2:F36のデータをコピー
    data_f = sh_next.get("F2:F36")
    sh_pic.update(range_name="F2:F36", values=data_f, value_input_option="USER_ENTERED")

    # T2:T36のデータをコピー
    data_t = sh_next.get("T2:T36")
    sh_pic.update(range_name="T2:T36", values=data_t, value_input_option="USER_ENTERED")

    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise