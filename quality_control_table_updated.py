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
import re

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

# 数値検査記録 Access DB（検査員名取得用）
NUMERICAL_INSPECTION_DB_PATH = r"\\192.168.1.200\共有\品質保証課\☆数値検査\数値検査記録.accdb"

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


def _normalize_date_for_match(value):
    """
    スプレッドシート／Access の指示日を比較用に日付に正規化する。
    None/空は None。datetime は日付部分のみ、文字列は yyyy/mm/dd 等を解釈。
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        s = value[:10]
        for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
            try:
                return datetime.datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


# --- メイン処理 ---
try:
    # 認証情報を設定（環境変数から読み込み）
    scope = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(RESOLVED_GOOGLE_API_KEY_FILE, scope)
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

    # 表面処理品データ読込み、書込み AI3:AQ22
    cell_range = 'AI3:AQ22'
    data3 = sh_pic.get(cell_range)

    # AO列の数式を事前に取得して保持（AO列はインデックス6）
    ao_formulas = []
    for row_num in range(3, 23):  # 3行目から22行目まで
        ao_cell = sh_pic.acell(f'AO{row_num}', value_render_option='FORMULA')
        ao_formulas.append(ao_cell.value if ao_cell.value else '')

    # 空行を除外する前に、各行の実際の行番号を記録
    data3_with_row_nums = []
    actual_row_nums = []  # data3内のインデックス -> 実際のスプレッドシートの行番号
    for i, row in enumerate(data3):
        if row:  # 空行でない場合
            data3_with_row_nums.append(row)
            actual_row_nums.append(3 + i)  # 実際の行番号（3行目起点）

    # フィルタリング（9列目が'TRUE'でない行を保持）
    filtered_data = []
    row_mapping = []  # フィルタリング後のインデックス -> 実際のスプレッドシートの行番号
    for i, row in enumerate(data3_with_row_nums):
        if len(row) > 8 and row[8] != 'TRUE':
            filtered_data.append(row)
            row_mapping.append(actual_row_nums[i])  # 実際の行番号を記録

    if filtered_data:
        # AI列からAN列までを更新（AO列を除く、6列分）
        ai_to_an_data = [[row[j] if j < len(row) else '' for j in range(6)] for row in filtered_data]
        end_row = 3 + len(ai_to_an_data) - 1
        sh_pic.update(values=ai_to_an_data, range_name=f"AI3:AN{end_row}")

        # AO列の数式を復元（行番号を調整）
        ao_formulas_to_restore = []
        for new_idx, old_row_num in enumerate(row_mapping):
            # old_row_numは実際のスプレッドシートの行番号（3-22の範囲）
            # 新しい行番号 = 3 + new_idx
            new_row_num = 3 + new_idx

            # ao_formulasのインデックス = old_row_num - 3
            formula_idx = old_row_num - 3
            if 0 <= formula_idx < len(ao_formulas) and ao_formulas[formula_idx]:
                formula = ao_formulas[formula_idx]
                # 数式内の行番号を新しい行番号に置換（例: AN3 -> AN4など）
                updated_formula = formula.replace(f'AN{old_row_num}', f'AN{new_row_num}')
                ao_formulas_to_restore.append([updated_formula])
            else:
                ao_formulas_to_restore.append([''])

        if ao_formulas_to_restore:
            end_row = 3 + len(ao_formulas_to_restore) - 1
            sh_pic.update(values=ao_formulas_to_restore, range_name=f"AO3:AO{end_row}", value_input_option='USER_ENTERED')

        # AP列からAQ列までを更新
        ap_to_aq_data = [[row[7] if len(row) > 7 else '', row[8] if len(row) > 8 else ''] for row in filtered_data]
        if ap_to_aq_data:
            end_row = 3 + len(ap_to_aq_data) - 1
            sh_pic.update(values=ap_to_aq_data, range_name=f"AP3:AQ{end_row}")

        # AQ列の'FALSE'をFalseに変換
        cell_range = f'AQ3:AQ{3 + len(filtered_data) - 1}'
        data4 = sh_pic.get(cell_range)
        for row_index, row in enumerate(data4):
            for col_index, cell_value in enumerate(row):
                if cell_value == 'FALSE':
                    data4[row_index][col_index] = False
        sh_pic.update(values=data4, range_name=cell_range)

    # 在庫洗浄品・二次工程完了品
    cell_range = 'AQ24:AQ47'
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

    cell_range = 'AG3:AG57'
    data8 = sh_pic.get(cell_range)
    for row_index, row in enumerate(data8):
        for col_index, cell_value in enumerate(row):
            if cell_value == 'TRUE':
                data8[row_index][col_index] = False
            elif cell_value == 'FALSE':
                data8[row_index][col_index] = False
    sh_pic.update(values=data8, range_name=cell_range)

    cell_range = 'AF3:AF57'
    sh_pic.batch_clear([cell_range])
    for i in range(len(name_list)):
        sh_pic.update_cell(3 + i, 32, name_list[i])

    # 表面処理品データ_数式設定 AL3:AM22
    start_row, end_row = 3, 22
    al_formulas = [f"=XLOOKUP($AK{row},'製品マスタ'!$B:$B,'製品マスタ'!$C:$C,\"\",FALSE)" for row in range(start_row, end_row + 1)]
    am_formulas = [f"=XLOOKUP($AK{row},'製品マスタ'!$B:$B,'製品マスタ'!$A:$A,\"\",FALSE)" for row in range(start_row, end_row + 1)]

    # 数式をセル範囲に入力
    def set_formulas_in_batches(sh_pic, start_cell, formulas):
        for i in range(0, len(formulas), 5):  # 5件ずつ書き込み（API制限対策）
            end_row = start_row + i + len(formulas[i:i+5]) - 1
            cell_range = f"{start_cell}{start_row + i}:{start_cell}{end_row}"
            formulas_batch = [[formula] for formula in formulas[i:i+5]]
            sh_pic.update(range_name=cell_range, values=formulas_batch, value_input_option='USER_ENTERED')
            time.sleep(1)  # API呼び出し制限を避けるためのウェイト

    # AJ列とAK列の数式をバッチ処理で入力
    set_formulas_in_batches(sh_pic, 'AL', al_formulas)
    set_formulas_in_batches(sh_pic, 'AM', am_formulas)

    # セル範囲AL24:AL47のデータをクリア
    cell_range_clear = 'AP24:AP47'
    empty_data = [[""] * len(data5[0]) for _ in range(len(data5))]
    sh_pic.update(values=empty_data, range_name=cell_range_clear)

    # F2:F36のデータをコピー
    data_f = sh_next.get("F2:F36")
    sh_pic.update(range_name="F2:F36", values=data_f, value_input_option="USER_ENTERED")

    # T2:T36のデータをコピー
    data_t = sh_next.get("T2:T36")
    sh_pic.update(range_name="T2:T36", values=data_t, value_input_option="USER_ENTERED")

    # 列の表示形式を日付（yyyy/mm/dd）に設定
    date_format = {"numberFormat": {"type": "DATE", "pattern": "yyyy/mm/dd"}}
    date_columns = ["Z:Z", "AD:AD", "AE:AE", "AJ:AJ", "AN:AN", "AO:AO"]
    for col_range in date_columns:
        sh_pic.format(col_range, date_format)

    # --- データ Sheet A130:C250 から検査員名を取得し I 列に書き込む ---
    cell_range_inspector = "A130:C250"
    data_inspector = sh_data.get(cell_range_inspector)
    rows_with_row_num = []
    for i, row in enumerate(data_inspector):
        sheet_row_num = 130 + i
        a = str(row[0]).strip() if row and len(row) > 0 and row[0] is not None else ""
        b = str(row[1]).strip() if row and len(row) > 1 and row[1] is not None else ""
        c = str(row[2]).strip() if row and len(row) > 2 and row[2] is not None else ""
        rows_with_row_num.append((sheet_row_num, a, b, c))

    conn_str_numerical = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={NUMERICAL_INSPECTION_DB_PATH}"
    )
    conn_numerical = pyo.connect(conn_str_numerical)
    cur_numerical = conn_numerical.cursor()

    cur_numerical.execute("SELECT 号機, 指示日, 品番, 生産ロットID FROM t_現品票履歴")
    fuken_hist = cur_numerical.fetchall()

    cur_numerical.execute("SELECT 生産ロットID, 検査員ID FROM t_数値検査記録")
    inspection_rec = cur_numerical.fetchall()

    cur_numerical.execute("SELECT 検査員ID, 検査員名 FROM t_数値検査員マスタ")
    inspector_master = cur_numerical.fetchall()

    cur_numerical.close()
    conn_numerical.close()

    # (号機, 指示日(date), 品番) -> 生産ロットID
    fuken_by_key = {}
    # 号機が空の場合用: (指示日(date), 品番) -> 生産ロットID（先頭1件）
    fuken_by_date_hinban = {}
    for r in fuken_hist:
        goki, shiji, hinban = r[0], r[1], r[2]
        lot_id = r[3]
        d = _normalize_date_for_match(shiji)
        key = (str(goki).strip() if goki else "", d, str(hinban).strip() if hinban else "")
        if key not in fuken_by_key:
            fuken_by_key[key] = lot_id
        key2 = (d, str(hinban).strip() if hinban else "")
        if key2 not in fuken_by_date_hinban:
            fuken_by_date_hinban[key2] = lot_id

    lot_to_inspector_id = {r[0]: r[1] for r in inspection_rec if r[0] is not None}

    inspector_id_to_name = {}
    for r in inspector_master:
        iid, name = r[0], r[1]
        if iid is not None:
            inspector_id_to_name[iid] = name if name else ""

    i_column_values = []
    for sheet_row_num, goki, shiji_str, hinban in rows_with_row_num:
        inspector_name = ""
        target_date = _normalize_date_for_match(shiji_str)
        if goki:
            # 号機あり: 号機・指示日・品番で照合
            key = (goki, target_date, hinban)
            if target_date is not None and key in fuken_by_key:
                lot_id = fuken_by_key[key]
                insp_id = lot_to_inspector_id.get(lot_id)
                if insp_id is not None:
                    inspector_name = inspector_id_to_name.get(insp_id, "") or ""
        else:
            # 号機が空: 指示日・品番のみで照合し、先頭1件の検査員IDを取得
            if target_date is not None and hinban:
                key2 = (target_date, hinban)
                if key2 in fuken_by_date_hinban:
                    lot_id = fuken_by_date_hinban[key2]
                    insp_id = lot_to_inspector_id.get(lot_id)
                    if insp_id is not None:
                        inspector_name = inspector_id_to_name.get(insp_id, "") or ""
        i_column_values.append([inspector_name])

    if i_column_values:
        sh_data.update(values=i_column_values, range_name="I130:I250")

    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise