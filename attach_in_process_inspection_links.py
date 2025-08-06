import openpyxl
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import shutil
import datetime
import smtplib
from email.mime.text import MIMEText
import traceback
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
    # ロボパット用
    wb_rob = openpyxl.load_workbook(r"\\192.168.1.200\共有\製造課\ロボパット\ロボパット用.xlsx", data_only=True)
    ws_rob = wb_rob["Data"]
    ws_link = wb_rob["品番リンク"]

    # 機番・品番　取得
    list_rob = []
    target_row = [9, 12]
    for col in range(2, 12):
        data = []
        for row in target_row:
            value1 = ws_rob.cell(row, col).value
            data.append(value1)
        list_rob.append(data)
    print(list_rob)

    # リンク取得
    last_row = ws_link.max_row
    for _ in range(0 + 1,last_row + 1):
        if ws_link.cell(_,1).value is not None:
            last_row1 = _

    list_link = []
    for item in list_rob:
        data = None  # デフォルトでNoneを設定
        for row in range(1, last_row1 + 1):
            if item[1] == ws_link.cell(row, 1).value:
                data = ws_link.cell(row, 2).value  # マッチする場合はデータを設定
                break  # マッチしたらループを終了
        list_link.append(data)

    # リスト結合
    data_list = []
    for rob, link in zip(list_rob, list_link):
        data_list.append([rob[0], rob[1], link])

    print(data_list)
    wb_rob.close()

    # 認証情報を設定
    scope = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('aptest-384703-24764f69b34f.json', scope)
    client = gspread.authorize(creds)

    # 稼働中工程内検査シート
    ws = client.open_by_key("149nerm5Gma8sdhn5m18nvsXdCNasvv_P9qSEDRJhKxs") #スプレッドシートのkey
    sh = ws.worksheet("シート1")

    values = sh.get_all_values()

    # データとセルの範囲を出力
    for i, row in enumerate(values):
        for j, val in enumerate(row):
            cell_address = gspread.utils.rowcol_to_a1(i + 1, j + 1 + 1)
            for data in data_list:
                if data[0] == val:
                    new_link = data[2]  # 新しいリンク
                    # リンクを更新
                    existing_formula = sh.acell(cell_address, value_render_option='FORMULA').value
                    if existing_formula is not None:
                        if existing_formula.startswith('=HYPERLINK'):
                            # 既存のリンクがある場合、リンクの先だけを更新する
                            existing_link = existing_formula.split('"')[1]
                            if existing_link is not None and new_link is not None:
                                updated_formula = existing_formula.replace(str(existing_link), new_link)
                                sh.update_acell(cell_address, updated_formula)
                            else:
                                print("Error: The existing link or new link is None.")
                        else:
                            # 既存のリンクがない場合、新しいリンクを挿入する
                            sh.update_acell(cell_address, f'=HYPERLINK("{new_link}", "〇")')
                    else:
                        print("Error: The existing formula is None.")

    # セット品加工図更新
    processed_files = set()  # 処理済みのファイル名を追跡するセット

    for data in data_list:
        if data[0] is not None:
            if data[0] in processed_files:
                print(f"Duplicate found, skipping: {data[0]}")
                continue  # 重複があればスキップ

            old_file_path = rf"\\192.168.1.200\共有\製造課\加工工程管理表、プログラム\5加工図面\{data[1]}.pdf"
            new_folder_path = r"\\192.168.1.200\共有\製造課\ロボパット\セット品加工図準備用"
            new_file_name = f"{data[0]}.pdf"

            if os.path.exists(old_file_path):
                # コピー先ディレクトリが存在しない場合は作成
                if not os.path.exists(new_folder_path):
                    os.makedirs(new_folder_path)

                # ファイルをコピーして新しいファイル名に変更
                shutil.copy(old_file_path, os.path.join(new_folder_path, os.path.basename(old_file_path)))
                new_file_path = os.path.join(new_folder_path, new_file_name)
                os.rename(os.path.join(new_folder_path, os.path.basename(old_file_path)), new_file_path)

                # 処理済みのファイル名をセットに追加
                processed_files.add(data[0])
            else:
                print(f"File does not exist: {old_file_path}")

    path_dir = r"\\192.168.1.200\共有\製造課\ロボパット\セット品加工図準備用"
    move_dir = r"G:\.shortcut-targets-by-id\1x2MWa8ZiLFuPcHdd9jIGYQjkqR877pDo\1加工図面"

    # リンクではなく、実際のディレクトリパスを使用
    if os.path.exists(move_dir):
        list_file_name = os.listdir(path_dir)

        for i_file_name in list_file_name:
            join_path = os.path.join(path_dir, i_file_name)
            move_path = os.path.join(move_dir, i_file_name)

            if os.path.isfile(join_path):
                shutil.move(join_path, move_path)
    else:
        print(f"Move directory does not exist: {move_dir}")
        
    print("完了しました。")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise