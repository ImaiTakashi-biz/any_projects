import os
import glob
import win32print
import win32ui
import openpyxl
import smtplib
from email.mime.text import MIMEText
import traceback
import sys
import datetime
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
    # ロボパット用
    wb_rob = openpyxl.load_workbook(r"\\192.168.1.200\共有\製造課\ロボパット\ロボパット用.xlsx", data_only=True)
    ws_rob = wb_rob["Data"]
    ws_link = wb_rob["品番リンク"]

    # 翌日セット品番　取得
    list_rob = []
    for col in range(2, 12):
        value1 = ws_rob.cell(12, col).value
        list_rob.append(value1)
    wb_rob.close()

    print("取得した品番リスト:", list_rob)

    # PDFファイルの検索と印刷
    pdf_folder = r"\\192.168.1.200\共有\製造課\加工工程管理表、プログラム\5加工図面"
    printer_name = "iR-ADV C5735"  # 使用するプリンター名を指定

    try:
        # プリンターを設定
        printer = win32print.OpenPrinter(printer_name)
        print(f"プリンター '{printer_name}' を設定しました。")
    except Exception as e:
        print(f"プリンターの設定に失敗しました: {e}")
        # ここでエラー通知関数を呼び出す
        send_error_email(traceback.format_exc())
        sys.exit(1)

    for item in list_rob:
        if item is None:  # None に遭遇したらループ終了
            print("None に遭遇したためループを終了します。")
            break

        try:
            # フォルダー内で品番と一致するPDFファイルを検索
            search_pattern = os.path.join(pdf_folder, f"{item}*.pdf")
            pdf_files = glob.glob(search_pattern)

            if not pdf_files:
                print(f"品番 '{item}' に一致するPDFファイルが見つかりませんでした。")
                continue

            # 一致するPDFファイルを順次印刷
            for pdf_file in pdf_files:
                try:
                    print(f"印刷中: {pdf_file}")
                    # プリンターに直接印刷ジョブを送信
                    hprinter = win32print.OpenPrinter(printer_name)
                    try:
                        job = win32print.StartDocPrinter(hprinter, 1, ("PDF Job", None, "RAW"))
                        win32print.StartPagePrinter(hprinter)
                        with open(pdf_file, "rb") as pdf_file_content:
                            win32print.WritePrinter(hprinter, pdf_file_content.read())
                        win32print.EndPagePrinter(hprinter)
                        win32print.EndDocPrinter(hprinter)
                    finally:
                        win32print.ClosePrinter(hprinter)
                except Exception as e:
                    print(f"印刷中にエラーが発生しました: {e}")
                    # ここでエラー通知関数を呼び出す
                    send_error_email(f"印刷ジョブの送信中にエラーが発生しました。\nファイル: {pdf_file}\nエラー詳細:\n{traceback.format_exc()}")
        except Exception as e:
            print(f"品番 '{item}' の処理中にエラーが発生しました: {e}")
            # ここでエラー通知関数を呼び出す
            send_error_email(f"品番の処理中にエラーが発生しました。\n品番: {item}\nエラー詳細:\n{traceback.format_exc()}")

    print("処理が完了しました。")
    
except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise