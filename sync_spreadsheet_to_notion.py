import os
import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
from notion_client import Client
import smtplib
from email.mime.text import MIMEText

load_dotenv()

# Google API設定
# 環境変数からサービスアカウントキーファイル名を取得
GOOGLE_SERVICE_ACCOUNT_KEY_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_FILE")

# Notion API設定
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = "26c37bffefe88036b55dcb86a9342cc6"

# メール通知設定
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVERS = os.getenv("EMAIL_RECEIVERS", "").split(",") if os.getenv("EMAIL_RECEIVERS") else []
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Google API設定の存在確認
if not GOOGLE_SERVICE_ACCOUNT_KEY_FILE:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY_FILE が .env ファイルに設定されていません")

# Notion API設定の存在確認
if not NOTION_API_TOKEN:
    raise ValueError("NOTION_API_TOKEN が .env ファイルに設定されていません")

# メール通知設定の存在確認 (送信者とパスワードは必須)
if not EMAIL_SENDER or not EMAIL_PASSWORD:
    print("警告: メール通知設定 (EMAIL_SENDER, EMAIL_PASSWORD) が不完全です。エラー通知は送信されません。")

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

# エラー通知メール送信関数
def send_error_notification(subject, body):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVERS:
        print("エラー通知メールは送信されませんでした: メール設定が不完全です。")
        return

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = ', '.join(EMAIL_RECEIVERS)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"エラー通知メールを送信しました: {subject}")
    except Exception as e:
        print(f"エラー通知メールの送信中にエラーが発生しました: {e}")

# Google API認証ファイルパスを解決
RESOLVED_GOOGLE_API_KEY_FILE = resolve_google_api_key_file(GOOGLE_SERVICE_ACCOUNT_KEY_FILE)

# スプレッドシートの認証スコープ
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# 認証情報のロードとgspreadクライアントの認証
client = gspread.service_account(filename=RESOLVED_GOOGLE_API_KEY_FILE, scopes=SCOPES)

# Notionクライアントの初期化
notion = Client(auth=NOTION_API_TOKEN)

# スプレッドシートIDとシート名
SPREADSHEET_ID = '1jk__1dhS_8jTDah63MH3vKgn2MbSpx4t5wZSrwSYkHo'
SHEET_NAME = 'データ'
RANGE_NAME = 'W3:AB71'

try:
    # スプレッドシートを開く
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)

    # 指定範囲のデータを取得
    # get_values()はリストのリストを返す
    data = worksheet.get_values(RANGE_NAME)

    # AB列（Wから数えて6番目の列、インデックスは5）が空白ではない行をフィルタリング
    filtered_data = []
    for row in data:
        # 行がAB列までデータを持っているか確認し、AB列が空白でないことを確認
        if len(row) > 5 and row[5].strip() != '':
            # Z列（インデックス3）とAA列（インデックス4）を削除
            # 後ろから削除するとインデックスがずれない
            del row[4] # AA列を削除
            del row[3] # Z列を削除

            # インデックスを並び替える (W, X, Y, AB -> X, Y, W, AB)
            # 現在のrowは [W, X, Y, AB] の状態
            # 新しい並び順は [row[1], row[2], row[0], row[3]]
            reordered_row = [row[1], row[2], row[0], row[3]]

            # 取得日を追加
            # Notionの日付形式に合わせてYYYY-MM-DDに変換
            reordered_row.append("2025-09-12")

            filtered_data.append(reordered_row)

    # フィルタリングされたデータをプリント表示
    print("フィルタリングされたデータ:")
    for row in filtered_data:
        print(row)

    # Notionにデータを書き込む
    print("\nNotionにデータを書き込み中...")
    total_items = len(filtered_data)
    bar_length = 20 # 進捗バーの長さ

    for i, item in enumerate(filtered_data):
        hinban = item[0] # X列
        hinmei = item[1] # Y列
        kyakusaki_mei = item[2] # W列
        kensain = item[3] # AB列
        kensabi = item[4] # 取得日

        # 進捗の計算と表示
        progress = (i + 1) / total_items
        arrow = '=' * int(round(progress * bar_length) - 1) + '>'
        spaces = ' ' * (bar_length - len(arrow))
        # 緑色に設定し、バーとパーセンテージを表示、最後に色をリセット
        print(f"\r\033[92m{i+1}/{total_items}\033[0m] \033[92m{int(progress * 100)}%\033[0m [\033[92m{arrow}{spaces}\033[0m]", end='')

        try:
            notion.pages.create(
                parent={"database_id": NOTION_DATABASE_ID},
                properties={
                    "品番": {
                        "title": [
                            {
                                "text": {
                                    "content": hinban
                                }
                            }
                        ]
                    },
                    "品名": {
                        "rich_text": [
                            {
                                "text": {
                                    "content": hinmei
                                }
                            }
                        ]
                    },
                    "客先名": {
                        "rich_text": [
                            {
                                "text": {
                                    "content": kyakusaki_mei
                                }
                            }
                        ]
                    },
                    "検査員": {
                        "rich_text": [
                            {
                                "text": {
                                    "content": kensain
                                }
                            }
                        ]
                    },
                    "検査日": {
                        "date": {
                            "start": kensabi
                        }
                    }
                }
            )
        except Exception as e:
            # エラー時は進捗バーの行を上書きしないように改行してから表示
            error_message = f"Notionへの書き込み中にエラーが発生しました (品番: {hinban}): {e}"
            print(f"\n{error_message}")
            send_error_notification(
                subject=f"Notion同期エラー: 品番 {hinban}",
                body=error_message
            )

    # 処理完了後に改行
    print()

except gspread.exceptions.SpreadsheetNotFound as e:
    error_message = f"エラー: スプレッドシートID '{SPREADSHEET_ID}' が見つかりません。詳細: {e}"
    print(error_message)
    send_error_notification(
        subject="Notion同期エラー: スプレッドシート見つからず",
        body=error_message
    )
except gspread.exceptions.WorksheetNotFound as e:
    error_message = f"エラー: シート名 '{SHEET_NAME}' が見つかりません。詳細: {e}"
    print(error_message)
    send_error_notification(
        subject="Notion同期エラー: シート見つからず",
        body=error_message
    )
except FileNotFoundError as e:
    error_message = f"エラー: {e}"
    print(error_message)
    send_error_notification(
        subject="Notion同期エラー: ファイル見つからず",
        body=error_message
    )
except Exception as e:
    error_message = f"予期せぬエラーが発生しました: {e}"
    print(error_message)
    send_error_notification(
        subject="Notion同期エラー: 予期せぬエラー",
        body=error_message
    )