import requests
import time
import os
from pathlib import Path
import io
import datetime
import smtplib
from email.mime.text import MIMEText
import traceback
import sys
import hashlib
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()

# --- メール通知用の設定 ---
# 通知アカウント
# ・takada@araiseimitsu.onmicrosoft.com
# ・imai@araiseimitsu.onmicrosoft.com
# ・n.kizaki@araiseimitsu.onmicrosoft.com
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

# ARAICHAT API設定
ARAICHAT_BASE_URL = os.getenv("ARAICHAT_BASE_URL", "https://araichat-966672454924.asia-northeast1.run.app/")
ARAICHAT_API_KEY = os.getenv("ARAICHAT_API_KEY")
ARAICHAT_ROOM_ID = os.getenv("ARAICHAT_ROOM_ID")

# Google Drive設定
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_FILE", "aptest-384703-24764f69b34f.json")
GOOGLE_DRIVE_SCOPES = [
    'https://www.googleapis.com/auth/drive'  # フルアクセス（読み取り、書き込み、削除）
]

# 送信対象の設定（フォルダ対応版）
# 単一ファイル指定（既存）
target_google_drive_file_id = "1Sdqhu6zG8LhzILklNt_TvNp1ySjRFR-G"

# フォルダ指定（新規追加）
# フォルダリンク：https://drive.google.com/drive/folders/1abr7ab8lhHcbapMr9hVqOQAAnaG2OEcU?usp=sharing
target_google_drive_folder_id = "1xOWQuGjzeaadLpybmCg93e-89O9Bu3nN"

# HTMLファイル専用フィルター設定
file_filter_config = {
    'extensions': ['.html', '.htm'],  # HTMLファイルのみ対象
    'max_size_mb': 25,  # 最大ファイルサイズ（MB）
    'exclude_patterns': [r'~\$.*', r'\.tmp$']  # 除外パターン
}

# 動作モード設定
USE_FOLDER_MODE = True  # True: フォルダモード, False: 単一ファイルモード

# 削除設定
DELETE_AFTER_UPLOAD = False  # True: 配信成功後にファイル削除, False: 削除しない
DELETE_LOCAL_CACHE = False   # True: ローカルキャッシュも削除, False: キャッシュ保持

def get_google_drive_service():
    """
    Google Drive APIサービスオブジェクトを取得

    Returns:
        googleapiclient.discovery.Resource: Google Drive APIサービス
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        service_account_file = os.path.join(base_dir, GOOGLE_SERVICE_ACCOUNT_FILE)

        if not os.path.exists(service_account_file):
            raise FileNotFoundError(f"Google サービスアカウントファイルが見つかりません: {service_account_file}")

        credentials = Credentials.from_service_account_file(
            service_account_file,
            scopes=GOOGLE_DRIVE_SCOPES
        )

        service = build('drive', 'v3', credentials=credentials)
        print("Google Drive APIサービスの初期化完了")
        return service

    except Exception as e:
        error_msg = f"Google Drive APIサービスの初期化エラー: {str(e)}"
        print(error_msg)
        send_error_email(f"Google Drive API初期化エラー:\n{error_msg}")
        return None

def download_file_from_google_drive(file_id):
    """
    Google Driveからファイルをダウンロードしてバイトデータとファイル名を取得

    Args:
        file_id (str): Google DriveのファイルID

    Returns:
        tuple: (file_data: bytes, file_name: str) 成功時、(None, None) 失敗時
    """
    try:
        service = get_google_drive_service()
        if not service:
            return None, None

        # ファイルのメタデータを取得
        print(f"Google Driveファイル情報取得中: {file_id}")
        file_metadata = service.files().get(fileId=file_id).execute()
        file_name = file_metadata.get('name', 'unknown_file')
        file_size = file_metadata.get('size', 'Unknown')
        mime_type = file_metadata.get('mimeType', 'Unknown')

        print(f"ファイル情報:")
        print(f"  名前: {file_name}")
        print(f"  サイズ: {file_size} bytes")
        print(f"  MIMEタイプ: {mime_type}")

        # ファイルの内容をダウンロード
        print("ファイルダウンロード開始...")
        request = service.files().get_media(fileId=file_id)
        file_data = io.BytesIO()
        downloader = MediaIoBaseDownload(file_data, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                print(f"ダウンロード進行状況: {int(status.progress() * 100)}%")

        file_bytes = file_data.getvalue()
        print(f"ダウンロード完了: {len(file_bytes)} bytes取得")

        return file_bytes, file_name

    except Exception as e:
        error_msg = f"Google Driveファイルダウンロードエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"Google Driveファイルダウンロードエラー:\n{error_msg}")
        return None, None

def list_files_in_folder(folder_id):
    """
    Google Driveフォルダ内のファイル一覧を取得

    Args:
        folder_id (str): Google DriveのフォルダID

    Returns:
        list: ファイル情報のリスト
    """
    try:
        service = get_google_drive_service()
        if not service:
            return []

        print(f"フォルダ内ファイル検索中: {folder_id}")

        # フォルダ内のファイルを検索（サブフォルダは除外）
        query = f"'{folder_id}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, size, modifiedTime, webViewLink)",
            orderBy="name"
        ).execute()

        files = results.get('files', [])
        print(f"フォルダ内のファイル数: {len(files)}")

        for i, file in enumerate(files, 1):
            size_mb = int(file.get('size', 0)) / (1024 * 1024) if file.get('size') else 0
            print(f"  {i:2d}. {file['name']} (ID: {file['id']}, サイズ: {size_mb:.1f}MB)")

        return files

    except Exception as e:
        error_msg = f"フォルダ内ファイル取得エラー: {str(e)}"
        print(error_msg)
        send_error_email(f"Google Driveフォルダアクセスエラー:\n{error_msg}")
        return []

def apply_file_filter(files, file_filter):
    """
    ファイルリストにフィルターを適用

    Args:
        files (list): ファイル情報のリスト
        file_filter (dict): フィルター条件

    Returns:
        list: フィルター適用後のファイルリスト
    """
    if not file_filter:
        return files

    filtered_files = []

    for file_info in files:
        file_name = file_info['name']
        file_size = int(file_info.get('size', 0))

        # 除外パターンチェック
        if 'exclude_patterns' in file_filter:
            import re
            skip_file = False
            for pattern in file_filter['exclude_patterns']:
                if re.search(pattern, file_name):
                    print(f"スキップ: {file_name} (除外パターンに一致)")
                    skip_file = True
                    break
            if skip_file:
                continue

        # 拡張子フィルター
        if 'extensions' in file_filter:
            extensions = file_filter['extensions']
            if not any(file_name.lower().endswith(ext.lower()) for ext in extensions):
                print(f"スキップ: {file_name} (対象外拡張子)")
                continue

        # ファイルサイズフィルター
        if 'max_size_mb' in file_filter:
            max_size_bytes = file_filter['max_size_mb'] * 1024 * 1024
            if file_size > max_size_bytes:
                print(f"スキップ: {file_name} (サイズ制限超過: {file_size / 1024 / 1024:.1f}MB)")
                continue

        filtered_files.append(file_info)

    print(f"フィルター適用後: {len(filtered_files)}件のHTMLファイルが対象")
    return filtered_files

# 送信履歴管理（重複防止用）
SENT_CACHE_FILE = Path(__file__).parent / "araichat_sent_cache.json"
CACHE_TTL_HOURS = 24  # キャッシュ保持期間（時間）

def calculate_file_digest(file_data, file_name):
    """
    ファイルの内容から一意なハッシュ値を計算
    
    Args:
        file_data (bytes): ファイルデータ
        file_name (str): ファイル名
    
    Returns:
        str: SHA256ハッシュ値
    """
    # ファイル名と内容を組み合わせてハッシュ化
    combined = f"{file_name}:{len(file_data)}:".encode('utf-8') + file_data
    return hashlib.sha256(combined).hexdigest()

def load_sent_cache():
    """
    送信履歴キャッシュを読み込む
    
    Returns:
        dict: 送信履歴（キー: digest, 値: {'file_name': str, 'sent_time': int}）
    """
    if SENT_CACHE_FILE.exists():
        try:
            with open(SENT_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                # 古いエントリを削除（TTL超過）
                current_time = int(time.time())
                ttl_seconds = CACHE_TTL_HOURS * 3600
                return {
                    k: v for k, v in cache.items()
                    if current_time - v.get('sent_time', 0) < ttl_seconds
                }
        except Exception as e:
            print(f"⚠️ 送信履歴キャッシュ読み込みエラー: {e}")
            return {}
    return {}

def save_sent_cache(cache):
    """
    送信履歴キャッシュを保存
    
    Args:
        cache (dict): 送信履歴
    """
    try:
        SENT_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SENT_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 送信履歴キャッシュ保存エラー: {e}")

def check_already_sent(file_digest, file_name):
    """
    ファイルが既に送信済みかチェック
    
    Args:
        file_digest (str): ファイルのハッシュ値
        file_name (str): ファイル名
    
    Returns:
        bool: 送信済みの場合はTrue
    """
    cache = load_sent_cache()
    
    if file_digest in cache:
        sent_info = cache[file_digest]
        sent_time = datetime.datetime.fromtimestamp(sent_info.get('sent_time', 0))
        print(f"⚠️ 既に送信済みとしてスキップ: {file_name}")
        print(f"   前回送信日時: {sent_time.strftime('%Y/%m/%d %H:%M:%S')}")
        print(f"   ハッシュ値: {file_digest[:16]}...")
        return True
    return False

def mark_as_sent(file_digest, file_name):
    """
    ファイルを送信済みとしてマーク
    
    Args:
        file_digest (str): ファイルのハッシュ値
        file_name (str): ファイル名
    """
    cache = load_sent_cache()
    cache[file_digest] = {
        'file_name': file_name,
        'sent_time': int(time.time())
    }
    save_sent_cache(cache)

def send_file_to_araichat(file_data, file_name):
    """
    ファイルデータをARAICHATに送信（リトライ処理＋重複防止付き）

    Args:
        file_data (bytes): アップロードするファイルのバイトデータ
        file_name (str): ファイル名

    Returns:
        bool: 成功時はTrue、失敗時はFalse（既に送信済みの場合はTrue）
    """
    # ファイルのハッシュ値を計算（重複チェック用）
    file_digest = calculate_file_digest(file_data, file_name)
    
    # 既に送信済みかチェック
    if check_already_sent(file_digest, file_name):
        print(f"✅ 既に送信済みのためスキップ: {file_name}")
        return True
    
    # 環境変数の確認
    print(f"=== ARAICHAT送信設定確認 ===")
    print(f"BASE_URL: {ARAICHAT_BASE_URL}")
    print(f"ROOM_ID: {ARAICHAT_ROOM_ID}")
    print(f"API_KEY: {'設定済み' if ARAICHAT_API_KEY else '未設定'}")
    
    if not ARAICHAT_API_KEY:
        error_msg = "ARAICHAT_API_KEY が設定されていません"
        print(f"❌ {error_msg}")
        send_error_email(f"ARAICHAT設定エラー:\n{error_msg}")
        return False
        
    if not ARAICHAT_ROOM_ID:
        error_msg = "ARAICHAT_ROOM_ID が設定されていません"
        print(f"❌ {error_msg}")
        send_error_email(f"ARAICHAT設定エラー:\n{error_msg}")
        return False

    # URLの末尾スラッシュを調整
    base_url = ARAICHAT_BASE_URL.rstrip("/")
    url = f"{base_url}/api/integrations/send/{ARAICHAT_ROOM_ID}"
    headers = {
        "Authorization": f"Bearer {ARAICHAT_API_KEY}",
        # 冪等性キー（サーバー側が対応している場合、重複防止に有効）
        "Idempotency-Key": f"gdrive:{file_digest[:32]}"
    }
    data = {"text": f"Google Driveからファイルを送信: {file_name}"}
    
    print(f"送信URL: {url}")
    print(f"ファイルサイズ: {len(file_data)} bytes")
    print(f"ファイルハッシュ: {file_digest[:16]}...")
    print(f"ARAICHATへファイル送信開始: {file_name}")
    
    # リトライ設定
    max_retries = 3
    backoff_seconds = 2  # 初期待機時間（指数バックオフ: 2秒、4秒、8秒）
    timeout_connect = 5   # 接続タイムアウト（秒）
    timeout_read = 180    # 読み取りタイムアウト（秒）
    
    for attempt in range(1, max_retries + 1):
        try:
            # ファイルデータをBytesIOで作成（毎回新規作成が必要）
            files = [("files", (file_name, io.BytesIO(file_data), "text/html"))]
            
            # タイムアウトを個別に設定（接続タイムアウトと読み取りタイムアウト）
            timeout = (timeout_connect, timeout_read)
            
            if attempt > 1:
                wait_time = backoff_seconds * (2 ** (attempt - 2))
                print(f"⏳ リトライ {attempt}/{max_retries}（{wait_time}秒待機後）...")
                time.sleep(wait_time)
            
            start_time = time.time()
            resp = requests.post(url, headers=headers, data=data, files=files, timeout=timeout)
            elapsed_time = time.time() - start_time
            
            # レスポンス詳細をログ出力
            print(f"レスポンスステータス: {resp.status_code}（処理時間: {elapsed_time:.2f}秒）")
            print(f"レスポンスヘッダー: {dict(resp.headers)}")
            
            try:
                response_text = resp.text
                print(f"レスポンス内容: {response_text}")
            except:
                print("レスポンス内容の取得に失敗")
            
            resp.raise_for_status()
            result = resp.json()
            print(f"✅ ARAICHATへファイル送信成功: {file_name}")
            print(f"送信結果: {result}")
            
            # 送信成功時のみ送信履歴に記録（重複防止）
            mark_as_sent(file_digest, file_name)
            print(f"送信履歴を記録しました: {file_name}")
            
            return True
            
        except requests.exceptions.Timeout as e:
            elapsed_time = time.time() - start_time if 'start_time' in locals() else 0
            if attempt < max_retries:
                print(f"⏱️ タイムアウト発生（{elapsed_time:.2f}秒）: {e} - リトライします")
                continue
            else:
                # 全てのリトライが失敗した場合のみエラー通知
                error_msg = f"ARAICHAT送信タイムアウトエラー（{max_retries}回試行後）: {file_name}"
                print(f"❌ {error_msg}")
                send_error_email(f"ARAICHAT送信エラー:\n{error_msg}")
                return False
                
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            response_text = e.response.text if e.response else ""
            
            # 一時的なサーバーエラー（5xx）の場合はリトライ
            if status_code and 500 <= status_code < 600 and attempt < max_retries:
                print(f"⚠️ HTTP {status_code} エラー: {e} - リトライします")
                continue
            else:
                # クライアントエラー（4xx）や最終リトライ失敗時はエラー通知
                error_msg = f"ARAICHAT送信HTTPエラー: {e}\nステータスコード: {status_code}\nレスポンス: {response_text}"
                print(f"❌ {error_msg}")
                send_error_email(f"ARAICHAT送信エラー:\n{error_msg}")
                return False
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                print(f"⚠️ ネットワークエラー: {e} - リトライします")
                continue
            else:
                # 全てのリトライが失敗した場合のみエラー通知
                error_msg = f"ARAICHAT送信リクエストエラー（{max_retries}回試行後）: {str(e)}"
                print(f"❌ {error_msg}")
                send_error_email(f"ARAICHAT送信エラー:\n{error_msg}")
                return False
                
        except Exception as e:
            # 予期しないエラーは即座に通知
            error_msg = f"ARAICHAT送信予期しないエラー: {str(e)}"
            print(f"❌ {error_msg}")
            send_error_email(f"ARAICHAT送信エラー:\n{error_msg}")
            return False
    
    # このコードには到達しないはずだが、念のため
    error_msg = f"ARAICHAT送信失敗（全リトライ試行後）: {file_name}"
    print(f"❌ {error_msg}")
    send_error_email(f"ARAICHAT送信エラー:\n{error_msg}")
    return False

def delete_file_from_google_drive(file_id, file_name):
    """
    Google Driveからファイルを削除

    Args:
        file_id (str): Google DriveのファイルID
        file_name (str): ファイル名（ログ用）

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """
    try:
        service = get_google_drive_service()
        if not service:
            print(f"⚠️ Google Drive APIサービスの初期化に失敗: {file_name}")
            return False

        print(f"Google Driveからファイル削除開始: {file_name}")
        
        # ファイル削除実行
        service.files().delete(fileId=file_id).execute()
        print(f"✅ Google Driveファイル削除成功: {file_name}")
        return True

    except Exception as e:
        error_msg = f"Google Driveファイル削除失敗: {file_name} - {str(e)}"
        print(f"⚠️ {error_msg}")
        # 削除エラーは重大ではないため、メール通知はスキップ
        return False

def send_folder_files_to_araichat(folder_id, file_filter=None):
    """
    Google Driveフォルダ内のファイルをARAICHATに送信

    Args:
        folder_id (str): Google DriveのフォルダID
        file_filter (dict): ファイルフィルター条件

    Returns:
        dict: 送信結果の詳細
    """
    try:
        print(f"\n=== フォルダ内HTMLファイル送信開始 ===")
        print(f"フォルダID: {folder_id}")
        print(f"削除モード: {'有効' if DELETE_AFTER_UPLOAD else '無効（ファイル保持）'}")

        # フォルダ内ファイル一覧取得
        files = list_files_in_folder(folder_id)
        if not files:
            print("フォルダ内にファイルが見つかりません")
            return {'success': False, 'sent_files': [], 'failed_files': [], 'deleted_files': [], 'total_files': 0}

        # ファイルフィルタリング
        filtered_files = apply_file_filter(files, file_filter)
        if not filtered_files:
            print("フィルター条件に一致するHTMLファイルがありません")
            return {'success': False, 'sent_files': [], 'failed_files': [], 'deleted_files': [], 'total_files': 0}

        sent_files = []
        failed_files = []
        deleted_files = []

        print(f"\n送信対象HTMLファイル: {len(filtered_files)}件")
        print("=" * 50)

        for i, file_info in enumerate(filtered_files, 1):
            file_id = file_info['id']
            file_name = file_info['name']

            print(f"\n[{i}/{len(filtered_files)}] 送信中: {file_name}")
            
            # ファイルをダウンロード
            file_data, _ = download_file_from_google_drive(file_id)
            if not file_data:
                failed_files.append(file_name)
                print(f"❌ {file_name} ダウンロード失敗 - ファイル送信をスキップ")
                continue

            # ARAICHATに送信
            result = send_file_to_araichat(file_data, file_name)

            if result:
                sent_files.append(file_name)
                print(f"✅ {file_name} 送信完了")
                
                # 送信成功時の処理
                if DELETE_AFTER_UPLOAD:
                    print(f"配信成功により削除実行: {file_name}")
                    if delete_file_from_google_drive(file_id, file_name):
                        deleted_files.append(file_name)
                    else:
                        print(f"⚠️ ファイル削除失敗（手動で削除してください）: {file_name}")
                else:
                    print(f"✅ {file_name} 送信完了 - ファイル保持")
            else:
                failed_files.append(file_name)
                print(f"❌ {file_name} 送信失敗")

            # 送信間隔を空ける（API制限対策）
            if i < len(filtered_files):
                print("次のファイル送信まで2秒待機...")
                time.sleep(2)

        print(f"\n=== 送信結果 ===")
        print(f"成功: {len(sent_files)}件")
        print(f"失敗: {len(failed_files)}件")
        print(f"削除: {len(deleted_files)}件")
        print(f"合計: {len(filtered_files)}件")

        if sent_files:
            print("\n✅ 送信成功ファイル:")
            for file_name in sent_files:
                print(f"  - {file_name}")

        if deleted_files:
            print("\n🗑️ 削除完了ファイル:")
            for file_name in deleted_files:
                print(f"  - {file_name}")

        if failed_files:
            print("\n❌ 送信失敗ファイル:")
            for file_name in failed_files:
                print(f"  - {file_name}")

        return {
            'success': len(failed_files) == 0,
            'sent_files': sent_files,
            'failed_files': failed_files,
            'deleted_files': deleted_files,
            'total_files': len(filtered_files)
        }

    except Exception as e:
        error_msg = f"フォルダファイル送信エラー: {str(e)}"
        print(error_msg)
        send_error_email(f"ARAICHATフォルダ送信エラー:\n{error_msg}")
        return {'success': False, 'sent_files': [], 'failed_files': [], 'deleted_files': [], 'total_files': 0}

def send_file_to_araichat_single(file_id=None):
    """
    Google Driveから単一ファイルをARAICHATに送信

    Args:
        file_id (str, optional): Google DriveファイルID。指定されない場合はデフォルト値を使用

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """
    # ファイルIDの決定
    actual_file_id = file_id if file_id else target_google_drive_file_id

    try:
        print("=== Google Driveからファイル取得 ===")
        print(f"ファイルID: {actual_file_id}")

        file_data, file_name = download_file_from_google_drive(actual_file_id)
        if not file_data:
            print("Google Driveからのファイル取得に失敗しました")
            return False

        print("\n=== ARAICHATファイル送信 ===")
        success = send_file_to_araichat(file_data, file_name)

        if success:
            print("\n✅ ファイル送信が正常に完了しました")
            
            # 送信成功時の処理（単一ファイルモード）
            if DELETE_AFTER_UPLOAD and not USE_FOLDER_MODE:
                print(f"\n=== ファイル削除処理 ===")
                print(f"配信成功により削除実行: {file_name}")
                delete_file_from_google_drive(actual_file_id, file_name)
            else:
                print(f"✅ ファイル送信完了 - ファイル保持: {file_name}")
        else:
            print("\n❌ ファイル送信に失敗しました")

        return success

    except Exception as e:
        error_msg = f"予期しないエラーが発生しました: {str(e)}"
        print(error_msg)
        send_error_email(f"ARAICHATファイル送信エラー:\n{error_msg}")
        return False

# --- メイン処理 ---
try:
    if __name__ == "__main__":
        print("=== ARAICHAT ファイル送信スクリプト (フォルダ対応版) ===")
        
        # 環境変数の確認
        print(f"\n=== 環境変数設定確認 ===")
        print(f"ARAICHAT_BASE_URL: {ARAICHAT_BASE_URL}")
        print(f"ARAICHAT_ROOM_ID: {ARAICHAT_ROOM_ID}")
        print(f"ARAICHAT_API_KEY: {'設定済み' if ARAICHAT_API_KEY else '❌ 未設定'}")
        print(f"GOOGLE_SERVICE_ACCOUNT_FILE: {GOOGLE_SERVICE_ACCOUNT_FILE}")
        print(f"削除モード: {'有効' if DELETE_AFTER_UPLOAD else '無効（ファイル保持）'}")
        
        # 必須環境変数のチェック
        missing_vars = []
        if not ARAICHAT_API_KEY:
            missing_vars.append("ARAICHAT_API_KEY")
        if not ARAICHAT_ROOM_ID:
            missing_vars.append("ARAICHAT_ROOM_ID")
        if not EMAIL_SENDER:
            missing_vars.append("EMAIL_SENDER")
        if not EMAIL_PASSWORD:
            missing_vars.append("EMAIL_PASSWORD")
        if not EMAIL_RECEIVERS:
            missing_vars.append("EMAIL_RECEIVERS")
            
        if missing_vars:
            error_msg = f"以下の環境変数が設定されていません: {', '.join(missing_vars)}"
            print(f"\n❌ {error_msg}")
            print("⚠️ .envファイルを作成して必要な環境変数を設定してください")
            raise ValueError(error_msg)

        if USE_FOLDER_MODE:
            print(f"\n動作モード: フォルダ一括送信（HTMLファイル専用）")
            print(f"送信対象フォルダID: {target_google_drive_folder_id}")
            print(f"フィルター設定: {file_filter_config}")
            print("=" * 60)

            result = send_folder_files_to_araichat(
                target_google_drive_folder_id,
                file_filter_config
            )

            if result['success']:
                print(f"\n🎉 フォルダ内HTMLファイル送信完了: {len(result['sent_files'])}件")
                if DELETE_AFTER_UPLOAD:
                    print(f"🗑️ ファイル削除完了: {len(result['deleted_files'])}件")
                else:
                    print(f"📁 ファイル保持: {len(result['sent_files'])}件")
            elif result['total_files'] > 0:
                print(f"\n⚠️ 一部送信失敗: 成功{len(result['sent_files'])}件, 失敗{len(result['failed_files'])}件")
                if DELETE_AFTER_UPLOAD:
                    print(f"🗑️ ファイル削除完了: {len(result['deleted_files'])}件")
                else:
                    print(f"📁 ファイル保持: {len(result['sent_files'])}件")
            else:
                print(f"\n❌ 送信対象HTMLファイルなし")

        else:
            print(f"\n動作モード: 単一ファイル送信")
            print(f"送信対象ファイルID: {target_google_drive_file_id}")
            print("=" * 60)

            print(f"\n=== 直接ファイルダウンロード開始 ===")
            result = send_file_to_araichat_single(target_google_drive_file_id)

            if result:
                print("\n✅ ファイル送信が正常に完了しました")
                if DELETE_AFTER_UPLOAD:
                    print("🗑️ 送信成功によりファイル削除を実行しました")
                else:
                    print("📁 ファイルは保持されました")
            else:
                print("\n❌ ファイル送信に失敗しました")

        print("\n🎉 スクリプトが正常に完了しました！")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise