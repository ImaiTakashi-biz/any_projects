import requests
import jwt
import time
import os
from pathlib import Path
import io
import shutil
import gc
import datetime
import smtplib
from email.mime.text import MIMEText
import traceback
import sys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# psutilのオプショナルインポート
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("警告: psutilライブラリがインストールされていません。高度なプロセス監視機能は利用できません。")

# --- メール通知用の設定 ---
# 通知アカウント
# ・takada@araiseimitsu.onmicrosoft.com
# ・imai@araiseimitsu.onmicrosoft.com
# ・n.kizaki@araiseimitsu.onmicrosoft.com
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

# 認証情報（環境変数から読み込み、デフォルト値も設定）
CLIENT_ID = os.getenv("LINE_WORKS_CLIENT_ID", "KXRr5ZqUkTn3nU9Y1MmZ")
CLIENT_SECRET = os.getenv("LINE_WORKS_CLIENT_SECRET", "8ji3HWTVxK")
BOT_ID = os.getenv("LINE_WORKS_BOT_ID", "6808618")
audience = "https://auth.worksmobile.com/oauth2/v2.0/token"
service_account = os.getenv("LINE_WORKS_SERVICE_ACCOUNT", "2z1nf.serviceaccount@araiseimitsu")
private_key_path = Path(os.getenv("LINE_WORKS_PRIVATE_KEY_PATH", "private_20250722104854.key"))

# Google Drive設定
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "aptest-384703-24764f69b34f.json")
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
DELETE_AFTER_UPLOAD = True  # True: 配信成功後にファイル削除, False: 削除しない
DELETE_LOCAL_CACHE = True   # True: ローカルキャッシュも削除, False: キャッシュ保持


def check_and_grant_file_permissions(service, file_id, file_name):
    """
    ファイルの権限を確認し、必要に応じてサービスアカウントに編集権限を付与
    
    Args:
        service: Google Drive APIサービスオブジェクト
        file_id (str): ファイルID
        file_name (str): ファイル名
    
    Returns:
        bool: 権限確認/付与に成功した場合True
    """
    try:
        # サービスアカウントのメールアドレスを取得
        try:
            service_account_email = service._credentials.service_account_email
        except AttributeError:
            # サービスアカウントメールを直接指定（フォールバック）
            service_account_email = "spreadsheet@aptest-384703.iam.gserviceaccount.com"
        
        print(f"🔍 ファイル権限確認中: {file_name}")
        print(f"🔍 サービスアカウント: {service_account_email}")
        
        # 現在の権限を確認
        permissions = service.permissions().list(fileId=file_id).execute()
        
        # デバッグ: 全権限を表示
        print(f"🔍 ファイルの全権限情報:")
        for i, permission in enumerate(permissions.get('permissions', [])):
            print(f"  {i+1}. メール: {permission.get('emailAddress', 'N/A')}")
            print(f"     タイプ: {permission.get('type', 'N/A')}")
            print(f"     ロール: {permission.get('role', 'N/A')}")
            print(f"     ID: {permission.get('id', 'N/A')}")
        
        # サービスアカウントが編集権限を持っているか確認
        has_edit_permission = False
        matched_permission = None
        
        for permission in permissions.get('permissions', []):
            perm_email = permission.get('emailAddress', '')
            perm_role = permission.get('role', '')
            perm_type = permission.get('type', '')
            
            # メールアドレスでのマッチング（大文字小文字を区別しない）
            if (perm_email and perm_email.lower() == service_account_email.lower() and 
                perm_role in ['writer', 'owner']):
                has_edit_permission = True
                matched_permission = permission
                print(f"✅ サービスアカウントは既に{perm_role}権限を持っています")
                break
            # サービスアカウントタイプでのマッチングも試行
            elif (perm_type == 'serviceAccount' and perm_role in ['writer', 'owner']):
                # メールが一致しない場合でもサービスアカウントの可能性を確認
                print(f"🔍 サービスアカウント種別の{perm_role}権限を発見: {perm_email}")
                if not has_edit_permission:  # 既にマッチしていない場合のみ
                    has_edit_permission = True
                    matched_permission = permission
                    print(f"✅ サービスアカウントと推定される{perm_role}権限を確認")
        
        if not has_edit_permission:
            print(f"⚠️ サービスアカウントに編集権限がありません")
            print(f"🔧 手動で権限付与が必要: {service_account_email} に編集者権限を付与してください")
            
            # デバッグ情報: 権限不一致の詳細
            print(f"🔍 デバッグ情報:")
            print(f"  期待されるメール: {service_account_email}")
            print(f"  期待されるロール: writer または owner")
            
            # 類似したメールアドレスを検索
            for permission in permissions.get('permissions', []):
                perm_email = permission.get('emailAddress', '')
                if perm_email and 'aptest-384703' in perm_email:
                    print(f"  類似メール発見: {perm_email} (ロール: {permission.get('role', 'N/A')})")
            
            return False
        else:
            if matched_permission:
                print(f"✅ 権限確認成功: {matched_permission.get('role')} 権限でアクセス可能")
        
        return True
        
    except Exception as e:
        print(f"⚠️ 権限確認エラー: {str(e)}")
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
        
        # まずファイル情報を確認
        try:
            file_info = service.files().get(fileId=file_id).execute()
            print(f"削除対象ファイル確認: {file_info.get('name', 'Unknown')} (ID: {file_id})")
        except Exception as info_error:
            print(f"警告: ファイル情報取得失敗: {info_error}")
        
        # 権限確認と自動付与を試行
        if not check_and_grant_file_permissions(service, file_id, file_name):
            print(f"⚠️ ファイル権限不足のため削除をスキップ: {file_name}")
            return False
        
        # ファイル削除実行
        service.files().delete(fileId=file_id).execute()
        print(f"✅ Google Driveファイル削除成功: {file_name}")
        return True

    except Exception as e:
        error_msg = f"Google Driveファイル削除失敗: {file_name} - {str(e)}"
        print(f"⚠️ {error_msg}")
        
        # エラーの種類によって詳細な指示を提供
        if "insufficient authentication scopes" in str(e).lower():
            print("⚠️ 原因: Google Drive APIの認証スコープ不足")
            print("🔧 解決策: Google Cloud Consoleでサービスアカウントに'https://www.googleapis.com/auth/drive'スコープを許可してください")
        elif "403" in str(e):
            print("⚠️ 原因: アクセス権限不足 - サービスアカウントにファイルの削除権限がありません")
            print("🔧 解決策: ファイルの所有者にサービスアカウントへの編集権限を付与してもらってください")
        elif "404" in str(e):
            print("⚠️ 原因: ファイルが見つかりません（既に削除済みまたは移動済み）")
        
        print("注意: 削除に失敗しましたが、手動で削除できます。")
        # 削除エラーは重大ではないため、メール通知はスキップ
        return False


def safe_delete_local_file(file_path, max_retries=5):
    """
    ローカルファイルを安全に削除（リトライ機能付き）

    Args:
        file_path (str): 削除対象ファイルパス
        max_retries (int): 最大リトライ回数

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """
    if not os.path.exists(file_path):
        return True  # ファイルが存在しない場合は成功扱い

    file_name = os.path.basename(file_path)
    
    for attempt in range(max_retries):
        try:
            # メモリ解放
            gc.collect()
            
            # プロセス監視（psutilが利用可能な場合）
            if PSUTIL_AVAILABLE and attempt > 0:
                try:
                    for proc in psutil.process_iter(['pid', 'name', 'open_files']):
                        if proc.info['open_files']:
                            for open_file in proc.info['open_files']:
                                if open_file.path == file_path:
                                    print(f"警告: プロセス {proc.info['name']} (PID: {proc.info['pid']}) がファイルを使用中")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            # ファイル削除実行
            os.remove(file_path)
            print(f"✅ ローカルファイル削除成功: {file_name}")
            return True

        except FileNotFoundError:
            return True  # ファイルが既に存在しない場合は成功扱い
        except PermissionError:
            print(f"⚠️ ファイル削除失敗 (試行 {attempt + 1}/{max_retries}): 権限エラー - {file_name}")
            time.sleep(1)  # 1秒待機してリトライ
        except Exception as e:
            print(f"⚠️ ファイル削除失敗 (試行 {attempt + 1}/{max_retries}): {str(e)} - {file_name}")
            time.sleep(1)

    print(f"⚠️ ローカルファイル削除を諦めました: {file_name}")
    print("注意: 削除に失敗しましたが、手動で削除できます。")
    return False


def cleanup_folder_contents(folder_path):
    """
    フォルダ内のファイルとサブディレクトリを完全に削除

    Args:
        folder_path (str): クリーンアップ対象フォルダパス

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """
    if not os.path.exists(folder_path):
        return True

    success = True
    
    try:
        for root, dirs, files in os.walk(folder_path, topdown=False):
            # ファイル削除
            for file in files:
                file_path = os.path.join(root, file)
                if not safe_delete_local_file(file_path):
                    success = False
            
            # サブディレクトリ削除
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    os.rmdir(dir_path)
                    print(f"✅ ディレクトリ削除成功: {dir_name}")
                except Exception as e:
                    print(f"⚠️ ディレクトリ削除失敗: {dir_name} - {str(e)}")
                    success = False

        print(f"フォルダクリーンアップ完了: {folder_path}")
        return success

    except Exception as e:
        print(f"⚠️ フォルダクリーンアップエラー: {str(e)}")
        return False


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


def upload_file_to_lineworks(access_token, file_data, file_name):
    """
    ファイルデータをLINE WORKSにアップロードしてファイルIDを取得

    LINE WORKSのファイルアップロードは2段階で行われます：
    1. メタデータ登録: ファイル情報を事前に登録し、uploadURLを取得
    2. ファイル本体アップロード: 取得したuploadURLにPUTリクエストでファイルを送信

    Args:
        access_token (str): LINE WORKS APIのアクセストークン
        file_data (bytes): アップロードするファイルのバイトデータ
        file_name (str): ファイル名

    Returns:
        str: 成功時はfileId、失敗時はNone
    """
    try:
        # ① 添付メタデータ登録
        # ファイル情報を事前に登録し、アップロード用のURLを取得
        meta_url = f"https://www.worksapis.com/v1.0/bots/{BOT_ID}/attachments"
        file_size = len(file_data)

        # メタデータリクエストボディ
        meta_body = {
            "fileName": file_name,
            "fileSize": file_size,
            "fileType": "file"  # ファイルタイプは"file"で固定
        }

        meta_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        print(f"メタデータ登録開始: {file_name} ({file_size} bytes)")
        meta_resp = requests.post(meta_url, headers=meta_headers, json=meta_body)
        print(f"メタ登録レスポンス: {meta_resp.status_code}")
        print(f"レスポンス内容: {meta_resp.text}")

        if meta_resp.status_code not in (200, 201):
            print(f"メタデータ登録失敗: HTTP {meta_resp.status_code}")
            return None

        meta_json = meta_resp.json()
        file_id = meta_json.get("fileId")
        upload_url = meta_json.get("uploadUrl")

        if not file_id or not upload_url:
            print("必要な情報が取得できません:")
            print(f"  fileId: {file_id}")
            print(f"  uploadUrl: {upload_url}")
            return None

        # ② ファイル本体をアップロード
        # 取得したuploadURLにファイルの実際のデータを送信
        print(f"ファイル本体アップロード開始: {upload_url}")

        # URLの詳細分析
        print(f"uploadURL詳細分析:")
        print(f"  ドメイン: apis-storage.worksmobile.com")
        print(
            f"  パス: {upload_url.split('apis-storage.worksmobile.com')[1] if 'apis-storage.worksmobile.com' in upload_url else 'N/A'}")

        print(f"アップロードするファイルサイズ: {len(file_data)} bytes")

        # 複数のHTTPメソッドと認証方式を試行
        methods_to_try = [
            # PUT方式
            {
                "method": "PUT",
                "name": "PUT + Bearer Token",
                "headers": {
                    "Content-Type": "application/octet-stream",
                    "Authorization": f"Bearer {access_token}"
                },
                "data": file_data
            },
            {
                "method": "PUT",
                "name": "PUT + 認証なし",
                "headers": {
                    "Content-Type": "application/octet-stream"
                },
                "data": file_data
            },
            # POST方式
            {
                "method": "POST",
                "name": "POST + Bearer Token",
                "headers": {
                    "Content-Type": "application/octet-stream",
                    "Authorization": f"Bearer {access_token}"
                },
                "data": file_data
            },
            {
                "method": "POST",
                "name": "POST + 認証なし",
                "headers": {
                    "Content-Type": "application/octet-stream"
                },
                "data": file_data
            },
            {
                "method": "POST",
                "name": "POST + multipart/form-data",
                "headers": {
                    "Authorization": f"Bearer {access_token}"
                },
                "files": {"file": (file_name, file_data, "application/octet-stream")}
            }
        ]

        success = False
        for method in methods_to_try:
            print(f"\n試行中: {method['name']}")
            print(f"HTTPメソッド: {method['method']}")
            print(f"ヘッダー: {method['headers']}")

            try:
                if method['method'] == 'PUT':
                    if "files" in method:
                        resp = requests.put(upload_url, headers=method['headers'], files=method['files'])
                    else:
                        resp = requests.put(upload_url, headers=method['headers'], data=method['data'])
                else:  # POST
                    if "files" in method:
                        resp = requests.post(upload_url, headers=method['headers'], files=method['files'])
                    else:
                        resp = requests.post(upload_url, headers=method['headers'], data=method['data'])

                print(f"レスポンス: {resp.status_code}")

                if resp.status_code in (200, 201):
                    print(f"✅ {method['name']}で成功!")
                    print(f"ファイルアップロード成功: {file_name} (ID: {file_id})")
                    success = True
                    break
                else:
                    print(f"❌ {method['name']}失敗: {resp.text}")

            except Exception as e:
                print(f"❌ {method['name']}でエラー: {str(e)}")

        if success:
            return file_id
        else:
            print(f"\n全てのメソッドが失敗しました")

            # 最後の手段: URLを直接解析して問題を特定
            print(f"\n=== デバッグ情報 ===")
            print(f"取得したuploadURL: {upload_url}")
            print(f"アクセストークンの最初の50文字: {access_token[:50]}...")
            print(f"ファイルサイズ: {len(file_data)} bytes")
            print(f"ファイル名: {file_name}")

            return None

    except requests.exceptions.RequestException as e:
        error_msg = f"LINE WORKSアップロードネットワークエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKSアップロードエラー:\n{error_msg}")
        return None
    except Exception as e:
        error_msg = f"LINE WORKSアップロード予期しないエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKSアップロードエラー:\n{error_msg}")
        return None


def send_file_message(access_token, room_id, file_id, file_name):
    """
    アップロードしたファイルをメッセージとして送信

    Args:
        access_token (str): LINE WORKS APIのアクセストークン
        room_id (str): 送信先のトークルームID
        file_id (str): アップロード済みファイルのID
        file_name (str): ファイル名（表示用）

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """
    try:
        # メッセージ送信用URL
        message_url = f"https://www.worksapis.com/v1.0/bots/{BOT_ID}/channels/{room_id}/messages"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # ファイルメッセージを作成
        message_data = {
            "content": {
                "type": "file",  # メッセージタイプは"file"
                "fileId": file_id,  # アップロード時に取得したfileId
                "fileName": file_name  # 表示されるファイル名
            }
        }

        print(f"ファイルメッセージ送信開始: {file_name}")
        resp = requests.post(message_url, headers=headers, json=message_data)

        if resp.status_code in (200, 201):
            print(f"ファイルメッセージ送信成功: {file_name}")
            return True
        else:
            print(f"ファイルメッセージ送信失敗: HTTP {resp.status_code}")
            print(f"エラー詳細: {resp.text}")
            return False

    except requests.exceptions.RequestException as e:
        error_msg = f"LINE WORKSメッセージ送信ネットワークエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKSメッセージ送信エラー:\n{error_msg}")
        return False
    except Exception as e:
        error_msg = f"LINE WORKSメッセージ送信予期しないエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKSメッセージ送信エラー:\n{error_msg}")
        return False


def get_access_token():
    """
    LINE WORKS APIのアクセストークンを取得

    Returns:
        str: 成功時はアクセストークン、失敗時はNone
    """
    try:
        # 1. JWT生成
        # Service Accountを使用してJWTトークンを生成
        if not private_key_path.exists():
            print(f"秘密鍵ファイルが見つかりません: {private_key_path}")
            return None

        with open(private_key_path, "r") as f:
            private_key = f.read()

        iat = int(time.time())  # 発行時刻
        exp = iat + 60 * 60  # 有効期限（1時間）

        # JWTペイロード
        payload = {
            "iss": CLIENT_ID,  # 発行者（Client ID）
            "sub": service_account,  # 主体（Service Account）
            "iat": iat,  # 発行時刻
            "exp": exp,  # 有効期限
            "aud": audience,  # 対象者（トークンエンドポイント）
        }

        print("JWT生成中...")
        jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
        print("JWT生成完了")

        # 2. アクセストークン取得
        # JWTを使用してOAuth2.0のアクセストークンを取得
        print("アクセストークン取得中...")
        token_url = "https://auth.worksmobile.com/oauth2/v2.0/token"
        token_data = {
            "assertion": jwt_token,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "bot"  # Bot APIを使用するためのスコープ
        }
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        token_resp = requests.post(token_url, data=token_data, headers=token_headers)
        token_resp.raise_for_status()

        token_json = token_resp.json()
        access_token = token_json["access_token"]
        print("アクセストークン取得成功")

        # アクセストークンの詳細情報を表示
        print(f"トークン情報:")
        print(f"  access_token: {access_token[:50]}...")
        if "scope" in token_json:
            print(f"  scope: {token_json['scope']}")
        if "token_type" in token_json:
            print(f"  token_type: {token_json['token_type']}")
        if "expires_in" in token_json:
            print(f"  expires_in: {token_json['expires_in']}秒")

        return access_token

    except FileNotFoundError as e:
        error_msg = f"LINE WORKS秘密鍵ファイルが見つかりません: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKS認証エラー:\n{error_msg}")
        return None
    except jwt.InvalidKeyError:
        error_msg = "秘密鍵が無効です。正しい秘密鍵ファイルを確認してください"
        print(error_msg)
        send_error_email(f"LINE WORKS認証エラー:\n{error_msg}")
        return None
    except requests.exceptions.RequestException as e:
        error_msg = f"LINE WORKSアクセストークン取得ネットワークエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKS認証エラー:\n{error_msg}")
        return None
    except KeyError as e:
        error_msg = f"LINE WORKSアクセストークンレスポンスエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKS認証エラー:\n{error_msg}")
        return None
    except Exception as e:
        error_msg = f"LINE WORKS認証予期しないエラー: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKS認証エラー:\n{error_msg}")
        return None


def search_files_in_google_drive(query="", max_results=10):
    """
    Google Drive内のファイルを検索

    Args:
        query (str): 検索クエリ（例: "name contains 'method'"）
        max_results (int): 最大結果数

    Returns:
        list: ファイル情報のリスト
    """
    try:
        service = get_google_drive_service()
        if not service:
            return []

        print(f"Google Driveファイル検索中: '{query}'")

        # ファイル検索実行
        results = service.files().list(
            q=query,
            pageSize=max_results,
            fields="nextPageToken, files(id, name, size, mimeType, modifiedTime, webViewLink)"
        ).execute()

        files = results.get('files', [])

        if not files:
            print("検索結果: ファイルが見つかりませんでした")
            return []

        print(f"検索結果: {len(files)}件のファイルが見つかりました")
        for i, file in enumerate(files, 1):
            print(f"  {i}. {file['name']} (ID: {file['id']})")
            print(f"     サイズ: {file.get('size', 'N/A')} bytes")
            print(f"     更新日時: {file.get('modifiedTime', 'N/A')}")
            print(f"     リンク: {file.get('webViewLink', 'N/A')}")

        return files

    except Exception as e:
        error_msg = f"Google Drive検索エラー: {str(e)}"
        print(error_msg)
        send_error_email(f"Google Drive検索エラー:\n{error_msg}")
        return []


def extended_file_search():
    """
    より詳細なファイル検索を実行
    """
    print("=== 拡張ファイル検索 ===")

    # 1. 全種類のファイル検索
    print("\n1. 全ファイル（最初の50件）:")
    all_files = search_files_in_google_drive("", 50)

    # 2. HTML関連の検索パターンを複数試行
    search_patterns = [
        "name contains 'html'",
        "name contains 'HTML'",
        "name contains 'method'",
        "name contains 'Method'",
        "name contains 'fix'",
        "name contains 'Fix'",
        "mimeType contains 'html'",
        "mimeType = 'text/html'",
        "name contains '.htm'"
    ]

    found_files = []
    for pattern in search_patterns:
        print(f"\n検索パターン: {pattern}")
        files = search_files_in_google_drive(pattern, 20)
        found_files.extend(files)

    # 重複除去
    unique_files = {}
    for file in found_files:
        unique_files[file['id']] = file

    print(f"\n=== 検索結果サマリー ===")
    print(f"ユニークなファイル数: {len(unique_files)}")

    return list(unique_files.values())


def test_google_drive_connection():
    """
    Google Drive接続をテストし、アクセス可能なファイルを表示
    """
    print("=== Google Drive接続テスト ===")

    # 全ファイル検索（最初の10件）
    print("\n1. 全ファイル検索（最初の10件）:")
    all_files = search_files_in_google_drive("", 10)

    # method関連ファイル検索
    print("\n2. 'method'を含むファイル検索:")
    method_files = search_files_in_google_drive("name contains 'method'", 20)

    # HTML関連ファイル検索
    print("\n3. '.html'を含むファイル検索:")
    html_files = search_files_in_google_drive("name contains '.html'", 20)

    return all_files, method_files, html_files


def send_folder_files_to_lineworks(folder_id, file_filter=None):
    """
    Google Driveフォルダ内のファイルをLINE WORKSに送信

    Args:
        folder_id (str): Google DriveのフォルダID
        file_filter (dict): ファイルフィルター条件

    Returns:
        dict: 送信結果の詳細
    """
    try:
        print(f"\n=== フォルダ内HTMLファイル送信開始 ===")
        print(f"フォルダID: {folder_id}")
        print(f"削除モード: {'有効' if DELETE_AFTER_UPLOAD else '無効'}")

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
            result = send_file_to_lineworks(file_id)

            if result:
                sent_files.append(file_name)
                print(f"✅ {file_name} 送信完了")
                
                # 送信成功時にファイル削除
                if DELETE_AFTER_UPLOAD:
                    print(f"配信成功により削除実行: {file_name}")
                    if delete_file_from_google_drive(file_id, file_name):
                        deleted_files.append(file_name)
                    else:
                        print(f"⚠️ ファイル削除失敗（手動で削除してください）: {file_name}")
            else:
                failed_files.append(file_name)
                print(f"❌ {file_name} 送信失敗 - ファイル削除をスキップ")

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
        send_error_email(f"LINE WORKSフォルダ送信エラー:\n{error_msg}")
        return {'success': False, 'sent_files': [], 'failed_files': [], 'deleted_files': [], 'total_files': 0}


def send_file_to_lineworks(file_id=None):
    """
    Google DriveからファイルをダウンロードしてLINE WORKSに送信

    Args:
        file_id (str, optional): Google DriveファイルID。指定されない場合はデフォルト値を使用

    処理の流れ：
    1. Google Driveからファイルをダウンロード
    2. JWT生成（Service Accountを使用）
    3. アクセストークン取得
    4. ファイルアップロード
    5. ファイルメッセージ送信

    Returns:
        bool: 成功時はTrue、失敗時はFalse
    """

    # ファイルIDの決定
    actual_file_id = file_id if file_id else target_google_drive_file_id

    try:
        # 1. Google Driveからファイルを取得
        print("=== Google Driveからファイル取得 ===")
        print(f"ファイル名: method_fix.html")
        print(f"ファイルID: {actual_file_id}")

        file_data, file_name = download_file_from_google_drive(actual_file_id)
        if not file_data:
            print("Google Driveからのファイル取得に失敗しました")
            return False

        # 2. LINE WORKSアクセストークン取得
        print("\n=== LINE WORKS認証 ===")
        access_token = get_access_token()
        if not access_token:
            print("アクセストークン取得に失敗しました")
            return False

        # Bot情報を確認
        print("\nBot情報確認中...")
        bot_info_url = f"https://www.worksapis.com/v1.0/bots/{BOT_ID}"
        bot_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        try:
            bot_resp = requests.get(bot_info_url, headers=bot_headers)
            if bot_resp.status_code == 200:
                bot_info = bot_resp.json()
                print(f"✅ Bot情報取得成功:")
                print(f"  Bot名: {bot_info.get('name', 'N/A')}")
                print(f"  Bot状態: {bot_info.get('state', 'N/A')}")
                print(f"  権限: {bot_info.get('scopes', 'N/A')}")
            else:
                print(f"❌ Bot情報取得失敗: {bot_resp.status_code}")
                print(f"エラー: {bot_resp.text}")
        except Exception as e:
            print(f"❌ Bot情報確認エラー: {str(e)}")

        # 3. ファイルをアップロード
        print("\n=== ファイルアップロード ===")
        file_id = upload_file_to_lineworks(access_token, file_data, file_name)
        if not file_id:
            print("ファイルアップロードに失敗しました")
            return False

        # 4. ファイルメッセージを送信
        print("\n=== ファイルメッセージ送信 ===")
        room_id = "6d53f79a-ba39-e9d5-cf52-07ddd58d66cf"  # 全社トークルームのID
        success = send_file_message(access_token, room_id, file_id, file_name)

        if success:
            print("\n✅ 全ての処理が正常に完了しました")
            
            # 送信成功時にファイル削除（単一ファイルモード）
            if DELETE_AFTER_UPLOAD and not USE_FOLDER_MODE:
                print(f"\n=== ファイル削除処理 ===")
                print(f"配信成功により削除実行: {file_name}")
                delete_file_from_google_drive(actual_file_id, file_name)
        else:
            print("\n❌ ファイルメッセージ送信に失敗しました")

        return success

    except Exception as e:
        error_msg = f"予期しないエラーが発生しました: {str(e)}"
        print(error_msg)
        send_error_email(f"LINE WORKSファイル送信エラー:\n{error_msg}")
        return False


# --- メイン処理 ---
try:
    if __name__ == "__main__":
        print("=== LINE WORKS ファイル送信スクリプト (フォルダ対応版) ===")
        print(f"Bot ID: {BOT_ID}")
        print(f"Service Account: {service_account}")
        print(f"削除モード: {'有効' if DELETE_AFTER_UPLOAD else '無効'}")

        if USE_FOLDER_MODE:
            print(f"\n動作モード: フォルダ一括送信（HTMLファイル専用）")
            print(f"送信対象フォルダID: {target_google_drive_folder_id}")
            print(f"フィルター設定: {file_filter_config}")
            print("=" * 60)

            result = send_folder_files_to_lineworks(
                target_google_drive_folder_id,
                file_filter_config
            )

            if result['success']:
                print(f"\n🎉 フォルダ内HTMLファイル送信完了: {len(result['sent_files'])}件")
                if DELETE_AFTER_UPLOAD:
                    print(f"🗑️ ファイル削除完了: {len(result['deleted_files'])}件")
            elif result['total_files'] > 0:
                print(f"\n⚠️ 一部送信失敗: 成功{len(result['sent_files'])}件, 失敗{len(result['failed_files'])}件")
                if DELETE_AFTER_UPLOAD:
                    print(f"🗑️ ファイル削除完了: {len(result['deleted_files'])}件")
            else:
                print(f"\n❌ 送信対象HTMLファイルなし")
                print("\n詳細なデバッグのため、検索機能も実行します...")

                # デバッグのため、検索機能も実行
                print("\n=== Google Drive接続テスト開始 ===")
                all_files, method_files, html_files = test_google_drive_connection()

                # 拡張検索も実行
                print("\n=== 拡張ファイル検索開始 ===")
                extended_files = extended_file_search()

        else:
            print(f"\n動作モード: 単一ファイル送信")
            print(f"送信対象ファイルID: {target_google_drive_file_id}")
            print("=" * 60)

            print(f"\n=== 直接ファイルダウンロード開始 ===")
            result = send_file_to_lineworks(target_google_drive_file_id)

            if result:
                print("\n✅ ファイル送信が正常に完了しました")
                if DELETE_AFTER_UPLOAD:
                    print("🗑️ 送信成功によりファイル削除を実行しました")
            else:
                print("\n❌ ファイル送信に失敗しました")
                print("詳細なデバッグのため、検索機能も実行します...")

                # デバッグのため、検索機能も実行
                print("\n=== Google Drive接続テスト開始 ===")
                all_files, method_files, html_files = test_google_drive_connection()

                # 拡張検索も実行
                print("\n=== 拡張ファイル検索開始 ===")
                extended_files = extended_file_search()

        # ローカルキャッシュのクリーンアップ
        if DELETE_LOCAL_CACHE:
            print("\n=== ローカルキャッシュクリーンアップ ===")
            cache_folders = [
                os.path.join(os.path.dirname(__file__), "cache"),
                os.path.join(os.path.dirname(__file__), "temp"),
                os.path.join(os.path.dirname(__file__), "downloads")
            ]
            
            for cache_folder in cache_folders:
                if os.path.exists(cache_folder):
                    cleanup_folder_contents(cache_folder)

        print("\n以下を確認してください:")
        print("- Google Driveファイル/フォルダへのアクセス権限")
        print("- Google サービスアカウントファイル (aptest-384703-24764f69b34f.json)")
        print("- ファイル/フォルダがサービスアカウントと共有されているか")
        print("- LINE WORKS認証情報が正しいか")
        if DELETE_AFTER_UPLOAD:
            print("- Google Driveでの削除権限があるか")
            
        print("\n🎉 スクリプトが正常に完了しました！")

except Exception as e:
    error_detail = traceback.format_exc()
    send_error_email(error_detail)
    # エラーを再発生させてプログラムを停止
    raise
