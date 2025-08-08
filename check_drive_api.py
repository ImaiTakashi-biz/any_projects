#!/usr/bin/env python3
"""
Google Drive API有効化状況確認スクリプト
"""

import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 設定
GOOGLE_SERVICE_ACCOUNT_FILE = "aptest-384703-24764f69b34f.json"
GOOGLE_DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def check_service_account_info():
    """
    サービスアカウント情報を確認
    """
    print("=== サービスアカウント情報 ===")
    
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        service_account_file = os.path.join(base_dir, GOOGLE_SERVICE_ACCOUNT_FILE)
        
        if not os.path.exists(service_account_file):
            print(f"❌ サービスアカウントファイルが見つかりません: {service_account_file}")
            return False
        
        with open(service_account_file, 'r') as f:
            account_info = json.load(f)
        
        print(f"✅ サービスアカウントファイル: 存在")
        print(f"📧 クライアントメール: {account_info.get('client_email', 'N/A')}")
        print(f"🔑 プロジェクトID: {account_info.get('project_id', 'N/A')}")
        print(f"🆔 クライアントID: {account_info.get('client_id', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ サービスアカウント情報確認エラー: {str(e)}")
        return False

def check_drive_api_access():
    """
    Google Drive APIへのアクセステスト
    """
    print("\n=== Google Drive API アクセステスト ===")
    
    try:
        # 認証情報の設定
        base_dir = os.path.dirname(os.path.abspath(__file__))
        service_account_file = os.path.join(base_dir, GOOGLE_SERVICE_ACCOUNT_FILE)
        
        credentials = Credentials.from_service_account_file(
            service_account_file, 
            scopes=GOOGLE_DRIVE_SCOPES
        )
        
        # Google Drive APIサービスの初期化
        service = build('drive', 'v3', credentials=credentials)
        print("✅ Google Drive APIサービス初期化: 成功")
        
        # APIアクセステスト（about.get で基本情報を取得）
        about = service.about().get(fields="user,storageQuota").execute()
        
        print("✅ Google Drive API アクセス: 成功")
        print(f"📊 API バージョン: v3")
        print(f"👤 認証ユーザー: {about.get('user', {}).get('displayName', 'Service Account')}")
        print(f"📧 ユーザーメール: {about.get('user', {}).get('emailAddress', 'N/A')}")
        
        # ファイル一覧取得テスト（最初の5件）
        print("\n--- ファイル一覧取得テスト ---")
        results = service.files().list(
            pageSize=5,
            fields="nextPageToken, files(id, name, mimeType)"
        ).execute()
        
        files = results.get('files', [])
        if files:
            print(f"✅ ファイル一覧取得: 成功 ({len(files)}件)")
            for i, file in enumerate(files, 1):
                print(f"  {i}. {file['name']} (ID: {file['id']})")
        else:
            print("⚠️  アクセス可能なファイルが見つかりません")
        
        return True
        
    except HttpError as e:
        print(f"❌ Google Drive API HTTPエラー: {e}")
        
        if e.resp.status == 403:
            print("💡 解決方法:")
            print("1. Google Cloud Console で Google Drive API が有効化されているか確認")
            print("2. サービスアカウントに適切な権限が付与されているか確認")
            print("3. https://console.cloud.google.com/apis/library/drive.googleapis.com")
        elif e.resp.status == 401:
            print("💡 解決方法:")
            print("1. サービスアカウントキーファイルが正しいか確認")
            print("2. 認証スコープが適切か確認")
        
        return False
        
    except Exception as e:
        print(f"❌ 予期しないエラー: {str(e)}")
        return False

def check_specific_file_access():
    """
    特定のファイルへのアクセステスト
    """
    print("\n=== 特定ファイルアクセステスト ===")

    test_file_id = "1xOWQuGjzeaadLpybmCg93e-89O9Bu3nN"  # method_fix.html
    
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        service_account_file = os.path.join(base_dir, GOOGLE_SERVICE_ACCOUNT_FILE)
        
        credentials = Credentials.from_service_account_file(
            service_account_file, 
            scopes=GOOGLE_DRIVE_SCOPES
        )
        
        service = build('drive', 'v3', credentials=credentials)
        
        # 特定ファイルの情報を取得
        file_metadata = service.files().get(fileId=test_file_id).execute()
        
        print(f"✅ ファイルアクセス: 成功")
        print(f"📄 ファイル名: {file_metadata.get('name', 'N/A')}")
        print(f"🔗 MIMEタイプ: {file_metadata.get('mimeType', 'N/A')}")
        print(f"📅 更新日時: {file_metadata.get('modifiedTime', 'N/A')}")
        
        return True
        
    except HttpError as e:
        print(f"❌ ファイルアクセスエラー: {e}")
        
        if e.resp.status == 404:
            print("💡 ファイルが見つからないか、アクセス権限がありません")
            print("   ファイルをサービスアカウントと共有してください")
        
        return False
        
    except Exception as e:
        print(f"❌ 予期しないエラー: {str(e)}")
        return False

def main():
    """
    メイン実行関数
    """
    print("🔍 Google Drive API 有効化状況確認スクリプト")
    print("=" * 60)
    
    # 1. サービスアカウント情報確認
    if not check_service_account_info():
        print("\n❌ サービスアカウント設定に問題があります")
        return
    
    # 2. Drive API アクセステスト
    if not check_drive_api_access():
        print("\n❌ Google Drive API へのアクセスに問題があります")
        print("\n🔧 Google Cloud Console での確認手順:")
        print("1. https://console.cloud.google.com にアクセス")
        print("2. プロジェクト 'aptest-384703' を選択")
        print("3. 左メニュー > APIs & Services > Library")
        print("4. 'Google Drive API' を検索して有効化")
        return
    
    # 3. 特定ファイルアクセステスト
    check_specific_file_access()
    
    print("\n🎉 全てのテストが完了しました")
    print("Google Drive API は正常に有効化されており、アクセス可能です")

if __name__ == "__main__":
    main()

