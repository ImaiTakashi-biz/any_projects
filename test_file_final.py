import requests
import time
import os

# 新しいAPIキーとルームIDを使用
BASE_URL = "https://araichat-966672454924.asia-northeast1.run.app/"
API_KEY = "ak_utNK5KIjQNLgNPC6Ms95517oBVa1bOBR7mHsQBP-juw"
ROOM_ID = "4"

def send_file_test():
    url = f"{BASE_URL}/api/integrations/send/{ROOM_ID}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    # 小さなテストファイルを作成
    test_content = """ファイル送信テスト用ドキュメント

これは外部Pythonスクリプトからのファイル送信テストです。

内容:
- APIキー: ak_bGSQH76OoAnyhzLJpA3j3QNOpz41cNIYTfsPAaXeUXA
- ルームID: 2
- テスト日時: 2025-08-31

ファイル送信機能が正常に動作することを確認しています。"""
    
    # テストファイルを作成
    with open("test_upload.txt", "w", encoding="utf-8") as f:
        f.write(test_content)
    
    print("=== ファイル送信テスト開始 ===")
    print(f"APIキー: {API_KEY[:20]}...")
    print(f"ルームID: {ROOM_ID}")
    
    data = {"text": "外部スクリプトからファイル付きメッセージ送信テスト"}
    
    try:
        with open("test_upload.txt", "rb") as f:
            files = [("files", ("test_upload.txt", f))]
            print("ファイル送信中...")
            resp = requests.post(url, headers=headers, data=data, files=files, timeout=15)
            resp.raise_for_status()
            result = resp.json()
            print("✅ 送信成功:", result)
            return result
    except requests.exceptions.Timeout:
        print("❌ タイムアウトエラー: サーバーの応答が遅すぎます")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ リクエストエラー: {e}")
        return None
    except Exception as e:
        print(f"❌ 予期しないエラー: {e}")
        return None
    finally:
        # クリーンアップ
        if os.path.exists("test_upload.txt"):
            os.remove("test_upload.txt")
            print("テストファイルを削除しました")

if __name__ == "__main__":
    result = send_file_test()
    print("=== ファイル送信テスト完了 ===")
    if result:
        print("テスト結果: 成功")
    else:
        print("テスト結果: 失敗")