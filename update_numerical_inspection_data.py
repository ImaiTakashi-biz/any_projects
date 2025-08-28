import sqlite3
import sys
import os
import smtplib
import traceback
from datetime import datetime, date
from email.mime.text import MIMEText
from notion_client import Client
import pytz
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()


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

# --- SQLiteデータベース設定 ---
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")

if not SQLITE_DB_PATH:
    raise ValueError("SQLITE_DB_PATH が .env ファイルに設定されていません")

# --- Notion API設定 ---
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # DB_数値工程内テーブル
PRODUCT_MASTER_DATABASE_ID = os.getenv("PRODUCT_MASTER_DATABASE_ID")  # DB_数値検査製品マスター

# 環境変数の存在確認
if not NOTION_API_TOKEN:
    raise ValueError("NOTION_API_TOKEN が .env ファイルに設定されていません")
if not NOTION_DATABASE_ID:
    raise ValueError("NOTION_DATABASE_ID が .env ファイルに設定されていません")
if not PRODUCT_MASTER_DATABASE_ID:
    raise ValueError("PRODUCT_MASTER_DATABASE_ID が .env ファイルに設定されていません")

# --- カラムマッピング（SQLite : Notion）---
COLUMN_MAPPING = {
    'id': 'ID',
    'machine_no': '機番',
    'customer_name': '客先名',
    'part_number': '品番',
    'product_name': '品名',
    'cleaning_instruction': '洗浄指示',
    'acquisition_date': '指示日',
    'material_id': '材料識別',
    'notes': '備考',
    'product_master_relation': 'DB_数値検査製品マスター'  # リレーション用
}


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

日時: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

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


def get_today_data_from_sqlite():
    """
    SQLiteデータベースから今日の日付（東京時間）に一致するデータを取得する
    """
    try:
        # 東京時間で今日の日付を取得
        tokyo_tz = pytz.timezone('Asia/Tokyo')
        today = datetime.now(tokyo_tz).date()
        today_str = today.strftime('%Y-%m-%d')
        
        print(f"🔍 取得対象日付: {today_str}")
        
        # SQLiteデータベースに接続
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # データベース内のテーブル一覧を確認
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("📋 データベース内のテーブル一覧:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # テーブルが存在しない場合は使用可能なテーブルから適切なものを探す
        table_name = None
        for table in tables:
            table_name_candidate = table[0]
            if 'cleaning' in table_name_candidate.lower() or 'instruction' in table_name_candidate.lower():
                table_name = table_name_candidate
                break
        
        # テーブルが見つからない場合は、最初のテーブルを確認
        if not table_name and tables:
            table_name = tables[0][0]
            print(f"⚠️ cleaning_instructionsテーブルが見つからないため、{table_name}テーブルを確認します")
        elif not tables:
            raise Exception("データベースにテーブルが存在しません")
        
        # 選択されたテーブルの構造を確認
        cursor.execute(f"PRAGMA table_info({table_name})")
        table_info = cursor.fetchall()
        print(f"📋 {table_name}テーブル構造:")
        for column in table_info:
            print(f"  - {column[1]} ({column[2]})")
        
        # 今日の日付に一致するデータを取得
        # テーブルに必要なカラムがあるか確認
        required_columns = ['id', 'machine_no', 'customer_name', 'part_number', 'product_name', 
                           'cleaning_instruction', 'acquisition_date', 'material_id', 'notes']
        available_columns = [col[1] for col in table_info]
        
        # 必要なカラムがあるか確認
        missing_columns = [col for col in required_columns if col not in available_columns]
        if missing_columns:
            print(f"⚠️ 不足しているカラム: {missing_columns}")
            print(f"ℹ️ 利用可能なカラムでクエリを実行します")
            # 利用可能なカラムのみでクエリを構築
            select_columns = [col for col in required_columns if col in available_columns]
        else:
            select_columns = required_columns
        
        select_clause = ', '.join(select_columns)
        
        # acquisition_dateカラムが存在しない場合のWHERE条件を調整
        if 'acquisition_date' in select_columns:
            query = f"""
            SELECT {select_clause}
            FROM {table_name} 
            WHERE DATE(acquisition_date) = ?
            """
            print(f"🔍 実行するSQL: {query} (today_str: {today_str})")
            cursor.execute(query, (today_str,))
        else:
            print("⚠️ acquisition_dateカラムが存在しないため、全データを取得します")
            query = f"""
            SELECT {select_clause}
            FROM {table_name}
            """
            print(f"🔍 実行するSQL: {query}")
            cursor.execute(query)
        rows = cursor.fetchall()
        
        # 結果を辞書のリストに変換
        data_list = []
        for row in rows:
            data_dict = {}
            for i, column_name in enumerate(select_columns):
                if i < len(row):
                    value = row[i]
                    if column_name in ['id', 'cleaning_instruction', 'material_id']:
                        # 整数フィールドの安全な変換処理
                        if value is None or value == '' or value == 'NULL':
                            data_dict[column_name] = 0
                        else:
                            try:
                                data_dict[column_name] = int(value)
                            except (ValueError, TypeError):
                                print(f"⚠️ 整数変換エラー ({column_name}: {value}) - デフォルト値0を使用")
                                data_dict[column_name] = 0
                    else:
                        # 文字列フィールド
                        data_dict[column_name] = str(value) if value is not None else ''
                else:
                    # データが不足している場合のデフォルト値
                    if column_name in ['id', 'cleaning_instruction', 'material_id']:
                        data_dict[column_name] = 0
                    else:
                        data_dict[column_name] = ''
            data_list.append(data_dict)
        
        conn.close()
        
        print(f"✅ SQLiteから {len(data_list)} 件のデータを取得しました")
        return data_list
        
    except sqlite3.Error as e:
        error_msg = f"SQLiteデータベースエラー: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"データ取得中に予期しないエラーが発生しました: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)


def get_product_master_id_by_part_number(notion, part_number):
    """
    製品マスターデータベースから品番に基づいてページIDを取得する
    """
    try:
        if not part_number or part_number == '':
            return None
        
        print(f"🔍 品番 '{part_number}' を製品マスターで検索中...")
            
        # 製品マスターデータベースで品番を検索
        results = notion.databases.query(
            database_id=PRODUCT_MASTER_DATABASE_ID,
            filter={
                "property": "品番",  # タイトルプロパティ
                "title": {
                    "equals": str(part_number)
                }
            }
        )
        
        if results['results']:
            page_id = results['results'][0]['id']
            print(f"✅ 品番 '{part_number}' の製品マスターIDを取得しました: {page_id}")
            return page_id
        else:
            print(f"⚠️ 品番 '{part_number}' は製品マスターで見つかりませんでした")
            return None
            
    except Exception as e:
        print(f"❌ 製品マスターID取得エラー (part_number: {part_number}): {e}")
        return None


def create_select_option_if_needed(notion, database_id, property_name, option_name):
    """
    Selectプロパティに新しいオプションを作成する
    """
    try:
        # 現在のデータベース構造を取得
        database = notion.databases.retrieve(database_id=database_id)
        property_info = database['properties'].get(property_name, {})
        
        if property_info.get('type') != 'select':
            return False
        
        current_options = property_info.get('select', {}).get('options', [])
        
        # 既にオプションが存在するか確認
        for option in current_options:
            if option['name'] == option_name:
                return True  # 既に存在
        
        # 新しいオプションを追加
        new_option = {
            "name": option_name,
            "color": "default"  # デフォルトの色を使用
        }
        current_options.append(new_option)
        
        # データベースプロパティを更新
        update_data = {
            "properties": {
                property_name: {
                    "select": {
                        "options": current_options
                    }
                }
            }
        }
        
        notion.databases.update(database_id=database_id, **update_data)
        print(f"✅ {property_name}に新しいオプション '{option_name}' を作成しました")
        return True
        
    except Exception as e:
        print(f"⚠️ Selectオプションの作成に失敗: {e}")
        return False


def update_notion_database(data_list):
    """
    NotionデータベースにSQLiteから取得したデータを更新する
    """
    try:
        # Notionクライアントを初期化
        notion = Client(auth=NOTION_API_TOKEN)
        
        print("🔗 Notion APIに接続しました")
        
        # データベース情報を取得してカラム構造を確認
        database = notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
        print("📋 Notionデータベース構造:")
        for prop_name, prop_info in database['properties'].items():
            print(f"  - {prop_name} ({prop_info['type']})")
            if prop_info['type'] == 'select':
                options = prop_info.get('select', {}).get('options', [])
                print(f"    選択肢: {[opt['name'] for opt in options]}")
        
        # 洗浄指示と材料識別のSelectオプションマッピングを作成
        cleaning_instruction_mapping = {}
        material_id_mapping = {}
        
        # 洗浄指示のマッピング
        cleaning_prop = database['properties'].get('洗浄指示', {})
        if cleaning_prop.get('type') == 'select':
            options = cleaning_prop.get('select', {}).get('options', [])
            for opt in options:
                # 数値として解釈可能な選択肢をマッピング
                try:
                    key = int(opt['name'])
                    cleaning_instruction_mapping[key] = opt['name']
                except ValueError:
                    # 数値でない場合は文字列として扱う
                    cleaning_instruction_mapping[opt['name']] = opt['name']
            print(f"🔧 洗浄指示マッピング: {cleaning_instruction_mapping}")
        
        # 材料識別のマッピング
        material_prop = database['properties'].get('材料識別', {})
        if material_prop.get('type') == 'select':
            options = material_prop.get('select', {}).get('options', [])
            for opt in options:
                # 数値として解釈可能な選択肢をマッピング
                try:
                    key = int(opt['name'])
                    material_id_mapping[key] = opt['name']
                except ValueError:
                    # 数値でない場合は文字列として扱う
                    material_id_mapping[opt['name']] = opt['name']
            print(f"🔧 材料識別マッピング: {material_id_mapping}")
        
        success_count = 0
        error_count = 0
        
        for data in data_list:
            try:
                # 製品マスターから品番に基づいてIDを取得（エラーがあっても継続）
                part_number = data.get('part_number', '')
                product_master_id = None
                try:
                    product_master_id = get_product_master_id_by_part_number(notion, part_number)
                except Exception as master_error:
                    print(f"⚠️ 製品マスターID取得でエラーが発生しましたが、処理を継続します: {master_error}")
                
                # Notionページのプロパティを構築
                properties = {}
                
                # 全てのフィールドをマッピング（IDも含む）
                for sqlite_col, notion_col in COLUMN_MAPPING.items():
                    # リレーション用の特殊処理
                    if sqlite_col == 'product_master_relation':
                        if product_master_id and notion_col in database['properties']:
                            prop_type = database['properties'][notion_col]['type']
                            if prop_type == 'relation':
                                properties[notion_col] = {
                                    "relation": [{"id": product_master_id}]
                                }
                            else:
                                print(f"⚠️ {notion_col}はリレーションプロパティではありません: {prop_type}")
                        continue
                    
                    value = data.get(sqlite_col, '')
                    
                    if notion_col in database['properties']:
                        prop_type = database['properties'][notion_col]['type']
                        
                        if prop_type == 'title':
                            # タイトルプロパティの場合（IDフィールド）
                            title_content = str(value) if value is not None else ""
                            properties[notion_col] = {
                                "title": [{
                                    "text": {
                                        "content": title_content
                                    }
                                }]
                            }
                        elif prop_type == 'rich_text':
                            # rich_textプロパティの場合（notes/備考等）
                            text_content = ""
                            if value is not None and str(value).strip() != "" and str(value).upper() != "NULL":
                                text_content = str(value)
                            properties[notion_col] = {
                                "rich_text": [{"text": {"content": text_content}}]
                            }
                        elif prop_type == 'number':
                            if isinstance(value, (int, float)) or str(value).isdigit():
                                properties[notion_col] = {
                                    "number": int(value) if value != '' else 0
                                }
                            else:
                                properties[notion_col] = {"number": 0}
                        elif prop_type == 'select':
                            # Selectプロパティの処理
                            select_value = None
                            if notion_col == '洗浄指示':
                                # 洗浄指示のSelectマッピング（"0"の場合は空欄にする）
                                if value == 0 or str(value) == "0":
                                    select_value = None  # 空欄にする
                                elif value in cleaning_instruction_mapping:
                                    select_value = cleaning_instruction_mapping[value]
                                elif str(value) in cleaning_instruction_mapping:
                                    select_value = cleaning_instruction_mapping[str(value)]
                                else:
                                    # マッピングにない値の場合、新しいオプションとして作成
                                    select_value = str(value) if value != '' else None
                                    if select_value:  # 空でない場合のみオプション作成
                                        # 新しいオプションを作成
                                        if create_select_option_if_needed(notion, NOTION_DATABASE_ID, '洗浄指示', select_value):
                                            cleaning_instruction_mapping[value] = select_value
                            elif notion_col == '材料識別':
                                # 材料識別のSelectマッピング（"0"の場合は空欄にする）
                                if value == 0 or str(value) == "0":
                                    select_value = None  # 空欄にする
                                elif value in material_id_mapping:
                                    select_value = material_id_mapping[value]
                                elif str(value) in material_id_mapping:
                                    select_value = material_id_mapping[str(value)]
                                else:
                                    # マッピングにない値の場合、新しいオプションとして作成
                                    select_value = str(value) if value != '' else None
                                    if select_value:  # 空でない場合のみオプション作成
                                        # 新しいオプションを作成
                                        if create_select_option_if_needed(notion, NOTION_DATABASE_ID, '材料識別', select_value):
                                            material_id_mapping[value] = select_value
                            
                            # selectプロパティの設定
                            if select_value:
                                properties[notion_col] = {
                                    "select": {"name": select_value}
                                }
                            else:
                                properties[notion_col] = {"select": None}  # 空欄
                        elif prop_type == 'date':
                            if value and value != '':
                                # 日付文字列をISO形式に変換
                                try:
                                    if isinstance(value, str):
                                        # 様々な日付形式に対応
                                        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S']:
                                            try:
                                                date_obj = datetime.strptime(value.split()[0], fmt.split()[0])
                                                properties[notion_col] = {
                                                    "date": {"start": date_obj.strftime('%Y-%m-%d')}
                                                }
                                                break
                                            except ValueError:
                                                continue
                                except Exception as date_error:
                                    print(f"⚠️ 日付変換エラー ({notion_col}: {value}): {date_error}")
                                    properties[notion_col] = {"date": None}
                        else:
                            # その他のタイプはテキストとして処理
                            properties[notion_col] = {
                                "rich_text": [{"text": {"content": str(value)}}]
                            }
                
                # Notionページを作成
                response = notion.pages.create(
                    parent={"database_id": NOTION_DATABASE_ID},
                    properties=properties
                )
                
                success_count += 1
                print(f"✅ データを正常に追加しました (SQLite ID: {data.get('id', 'N/A')})")
                
            except Exception as page_error:
                error_count += 1
                print(f"❌ ページ作成エラー (SQLite ID: {data.get('id', 'N/A')}): {page_error}")
                continue
        
        print(f"\n📊 処理結果:")
        print(f"  ✅ 成功: {success_count} 件")
        print(f"  ❌ エラー: {error_count} 件")
        print(f"  📄 合計: {len(data_list)} 件")
        
        return success_count, error_count
        
    except Exception as e:
        error_msg = f"Notion API接続エラー: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)


def main():
    """
    メイン処理
    """
    try:
        print("🚀 数値工程内検査データ更新スクリプトを開始します")
        print("=" * 60)
        
        # SQLiteから今日のデータを取得
        print("\n📥 SQLiteデータベースからデータを取得しています...")
        data_list = get_today_data_from_sqlite()
        
        if not data_list:
            print("ℹ️ 今日の日付に該当するデータがありませんでした。処理を終了します。")
            return
        
        # Notionデータベースを更新
        print("\n📤 Notionデータベースを更新しています...")
        success_count, error_count = update_notion_database(data_list)
        
        print("\n" + "=" * 60)
        print("🎉 処理が完了しました！")
        
        if error_count > 0:
            print(f"⚠️ {error_count} 件のエラーが発生しました。ログを確認してください。")
        
    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"\n❌ スクリプト実行中にエラーが発生しました:")
        print(error_detail)
        send_error_email(error_detail)
        sys.exit(1)


if __name__ == "__main__":
    main()