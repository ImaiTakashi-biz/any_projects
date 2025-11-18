"""
統合マスタファイル作成・更新スクリプト

1. 製品マスター.xlsからデータを読み込み、工程マスタ.xlsxを作成
2. Accessデータベースからデータを読み込み、スキルマスタCSVと製品マスタExcelを更新
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta

# ========= 設定値（あとから変更しやすいようにまとめる） =========

# ========= 工程マスタ作成用設定 =========
PRODUCT_MASTER_SOURCE_PATH = r"C:\Users\SEIZOU-20\Desktop\製品マスター.xls"  # 読み込みファイル（製品マスター）
PROCESS_MASTER_PATH = r"C:\Users\SEIZOU-20\Desktop\工程マスタ.xlsx"  # 書き込みファイル（工程マスタ）

# ========= スキルマスタ・製品マスタ更新用設定 =========
ACCESS_DB_PATH = r"\\192.168.1.200\共有\品質保証課\外観検査記録\外観検査記録照会.accdb"  # Accessファイルのパス
TABLE_NAME = "t_外観検査集計"  # 実績テーブル名
EXPORT_CSV_PATH = r"C:\Users\SEIZOU-20\Desktop\スキルマスタ_auto.csv"  # スキルマスタCSVの出力先

# 製品マスタ（Excel）の自動更新設定
PRODUCT_MASTER_XLSX_PATH = r"C:\Users\SEIZOU-20\Desktop\製品マスタ_auto.xlsx"  # このExcelを自動更新
PRODUCT_MASTER_SHEET_NAME = "Sheet1"
PRODUCT_MASTER_AVG_COL = "平均 / (作業時間按分)1個当たりの検査時間×60"

# 検査回数 → スキル のしきい値
HIGH_SKILL_MIN = 11   # 11回以上 → スキル1
MID_SKILL_MIN = 5     # 5〜10回   → スキル2
LOW_SKILL_MIN = 1     # 1〜4回    → スキル3

# どこまでの期間を見るか（Noneなら全期間）
USE_PERIOD_DAYS = None   # 直近365日を見る例 / Noneなら全期間

# ========= ここから下は基本いじらない想定 =========


def skill_from_count(n,
                     high_min=HIGH_SKILL_MIN,
                     mid_min=MID_SKILL_MIN,
                     low_min=LOW_SKILL_MIN):
    """検査回数 n からスキル(1/2/3/None)を返す"""
    if n is None or n == 0 or pd.isna(n):
        return None
    if n >= high_min:
        return 1
    elif n >= mid_min:
        return 2
    elif n >= low_min:
        return 3
    else:
        return None


def read_product_master(source_path: str) -> pd.DataFrame:
    """
    製品マスター.xlsからデータを読み込む
    
    Args:
        source_path: 読み込みファイルのパス
        
    Returns:
        読み込んだデータのDataFrame
    """
    print(f"製品マスターを読み込み中: {source_path} (シート名: 製品マスター)")
    
    # ファイルの存在確認
    if not Path(source_path).exists():
        raise FileNotFoundError(f"読み込みファイルが見つかりません: {source_path}")
    
    # Excelファイルを読み込み
    sheet_name = '製品マスター'
    file_ext = Path(source_path).suffix.lower()
    if file_ext == '.xls':
        try:
            df = pd.read_excel(source_path, engine='xlrd', sheet_name=sheet_name)
        except ImportError as e:
            error_msg = (
                f"xlrdライブラリがインストールされていません。\n"
                f"以下のコマンドでインストールしてください: pip install xlrd==2.0.1\n"
                f"エラー詳細: {e}"
            )
            print(f"エラー: {error_msg}")
            raise ImportError(error_msg) from e
        except Exception as e:
            error_msg = f"xlrdエンジンでの読み込みに失敗しました: {e}"
            print(f"エラー: {error_msg}")
            raise
    else:
        df = pd.read_excel(source_path, engine='openpyxl', sheet_name=sheet_name)
    
    print(f"読み込み完了: {len(df)}行のデータを取得")
    
    # 必要な列が存在するか確認
    required_columns = ['製品番号', '洗浄①', '工程②', '工程③', '工程④', '工程⑤', '工程⑥', '工程⑦', '工程⑧']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"必要な列が見つかりません: {missing_columns}")
    
    # 製品番号列がNaNまたは空文字列の行を除外
    df_filtered = df[df['製品番号'].notna() & (df['製品番号'].astype(str).str.strip() != '')].copy()
    
    print(f"フィルタリング後: {len(df_filtered)}行のデータ")
    
    return df_filtered


def create_process_master(df: pd.DataFrame, output_path: str) -> None:
    """
    工程マスタ.xlsxを作成する
    
    Args:
        df: 元のデータフレーム
        output_path: 出力ファイルのパス
    """
    print(f"工程マスタを作成中: {output_path} (シート名: Sheet1)")
    
    # 列名のマッピング
    column_mapping = {
        '製品番号': '品番',
        '洗浄①': '1',
        '工程②': '2',
        '工程③': '3',
        '工程④': '4',
        '工程⑤': '5',
        '工程⑥': '6',
        '工程⑦': '7',
        '工程⑧': '8'
    }
    
    # 必要な列のみを選択してマッピング
    df_output = df[list(column_mapping.keys())].copy()
    df_output = df_output.rename(columns=column_mapping)
    
    # 出力ディレクトリが存在しない場合は作成
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Excelファイルに書き込み
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_output.to_excel(writer, index=False, sheet_name='Sheet1')
    
    print(f"工程マスタの作成が完了しました: {output_path}")
    print(f"書き込み行数: {len(df_output)}行")


def create_skill_master() -> None:
    """Accessデータベースからデータを読み込み、スキルマスタCSVを作成する"""
    print("スキルマスタ作成処理を開始します")
    
    # Access接続
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={ACCESS_DB_PATH};"
    )
    
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        
        params = []
        where_clauses = ["[集計除外フラグ] = 0"]  # 集計対象のみ
        
        # 工程NOが0、1、2の場合は除外
        where_clauses.append("NOT (ISNUMERIC([工程NO]) AND (VAL([工程NO]) = 0 OR VAL([工程NO]) = 1 OR VAL([工程NO]) = 2))")
        
        # 工程NOが数字2桁（10-99）の場合は除外
        where_clauses.append("NOT (ISNUMERIC([工程NO]) AND LEN(CSTR([工程NO])) = 2 AND VAL([工程NO]) >= 10 AND VAL([工程NO]) <= 99)")
        
        if USE_PERIOD_DAYS is not None:
            date_from = datetime.today() - timedelta(days=USE_PERIOD_DAYS)
            where_clauses.append("[日付] >= ?")
            params.append(date_from)
        
        where_sql = " AND ".join(where_clauses)
        
        sql = f"""
        SELECT
            [品番],
            [品名],
            [工程NO],
            [検査員ID],
            [数量],
            [作業時間],
            [生産ロットID]
        FROM [{TABLE_NAME}]
        WHERE {where_sql}
        """
        
        df = pd.read_sql(sql, conn, params=params)
        print(f"Accessデータベースから{len(df)}行のデータを取得しました")
        
        # 検査回数ダミー列（1行＝1回としてカウント）
        df["検査回数ダミー"] = 1
        
        grouped = (
            df.groupby(["品番", "工程NO", "検査員ID"], as_index=False)
              .agg(検査回数=("検査回数ダミー", "count"))
        )
        
        print(f"各品番・各検査員の検査回数を集計しました: {len(grouped)}件")
        
        print("\n=== 各品番・各検査員の検査回数 ===")
        grouped_sorted = grouped.sort_values(["品番", "工程NO", "検査員ID"])
        for row in grouped_sorted.itertuples(index=False):
            print(f"品番: {row.品番}, 工程NO: {row.工程NO}, 検査員ID: {row.検査員ID}, 検査回数: {row.検査回数}")
        print(f"\n合計: {len(grouped_sorted)}件\n")
        
        # スキルを計算
        grouped["skill"] = grouped["検査回数"].map(skill_from_count)
        
        pivot = grouped.pivot_table(
            index=["品番", "工程NO"],
            columns="検査員ID",
            values="skill",
            aggfunc="first"
        ).reset_index()
        
        pivot.columns.name = None
        pivot = pivot.rename(columns={"工程NO": "工程"})
        
        # 既存スキルマスタCSVのヘッダ行を活かしつつ上書き出力
        if os.path.exists(EXPORT_CSV_PATH):
            existing_csv = pd.read_csv(EXPORT_CSV_PATH, nrows=3, header=None)
            cols = existing_csv.iloc[0].tolist()
            row1_values = existing_csv.iloc[0].tolist()
            row2_values = existing_csv.iloc[1].tolist()
            
            row1_first2 = [str(x) if not pd.isna(x) else '' for x in row1_values[:2]]
            row2_first2 = [str(x) if not pd.isna(x) else '' for x in row2_values[:2]]
            
            if row1_first2 == row2_first2 and len(existing_csv) > 2:
                row2_values = existing_csv.iloc[2].tolist()
            
            header_row1 = pd.DataFrame([row1_values], columns=cols)
            header_row2 = pd.DataFrame([row2_values], columns=cols)
            header_rows = pd.concat([header_row1, header_row2], ignore_index=True)
        else:
            cols = pivot.columns.tolist()
            header_row = pd.DataFrame([cols], columns=cols)
            header_row2 = pd.DataFrame([['#'] + [''] * (len(cols) - 1)], columns=cols)
            header_rows = pd.concat([header_row, header_row2], ignore_index=True)
        
        if len(header_rows) >= 2:
            header0 = header_rows.iloc[0].tolist()
            header1 = header_rows.iloc[1].tolist()
            if header0 == header1:
                fallback = ["#"] + [""] * (len(header_rows.columns) - 1)
                header_rows.iloc[1] = fallback
        
        pivot_aligned = pivot.reindex(columns=cols, fill_value=np.nan)
        key_cols = [cols[0], cols[1]]
        
        # ベクトル化された操作を使用
        for col in key_cols:
            if col in pivot_aligned.columns:
                pivot_aligned[col] = pivot_aligned[col].astype(str).replace('nan', np.nan)
        
        def merge_rows(group):
            """同じ品番+工程の行を統合し、NaNでない値を優先"""
            result = group.iloc[0].copy()
            for col in group.columns:
                if col not in key_cols:
                    non_nan_vals = group[col].dropna()
                    if len(non_nan_vals) > 0:
                        result[col] = non_nan_vals.iloc[-1]
                    else:
                        result[col] = np.nan
            return result
        
        pivot_aligned = pivot_aligned.groupby(key_cols, group_keys=False).apply(merge_rows, include_groups=False).reset_index()
        data_rows = pivot_aligned.sort_values(by=key_cols).reset_index(drop=True)
        data_rows_filled = data_rows.fillna('')
        output = pd.concat([header_rows, data_rows_filled], ignore_index=True)
        
        # CSVファイルに書き込み
        try:
            if os.path.exists(EXPORT_CSV_PATH):
                try:
                    os.remove(EXPORT_CSV_PATH)
                except PermissionError:
                    error_msg = f"ファイル '{EXPORT_CSV_PATH}' が他のプログラム（Excelなど）で開かれています。"
                    print(f"エラー: {error_msg}")
                    print("ファイルを閉じてから再度実行してください。")
                    raise
            
            output.to_csv(EXPORT_CSV_PATH, index=False, header=False, encoding="utf-8-sig")
            print("スキルマスタを出力しました:", EXPORT_CSV_PATH)
        except PermissionError as e:
            error_msg = f"CSVファイルへの書き込み権限がありません。"
            print(f"エラー: {error_msg}")
            print(f"ファイルパス: {EXPORT_CSV_PATH}")
            print("以下のいずれかを確認してください:")
            print("  1. ファイルがExcelなどで開かれていないか")
            print("  2. ファイルの書き込み権限があるか")
            print("  3. ディレクトリへの書き込み権限があるか")
            raise
        
    except Exception as e:
        print(f"エラー: スキルマスタ作成中にエラーが発生しました: {e}")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def update_product_master() -> None:
    """Accessデータベースからデータを読み込み、製品マスタExcelを更新する"""
    print("製品マスタ更新処理を開始します")
    
    # Access接続
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={ACCESS_DB_PATH};"
    )
    
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        
        params = []
        where_clauses = ["[集計除外フラグ] = 0"]
        
        # 工程NOが0、1、2の場合は除外
        where_clauses.append("NOT (ISNUMERIC([工程NO]) AND (VAL([工程NO]) = 0 OR VAL([工程NO]) = 1 OR VAL([工程NO]) = 2))")
        
        # 工程NOが数字2桁（10-99）の場合は除外
        where_clauses.append("NOT (ISNUMERIC([工程NO]) AND LEN(CSTR([工程NO])) = 2 AND VAL([工程NO]) >= 10 AND VAL([工程NO]) <= 99)")
        
        if USE_PERIOD_DAYS is not None:
            date_from = datetime.today() - timedelta(days=USE_PERIOD_DAYS)
            where_clauses.append("[日付] >= ?")
            params.append(date_from)
        
        where_sql = " AND ".join(where_clauses)
        
        sql = f"""
        SELECT
            [品番],
            [品名],
            [工程NO],
            [検査員ID],
            [数量],
            [作業時間],
            [生産ロットID]
        FROM [{TABLE_NAME}]
        WHERE {where_sql}
        """
        
        df = pd.read_sql(sql, conn, params=params)
        print(f"Accessデータベースから{len(df)}行のデータを取得しました")
        
        print("\n=== 製品マスタ用 一個当たり検査時間[秒]の集計 ===")
        
        # 品番・品名・工程・生産ロットIDごとに集計
        prod_grouped_by_lot = (
            df.groupby(["品番", "品名", "工程NO", "生産ロットID"], as_index=False)
              .agg(
                  数量=("数量", "first"),
                  作業時間=("作業時間", "sum")
              )
        )
        
        # 品番・品名・工程ごとに集計
        prod_grouped = (
            prod_grouped_by_lot.groupby(["品番", "品名", "工程NO"], as_index=False)
              .agg(
                  総数量=("数量", "sum"),
                  総作業時間分=("作業時間", "sum")
              )
        )
        
        # 1個当たり検査時間[秒]を計算
        prod_grouped["検査時間秒"] = np.where(
            prod_grouped["総数量"] > 0,
            prod_grouped["総作業時間分"] * 60.0 / prod_grouped["総数量"],
            np.nan
        )
        prod_grouped["検査時間秒"] = prod_grouped["検査時間秒"].round(2)
        
        print(f"製品マスタ用の集計が完了しました: {len(prod_grouped)}件")
        
        # 各品番・各工程の集計結果をターミナルに出力
        prod_grouped_sorted = prod_grouped.sort_values(["品番", "工程NO"])
        for row in prod_grouped_sorted.itertuples(index=False):
            inspection_time_str = f"{row.検査時間秒:.2f}" if pd.notna(row.検査時間秒) else ""
            print(f"品番: {row.品番}, 品名: {row.品名}, 工程NO: {row.工程NO}, "
                  f"総数量: {row.総数量}, 総作業時間分: {row.総作業時間分:.2f}, "
                  f"検査時間秒: {inspection_time_str}")
        print(f"\n合計: {len(prod_grouped_sorted)}件\n")
        
        # Excel製品マスタを読み込み
        if os.path.exists(PRODUCT_MASTER_XLSX_PATH):
            try:
                prod_master = pd.read_excel(PRODUCT_MASTER_XLSX_PATH, sheet_name=PRODUCT_MASTER_SHEET_NAME)
                if len(prod_master) == 0:
                    prod_master = pd.DataFrame(columns=prod_master.columns)
            except Exception as e:
                print(f"警告: 製品マスタExcelの読み込みに失敗しました: {e}")
                prod_master = pd.DataFrame()
        else:
            print(f"警告: 製品マスタExcelが見つかりません: {PRODUCT_MASTER_XLSX_PATH}")
            prod_master = pd.DataFrame()
        
        # 製品マスタが空の場合
        if len(prod_master) == 0:
            output_data = prod_grouped.copy()
            
            if "工程NO" in output_data.columns:
                output_data = output_data.rename(columns={"工程NO": "工程番号"})
            
            if len(prod_master.columns) > 0:
                output_data = output_data.reindex(columns=prod_master.columns.tolist(), fill_value=np.nan)
                if PRODUCT_MASTER_AVG_COL in output_data.columns:
                    output_data[PRODUCT_MASTER_AVG_COL] = prod_grouped["検査時間秒"].values
                if "品番" in output_data.columns:
                    output_data["品番"] = prod_grouped["品番"].values
                if "品名" in output_data.columns:
                    output_data["品名"] = prod_grouped["品名"].values
                if "工程番号" in output_data.columns:
                    output_data["工程番号"] = prod_grouped["工程NO"].values
            else:
                columns_to_keep = ["品番", "品名", "工程番号", "検査時間秒"]
                output_data = output_data[columns_to_keep]
            
            try:
                with pd.ExcelWriter(PRODUCT_MASTER_XLSX_PATH, engine="openpyxl", mode="w") as writer:
                    output_data.to_excel(writer, sheet_name=PRODUCT_MASTER_SHEET_NAME, index=False)
                print("製品マスタを出力しました（Accessデータベースから取得した内容）:", PRODUCT_MASTER_XLSX_PATH)
            except PermissionError:
                error_msg = f"製品マスタExcel '{PRODUCT_MASTER_XLSX_PATH}' が開かれています。"
                print(f"エラー: {error_msg}")
                print("Excelを閉じてから再度実行してください。")
                raise
            except Exception as e:
                print(f"エラー: 製品マスタ出力中にエラーが発生しました: {e}")
                raise
        else:
            # 既存の製品マスタがある場合
            prod_master["工程番号"] = prod_master["工程番号"].astype(str)
            prod_grouped["工程NO_str"] = prod_grouped["工程NO"].astype(str)
            
            merged = prod_master.merge(
                prod_grouped[["品番", "品名", "工程NO_str", "検査時間秒"]],
                left_on=["品番", "品名", "工程番号"],
                right_on=["品番", "品名", "工程NO_str"],
                how="left"
            )
            
            if PRODUCT_MASTER_AVG_COL not in merged.columns:
                merged[PRODUCT_MASTER_AVG_COL] = np.nan
            
            new_values = merged["検査時間秒"].round(2)
            merged[PRODUCT_MASTER_AVG_COL] = np.where(
                merged["検査時間秒"].notna(),
                new_values,
                merged[PRODUCT_MASTER_AVG_COL]
            )
            
            merged = merged.drop(columns=["工程NO_str", "検査時間秒"])
            
            try:
                with pd.ExcelWriter(PRODUCT_MASTER_XLSX_PATH, engine="openpyxl", mode="w") as writer:
                    merged.to_excel(writer, sheet_name=PRODUCT_MASTER_SHEET_NAME, index=False)
                print("製品マスタを更新しました:", PRODUCT_MASTER_XLSX_PATH)
            except PermissionError:
                error_msg = f"製品マスタExcel '{PRODUCT_MASTER_XLSX_PATH}' が開かれています。"
                print(f"エラー: {error_msg}")
                print("Excelを閉じてから再度実行してください。")
                raise
            except Exception as e:
                print(f"エラー: 製品マスタ更新中にエラーが発生しました: {e}")
                raise
        
    except Exception as e:
        print(f"エラー: 製品マスタ更新中にエラーが発生しました: {e}")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def main():
    """メイン処理"""
    try:
        # 1. 工程マスタを作成
        print("=" * 60)
        print("工程マスタ作成処理を開始します")
        print("=" * 60)
        df = read_product_master(PRODUCT_MASTER_SOURCE_PATH)
        create_process_master(df, PROCESS_MASTER_PATH)
        print("工程マスタ作成処理が完了しました")
        print(f"工程マスタの作成が完了しました: {PROCESS_MASTER_PATH}")
        
        # 2. スキルマスタを作成
        print("=" * 60)
        print("スキルマスタ作成処理を開始します")
        print("=" * 60)
        create_skill_master()
        print("スキルマスタ作成処理が完了しました")
        
        # 3. 製品マスタを更新
        print("=" * 60)
        print("製品マスタ更新処理を開始します")
        print("=" * 60)
        update_product_master()
        print("製品マスタ更新処理が完了しました")
        
        print("=" * 60)
        print("すべての処理が正常に完了しました")
        print("=" * 60)
        
    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
