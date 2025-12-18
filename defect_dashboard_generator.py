"""
昨日検査品 不具合分析ダッシュボード自動生成スクリプト

要件定義書_defect_dashboard_generator.md に基づく実装。
2つのAccess DB（外観検査集計 / 不具合情報）から昨日対象ロットの不具合を集計し、
過去3年の推移と合わせてSaaS風HTMLダッシュボードを生成する。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict
import re

import pandas as pd
import pyodbc
try:
    from jinja2 import Environment, FileSystemLoader, Template
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "jinja2 がインストールされていません。requirements.txt に追記済みです。"
        " `pip install -r requirements.txt` を実行してください。"
    ) from e

try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover
    genai = None

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

# Gemini クォータ超過時に以降の呼び出しを止めるためのフラグ
_GEMINI_QUOTA_EXCEEDED = False


# -----------------------------
# 設定
# -----------------------------

@dataclass
class Config:
    appearance_db_path: str = r"\\192.168.1.200\共有\品質保証課\外観検査記録\外観検査記録照会.accdb"
    appearance_table: str = "t_外観検査集計"
    defect_db_path: str = r"\\192.168.1.200\共有\品質保証課\外観検査記録\不具合情報記録.accdb"
    defect_table: str = "t_不具合情報"
    output_dir: str = "."
    template_path: Optional[str] = None  # 指定があれば外部HTMLテンプレートを利用
    logo_text: str = "ARAI"


DEFAULT_IGNORE_COLUMNS = {
    "生産ロットID", "指示日", "検査日", "日付", "検査日付", "品番", "品名", "工程NO", "工程", "号機", "時間",
    "数量", "総不具合数", "不良率",
}

FIXED_WORST_41ST_HINBANS = [
    "08121-26312A",
    "08121-26322A",
    "A41G1CA302",
    "20002100001-N",
    "06131-01710R",
    "06113-01310S",
    "FC00-1401-4",
    "MA1005-0518003",
    "06081-03911K",
    "H115A201G001-N",
    "4C-2205B",
]

FIXED_WORST_41ST_INFO: Dict[str, Dict[str, str]] = {
    "08121-26312A": {"品名": "ﾎﾝﾀｲ", "客先名": "不二プレシジョン", "主な不具合": "溝・内径寸法、外径・端面傷"},
    "08121-26322A": {"品名": "ﾎﾝﾀｲ", "客先名": "不二プレシジョン", "主な不具合": "溝・内径寸法、外径・端面傷"},
    "A41G1CA302": {"品名": "ｸﾛｽﾊﾞｰ", "客先名": "住友重機械工業", "主な不具合": "内径寸法、圧痕"},
    "20002100001-N": {"品名": "ﾍﾞｱﾘﾝｸﾞ受けC", "客先名": "ナカニシ", "主な不具合": "全長不良、傷、打痕、挽目"},
    "06131-01710R": {"品名": "ﾌﾟﾗﾝｼﾞｬ", "客先名": "不二テクノス", "主な不具合": "内径不良、傷、バリ、ムシレ"},
    "06113-01310S": {"品名": "ﾎﾙﾀﾞ", "客先名": "不二テクノス", "主な不具合": "全長・内径寸法、傷、ムシレ"},
    "FC00-1401-4": {"品名": "流量調整ﾕﾆｯﾄ本体", "客先名": "ハシダ技研工業", "主な不具合": "傷、打痕、偏心部ムシレ"},
    "MA1005-0518003": {"品名": "ﾍﾞｱﾘﾝｸﾞ受けJ", "客先名": "ナカニシ", "主な不具合": "内・外径寸法、傷、打痕"},
    "06081-03911K": {"品名": "ｷｭｳｲﾝｼ", "客先名": "不二テクノス", "主な不具合": "内・外径寸法、傷、バリ、ﾑｼﾚ"},
    "H115A201G001-N": {"品名": "ﾉｰｽﾞ", "客先名": "ナカニシ", "主な不具合": "内径寸法"},
    "4C-2205B": {"品名": "ｴﾝﾄﾞ", "客先名": "UEK", "主な不具合": "内径・ﾈｼﾞ、打痕、挽目、ﾑｼﾚ"},
}

# ARAIロゴ（Outlook-株式会社 新井精密.png をbase64埋め込み）
LOGO_BASE64 = (
    "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"
)


# -----------------------------
# 期判定 & Gemini
# -----------------------------

FIRST_TERM_NUMBER = 41
FIRST_TERM_START = date(2024, 10, 1)
FISCAL_YEAR_MONTHS = 12


@dataclass
class TermInfo:
    term_number: int
    start_date: date
    end_date: date


def get_term_info(target_date: date) -> TermInfo:
    months_diff = (target_date.year - FIRST_TERM_START.year) * 12 + (
        target_date.month - FIRST_TERM_START.month
    )
    term_offset = months_diff // FISCAL_YEAR_MONTHS
    term_number = FIRST_TERM_NUMBER + term_offset
    start_year = FIRST_TERM_START.year + term_offset
    start_date = date(start_year, FIRST_TERM_START.month, FIRST_TERM_START.day)
    end_date = date(start_year + 1, FIRST_TERM_START.month, FIRST_TERM_START.day) - timedelta(days=1)
    return TermInfo(term_number=term_number, start_date=start_date, end_date=end_date)


def get_previous_term_info(target_date: date) -> TermInfo:
    current = get_term_info(target_date)
    prev_start_year = current.start_date.year - 1
    prev_start = date(prev_start_year, FIRST_TERM_START.month, FIRST_TERM_START.day)
    prev_end = current.start_date - timedelta(days=1)
    prev_term_number = current.term_number - 1
    return TermInfo(term_number=prev_term_number, start_date=prev_start, end_date=prev_end)


def configure_gemini() -> None:
    if genai is None:
        raise RuntimeError("google-generativeai がインストールされていません。")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 GEMINI_API_KEY が設定されていません。")
    genai.configure(api_key=api_key)


def build_worst_part_prompt_for_term(
    term_info: TermInfo,
    part_number: str,
    part_name: str,
    customer: str,
    major_defects: str,
    trend_table: str,
    defect_kind_summary: str,
    today_qty: int,
    today_ng: int,
    today_rate: float,
    today_defect_kinds: str,
) -> str:
    term_label = f"{term_info.term_number}期（{term_info.start_date:%Y/%m/%d}〜{term_info.end_date:%Y/%m/%d}）"
    worst_label = f"{term_info.term_number}期ワースト品番"
    return f"""
以下は、当社（精密加工部品メーカー）における「{worst_label}」の
過去3年データと昨日の不具合データです。（対象期: {term_label}）

目的：製造がすぐ行動できる **短く要点だけのコメント** を作ること。
必ず **3〜6行以内** にまとめること。長文は禁止。

---
【対象】
品番: {part_number}
品名: {part_name}
客先: {customer}
主な不具合: {major_defects}

【過去3年の傾向】
{trend_table}

【不具合区分サマリ】
{defect_kind_summary}

【昨日の不具合】
検査数={today_qty}, 不良数={today_ng}, 不良率={today_rate:.2f}%
昨日の不具合: {today_defect_kinds}
---

以下の形式で **必ず** 出力してください（形式厳守）：

【評価】昨日の品質状態の一言評価（1行）
【判断】過去傾向と照らして「偶発か再発兆候か」の判断（1行）
【対策】製造がすぐ実施すべき対策（1〜2行）

【出力ルール】
- 必ず【評価】【判断】【対策】のラベルから始めること
- 各項目は1〜2行で完結すること
- 見出し・タイトル・品番の繰り返しは禁止
- **や##などのMarkdown装飾は禁止
- 「製造部各位」「品質報告」などの挨拶文は禁止
- 合計3〜6行以内に収めること
""".strip()


def generate_worst_part_comment(prompt: str, model_name: Optional[str] = None) -> str:
    if genai is None:
        return ""
    global _GEMINI_QUOTA_EXCEEDED
    if _GEMINI_QUOTA_EXCEEDED:
        return ""

    # モデル名は環境変数 GEMINI_MODEL で上書き可能。存在しない場合に備えフォールバックする。
    candidates = [
        model_name,
        os.environ.get("GEMINI_MODEL"),
        "gemini-1.5-pro-latest",
        "gemini-1.5-flash-latest",
        "gemini-2.0-flash",
    ]
    last_err: Optional[Exception] = None
    for name in [c for c in candidates if c]:
        try:
            model = genai.GenerativeModel(name)
            response = model.generate_content(prompt)
            return (response.text or "").strip()
        except Exception as e:  # pragma: no cover
            msg = str(e)
            if "429" in msg or "quota" in msg.lower() or "rate limit" in msg.lower():
                _GEMINI_QUOTA_EXCEEDED = True
                return ""
            last_err = e
            continue
    if last_err:
        raise last_err
    return ""


def build_general_part_prompt(
    part_number: str,
    part_name: str,
    customer: str,
    trend_table: str,
    defect_kind_summary: str,
    today_qty: int,
    today_ng: int,
    today_rate: float,
    today_defect_kinds: str,
) -> str:
    return f"""
以下は、当社（精密加工部品メーカー）における対象品番の
過去3年データと昨日の不具合データです。

目的：製造がすぐ行動できる **短く要点だけのコメント** を作ること。
必ず **3〜6行以内** にまとめること。長文は禁止。

---
【対象】
品番: {part_number}
品名: {part_name}
客先: {customer}

【過去3年の傾向】
{trend_table}

【不具合区分サマリ】
{defect_kind_summary}

【昨日の不具合】
検査数={today_qty}, 不良数={today_ng}, 不良率={today_rate:.2f}%
昨日の不具合: {today_defect_kinds}
---

以下の形式で **必ず** 出力してください（形式厳守）：

【評価】昨日の品質状態の一言評価（1行）
【判断】過去傾向と照らして「偶発か再発兆候か」の判断（1行）
【対策】製造がすぐ実施すべき対策（1〜2行）

【出力ルール】
- 必ず【評価】【判断】【対策】のラベルから始めること
- 各項目は1〜2行で完結すること
- 見出し・タイトル・品番の繰り返しは禁止
- **や##などのMarkdown装飾は禁止
- 「製造部各位」「品質報告」などの挨拶文は禁止
- 合計3〜6行以内に収めること
""".strip()

def load_config(path: Optional[str]) -> Config:
    if not path:
        return Config()
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    cfg = Config()
    for k, v in data.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg


def setup_logging(output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )


# -----------------------------
# Access 読み込み
# -----------------------------

def connect_access(db_path: str):
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={db_path};"
        r"ReadOnly=1;"
    )
    return pyodbc.connect(conn_str)


def read_access_table(db_path: str, table: str) -> pd.DataFrame:
    logging.info("reading Access table %s from %s", table, db_path)
    with connect_access(db_path) as conn:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="pandas only supports SQLAlchemy connectable*",
                category=UserWarning,
            )
            return pd.read_sql(f"SELECT * FROM {table}", conn)


def read_product_master(db_path: str) -> pd.DataFrame:
    table = "t_製品マスタ"
    try:
        df = read_access_table(db_path, table)
    except Exception as e:
        logging.warning("failed to read product master %s: %s", table, e)
        return pd.DataFrame()
    needed = {"製品番号", "製品名", "客先名"}
    if not needed.issubset(set(df.columns)):
        logging.warning("product master missing columns: %s", needed - set(df.columns))
        return pd.DataFrame()
    return df[list(needed)].drop_duplicates(subset=["製品番号"])


# -----------------------------
# データ整形・抽出
# -----------------------------

def find_date_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["指示日", "検査日", "検査日付", "日付", "実施日", "作成日"]
    for c in candidates:
        if c in df.columns:
            return c
    # datetime型らしい列をヒューリスティックに探す
    for c in df.columns:
        if "日" in c and df[c].dtype != object:
            return c
    return None


def normalize_dates(df: pd.DataFrame, col: Optional[str]) -> pd.DataFrame:
    if not col:
        return df
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def extract_today_lots(appearance_df: pd.DataFrame, run_date: datetime) -> pd.DataFrame:
    date_col = find_date_column(appearance_df)
    appearance_df = normalize_dates(appearance_df, date_col)

    if date_col:
        today_mask = appearance_df[date_col].dt.date == run_date.date()
        today_df = appearance_df.loc[today_mask].copy()
        logging.info("appearance rows for yesterday: %s", len(today_df))
    else:
        today_df = appearance_df.copy()
        logging.warning("no date column in appearance table; using all rows")

    if "生産ロットID" not in today_df.columns:
        raise KeyError("appearance table must include 生産ロットID")
    
    # 生産ロットIDが重複している場合、最初の1件のみを採用（数量は合算しない）
    before_count = len(today_df)
    today_df = today_df.drop_duplicates(subset=["生産ロットID"], keep="first")
    after_count = len(today_df)
    if before_count != after_count:
        logging.info("removed %s duplicate lot IDs", before_count - after_count)
    
    return today_df


def join_defects(today_lots_df: pd.DataFrame, defect_df: pd.DataFrame) -> pd.DataFrame:
    if "生産ロットID" not in defect_df.columns:
        raise KeyError("defect table must include 生産ロットID")
    lots = today_lots_df["生産ロットID"].dropna().astype(str).unique().tolist()
    defect_df = defect_df.copy()
    defect_df["生産ロットID"] = defect_df["生産ロットID"].astype(str)
    joined = defect_df[defect_df["生産ロットID"].isin(lots)].copy()
    # 不具合側に号機が無い場合、外観側から付与
    if "号機" not in joined.columns and "号機" in today_lots_df.columns:
        joined = joined.merge(
            today_lots_df[["生産ロットID", "号機"]],
            on="生産ロットID",
            how="left",
        )
    logging.info("defect rows for yesterday lots: %s", len(joined))
    return joined


def detect_defect_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []
    for c in df.columns:
        if c in DEFAULT_IGNORE_COLUMNS:
            continue
        if re.match(r"^ID\d+", str(c)) or str(c).startswith("ID"):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    if not cols and "総不具合数" in df.columns:
        cols = ["総不具合数"]
    return cols


def _summarize_defect_breakdown_row(row: pd.Series, defect_cols: List[str]) -> str:
    parts: List[str] = []
    for c in defect_cols:
        v = row.get(c, 0)
        try:
            if pd.isna(v) or float(v) <= 0:
                continue
        except Exception:
            continue
        parts.append(f"{c}{int(v)}")
    return "、".join(parts) if parts else "-"


def compute_today_summary(today_lots_df: pd.DataFrame, today_defects_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "品番" not in today_defects_df.columns and "品番" not in today_lots_df.columns:
        raise KeyError("品番 column not found in either table")

    key_col = "品番" if "品番" in today_defects_df.columns else "品番"
    group_keys: List[str] = [key_col]
    if "号機" in today_lots_df.columns or "号機" in today_defects_df.columns:
        group_keys.append("号機")
    # 指示日（ロット日）をグループキーに追加
    if "指示日" in today_lots_df.columns or "指示日" in today_defects_df.columns:
        group_keys.append("指示日")
    defect_cols = detect_defect_columns(today_defects_df)

    # 数量は外観側（あれば）→不具合側へフォールバック（合算せず最初の値を採用）
    qty_col = "数量" if "数量" in today_lots_df.columns else ("数量" if "数量" in today_defects_df.columns else None)
    if qty_col:
        if set(group_keys).issubset(set(today_lots_df.columns)):
            qty_by_hinban = today_lots_df.groupby(group_keys, as_index=False)[qty_col].first()
        else:
            qty_by_hinban = today_defects_df.groupby(group_keys, as_index=False)[qty_col].first()
    else:
        qty_by_hinban = today_defects_df[group_keys].drop_duplicates()
        qty_by_hinban["数量"] = 0

    # 総不具合数も合算せず最初の値を採用
    if "総不具合数" in today_defects_df.columns:
        total_def_by_hinban = today_defects_df.groupby(group_keys, as_index=False)["総不具合数"].first()
    else:
        total_def_by_hinban = today_defects_df.groupby(group_keys, as_index=False)[defect_cols].first()
        total_def_by_hinban["総不具合数"] = total_def_by_hinban[defect_cols].sum(axis=1)
        total_def_by_hinban = total_def_by_hinban[group_keys + ["総不具合数"]]

    summary = qty_by_hinban.merge(total_def_by_hinban, on=group_keys, how="outer").fillna(0)
    summary["不良率"] = summary.apply(
        lambda r: (r["総不具合数"] / r[qty_col]) if qty_col and r[qty_col] else 0.0,
        axis=1,
    )
    summary = summary.sort_values("不良率", ascending=False).reset_index(drop=True)

    # 区分別集計（見やすさ重視で1列にまとめる）- 合算せず最初の値を採用
    if defect_cols:
        defects_breakdown = today_defects_df.groupby(group_keys, as_index=False)[defect_cols].first()
        defects_breakdown["不具合内訳"] = defects_breakdown.apply(
            lambda r: _summarize_defect_breakdown_row(r, defect_cols),
            axis=1,
        )
        defects_breakdown = defects_breakdown[group_keys + ["不具合内訳"]]
    else:
        defects_breakdown = pd.DataFrame(columns=group_keys + ["不具合内訳"])

    # サマリーに内訳を統合
    summary = summary.merge(defects_breakdown, on=group_keys, how="left")
    summary["不具合内訳"] = summary["不具合内訳"].fillna("-")

    return summary, defects_breakdown


def filter_last_3years(defect_df: pd.DataFrame, run_date: datetime) -> pd.DataFrame:
    date_col = find_date_column(defect_df)
    defect_df = normalize_dates(defect_df, date_col)
    if not date_col:
        logging.warning("no date column in defect table; using all rows for 3-year stats")
        return defect_df
    cutoff = run_date - timedelta(days=365 * 3)
    return defect_df.loc[defect_df[date_col] >= cutoff].copy()


def compute_worst_hinban(defects_3y: pd.DataFrame) -> Optional[str]:
    if "品番" not in defects_3y.columns:
        return None
    qty_col = "数量" if "数量" in defects_3y.columns else None
    if "総不具合数" in defects_3y.columns:
        g = defects_3y.groupby("品番", as_index=False).agg({"総不具合数": "sum", **({qty_col: "sum"} if qty_col else {})})
        if qty_col:
            g["不良率"] = g["総不具合数"] / g[qty_col].replace(0, pd.NA)
        else:
            g["不良率"] = g["総不具合数"]
    else:
        defect_cols = detect_defect_columns(defects_3y)
        g = defects_3y.groupby("品番", as_index=False)[defect_cols].sum()
        g["総不具合数"] = g[defect_cols].sum(axis=1)
        g["不良率"] = g["総不具合数"]
    g = g.sort_values("不良率", ascending=False)
    return g.iloc[0]["品番"] if len(g) else None


def aggregate_trends(defects_3y: pd.DataFrame, target_hinbans: List[str], run_date: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "品番" not in defects_3y.columns:
        return pd.DataFrame(), pd.DataFrame()

    date_col = find_date_column(defects_3y)
    defects_3y = normalize_dates(defects_3y, date_col)
    if not date_col:
        return pd.DataFrame(), pd.DataFrame()

    defect_cols = detect_defect_columns(defects_3y)
    if "総不具合数" in defects_3y.columns:
        def_series = defects_3y["総不具合数"]
    else:
        def_series = defects_3y[defect_cols].sum(axis=1) if defect_cols else pd.Series(0, index=defects_3y.index)

    qty_col = "数量" if "数量" in defects_3y.columns else None
    base = defects_3y.copy()
    base["_defect_total"] = def_series
    base["_qty_total"] = base[qty_col] if qty_col else 0
    base = base[base["品番"].isin(target_hinbans)].copy()
    if base.empty:
        return pd.DataFrame(), pd.DataFrame()

    base["月"] = base[date_col].dt.to_period("M").dt.to_timestamp()
    base["四半期"] = base[date_col].dt.to_period("Q").dt.to_timestamp()

    monthly = base.groupby(["品番", "月"], as_index=False).agg({"_defect_total": "sum", "_qty_total": "sum"})
    monthly["不良率"] = monthly.apply(
        lambda r: (r["_defect_total"] / r["_qty_total"]) if r["_qty_total"] else 0.0,
        axis=1,
    )

    quarterly = base.groupby(["品番", "四半期"], as_index=False).agg({"_defect_total": "sum", "_qty_total": "sum"})
    quarterly["不良率"] = quarterly.apply(
        lambda r: (r["_defect_total"] / r["_qty_total"]) if r["_qty_total"] else 0.0,
        axis=1,
    )

    return monthly, quarterly


def make_auto_comment(monthly: pd.DataFrame, hinban: str) -> str:
    m = monthly[monthly["品番"] == hinban].sort_values("月")
    if len(m) < 3:
        return "過去データが少なく傾向判定できません。"
    last3 = m.tail(3)["不良率"].tolist()
    if last3[2] > last3[1] > last3[0]:
        return "直近3ヶ月で不良率が増加傾向です。要因の深掘りを推奨します。"
    if last3[2] < last3[1] < last3[0]:
        return "直近3ヶ月で不良率が改善傾向です。継続監視してください。"
    return "直近期で不良率は横ばいです。重点不具合の対策状況を確認してください。"


def compute_lot_history(defects_3y: pd.DataFrame, target_hinbans: List[str]) -> Dict[str, List[Dict[str, object]]]:
    """
    過去3年分のロット単位推移を返す。
    返却形式: {品番: [{生産ロットID, 日付, 号機, 数量, 総不具合数, 不良率}, ...]}
    """
    if defects_3y.empty or "品番" not in defects_3y.columns or "生産ロットID" not in defects_3y.columns:
        return {}

    date_col = find_date_column(defects_3y)
    defects_3y = normalize_dates(defects_3y, date_col)
    defect_cols = detect_defect_columns(defects_3y)

    base = defects_3y[defects_3y["品番"].isin(target_hinbans)].copy()
    if base.empty:
        return {}

    if "総不具合数" in base.columns:
        base["_defect_total"] = base["総不具合数"]
    else:
        base["_defect_total"] = base[defect_cols].sum(axis=1) if defect_cols else 0

    qty_col = "数量" if "数量" in base.columns else None
    base["_qty_total"] = base[qty_col] if qty_col else 0

    group_keys = ["品番", "生産ロットID"]
    if "号機" in base.columns:
        group_keys.append("号機")
    if date_col:
        group_keys.append(date_col)

    g = base.groupby(group_keys, as_index=False).agg({"_defect_total": "sum", "_qty_total": "sum"})
    g["不良率"] = g.apply(
        lambda r: (r["_defect_total"] / r["_qty_total"]) if r["_qty_total"] else 0.0,
        axis=1,
    )
    if date_col:
        g = g.sort_values(date_col)

    history: Dict[str, List[Dict[str, object]]] = {}
    for hinban, sub in g.groupby("品番"):
        rows: List[Dict[str, object]] = []
        for _, r in sub.iterrows():
            rows.append({
                "生産ロットID": str(r["生産ロットID"]),
                "日付": r[date_col].strftime("%Y-%m-%d") if date_col and pd.notna(r[date_col]) else "",
                "号機": str(r["号機"]) if "号機" in r else "",
                "数量": float(r["_qty_total"]),
                "総不具合数": float(r["_defect_total"]),
                "不良率": float(r["不良率"]),
            })
        history[str(hinban)] = rows
    return history


def build_trend_table_from_history(history_rows: List[Dict[str, object]], limit: int = 20) -> str:
    if not history_rows:
        return "過去ロットなし"
    rows = history_rows[-limit:]
    header = "日付, 生産ロットID, 号機, 数量, 不良数, 不良率"
    lines = [header]
    for r in rows:
        lines.append(
            f"{r.get('日付','')}, {r.get('生産ロットID','')}, {r.get('号機','')}, "
            f"{int(r.get('数量',0))}, {int(r.get('総不具合数',0))}, {r.get('不良率',0)*100:.2f}%"
        )
    return "\n".join(lines)


def build_trend_summary_from_history(history_rows: List[Dict[str, object]], recent_limit: int = 20) -> str:
    """
    過去3年の全体要約 + 直近期ロット表を返す。
    AIが「直近だけ」と誤解しないよう、期間・ロット数・年次傾向を明示する。
    """
    if not history_rows:
        return "過去3年のロットデータなし"

    # 全体期間
    dates = [r.get("日付") for r in history_rows if r.get("日付")]
    start = min(dates) if dates else ""
    end = max(dates) if dates else ""
    lot_count = len(history_rows)

    # 年次要約
    by_year: Dict[str, Dict[str, float]] = {}
    for r in history_rows:
        d = r.get("日付") or ""
        y = str(d)[:4] if d else "unknown"
        by_year.setdefault(y, {"qty": 0.0, "ng": 0.0})
        by_year[y]["qty"] += float(r.get("数量", 0) or 0)
        by_year[y]["ng"] += float(r.get("総不具合数", 0) or 0)

    year_lines = []
    for y in sorted(by_year.keys()):
        qty = by_year[y]["qty"]
        ng = by_year[y]["ng"]
        rate = (ng / qty * 100) if qty else 0.0
        year_lines.append(f"{y}: 検査数{int(qty)} / 不良数{int(ng)} / 不良率{rate:.2f}%")

    recent_table = build_trend_table_from_history(history_rows, limit=recent_limit)

    return "\n".join([
        f"【過去3年のロット推移 要約】",
        f"- 期間: {start} 〜 {end}",
        f"- ロット数: {lot_count}",
        *[f"- {l}" for l in year_lines],
        "",
        f"【直近期{recent_limit}ロットの詳細】",
        recent_table,
    ])


def build_defect_kind_summary(defects_3y: pd.DataFrame, hinban: str) -> str:
    if defects_3y.empty or "品番" not in defects_3y.columns:
        return "不具合区分データなし"
    sub = defects_3y[defects_3y["品番"].astype(str) == str(hinban)].copy()
    if sub.empty:
        return "不具合区分データなし"
    defect_cols = detect_defect_columns(sub)
    if not defect_cols:
        return "不具合区分データなし"
    sums = sub[defect_cols].sum().sort_values(ascending=False)
    total = float(sums.sum()) or 1.0
    parts = []
    for k, v in sums.head(6).items():
        if v <= 0:
            continue
        parts.append(f"{k}: {int(v)}件 ({v/total:.1%})")
    return " / ".join(parts) if parts else "不具合区分データなし"


# -----------------------------
# HTMLテンプレート
# -----------------------------

INLINE_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8"/>
  <title>Defect Dashboard {{ run_date }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <style>
    /* ========== ベーススタイル ========== */
    * { box-sizing: border-box; }
    body { 
      font-family: 'Segoe UI', 'Hiragino Sans', 'Meiryo', system-ui, sans-serif; 
      margin: 0; 
      background: linear-gradient(135deg, #f5f7fa 0%, #e4e8f0 100%);
      color:#1a1f36; 
      line-height: 1.6;
    }
    
    /* ========== ヘッダー ========== */
    header { 
      background: linear-gradient(135deg, #1e3a5f 0%, #0d47a1 50%, #1565c0 100%);
      padding: 20px 28px; 
      color: white; 
      position: relative; 
      overflow: hidden;
      box-shadow: 0 4px 20px rgba(13, 71, 161, 0.3);
    }
    header:before {
      content: '';
      position: absolute;
      top: -50%;
      right: -10%;
      width: 400px;
      height: 400px;
      background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
      border-radius: 50%;
    }
    header:after { 
      content: '';
      position: absolute;
      bottom: -30%;
      left: 20%;
      width: 300px;
      height: 300px;
      background: radial-gradient(circle, rgba(255,255,255,0.05) 0%, transparent 70%);
      border-radius: 50%;
    }
    .header-inner { display:flex; align-items:center; gap:16px; position:relative; z-index:1; }
    .brand-logo { 
      height: 50px; 
      width: auto; 
      background: white;
      padding: 8px 10px; 
      border-radius: 12px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }
    .brand-text { display:flex; flex-direction:column; gap:4px; }
    .brand-title { 
      font-weight: 800; 
      font-size: 24px; 
      letter-spacing: 0.5px; 
      line-height: 1.2;
      text-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .brand-subtitle { 
      opacity: 0.9; 
      font-weight: 500; 
      font-size: 14px;
      letter-spacing: 0.3px;
    }
    
    /* ========== メインコンテンツ ========== */
    main { padding: 24px 28px; max-width: 1280px; margin: 0 auto; }
    .card { 
      background: white; 
      border-radius: 12px; 
      padding: 20px 24px; 
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
      border: 1px solid #e5e7eb;
      margin-bottom: 20px;
    }
    
    /* ========== セクションヘッダー ========== */
    .section-header {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      margin: 4px 0 14px;
      border-radius: 8px;
      font-weight: 600;
      font-size: 15px;
    }
    .section-header .icon { font-size: 16px; }
    .section-header.worst { 
      background: #fef2f2;
      border: 1px solid #fecaca;
      color: #dc2626;
    }
    .section-header.normal { 
      background: #f0fdf4;
      border: 1px solid #bbf7d0;
      color: #16a34a;
    }
    .section-sub {
      font-size: 11px;
      font-weight: 500;
      color: inherit;
      opacity: 0.7;
      margin-left: auto;
    }
    .section-note {
      color: #6b7280;
      font-weight: 400;
    }
    
    /* ========== テーブル ========== */
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 12px 10px; text-align: right; vertical-align: top; }
    th { 
      text-align: left; 
      background: #f8fafc;
      position: sticky; 
      top: 0; 
      font-weight: 600; 
      color: #64748b;
      font-size: 12px;
      letter-spacing: 0.3px;
      border-bottom: 1px solid #e2e8f0;
    }
    tbody tr:not(.ai-row) {
      background: #ffffff;
      transition: background 0.15s ease;
    }
    tbody tr:not(.ai-row):hover {
      background: #f8fafc;
    }
    td.left { text-align: left; }
    td.key, td.name, td.customer, td.num { color: #1e293b; font-weight: 600; white-space: nowrap; font-size: 14px; }
    td.key { font-weight: 700; }
    td.num { font-variant-numeric: tabular-nums; }
    
    /* ========== バッジ ========== */
    .tag-badge { 
      display: inline-block; 
      width: 6px; 
      height: 6px; 
      margin-right: 8px; 
      border-radius: 50%; 
      background: #3b82f6;
    }
    
    /* ========== ロット一覧 ========== */
    .lot-list {
      margin: 0;
      padding: 0 0 0 20px;
      font-size: 13px;
      line-height: 1.6;
      list-style-type: disc;
    }
    .lot-list li {
      margin: 2px 0;
      padding: 0;
    }
    .lot-tag { 
      font-weight: 600; 
      color: #3b82f6;
      margin-right: 4px;
      font-size: 13px;
    }
    .lot-date { color: #64748b; font-size: 13px; margin-right: 6px; }
    .lot-qty, .lot-ng, .lot-rate { color: #475569; font-size: 13px; }
    .lot-breakdown { color: #475569; font-size: 13px; }
    .lot-qty.red, .lot-ng.red, .lot-rate.red, .lot-breakdown.red { 
      color: #dc2626; 
      font-weight: 600;
    }
    /* 不良率1%以下のロットはグレーアウト */
    .lot-list li.no-defect { color: #6b7280; }
    .lot-list li.no-defect .lot-tag { color: #6b7280; }
    .lot-list li.no-defect .lot-qty,
    .lot-list li.no-defect .lot-ng,
    .lot-list li.no-defect .lot-rate,
    .lot-list li.no-defect .lot-breakdown { color: #6b7280; }
    
    /* ========== 不良率テキスト ========== */
    .rate-text {
      font-weight: 600;
      font-size: 14px;
    }
    .rate-text.red {
      color: #dc2626;
    }
    
    /* ========== テーブル配置 ========== */
    table.summary th { white-space: nowrap; }
    table.summary th:nth-child(1),
    table.summary th:nth-child(2),
    table.summary th:nth-child(3),
    table.summary th:nth-child(7) { text-align: left; }
    table.summary th:nth-child(4),
    table.summary th:nth-child(5),
    table.summary th:nth-child(6) { text-align: right; }
    table.summary td:nth-child(1),
    table.summary td:nth-child(2),
    table.summary td:nth-child(3),
    table.summary td:nth-child(7) { text-align: left; }
    table.summary td:nth-child(4),
    table.summary td:nth-child(5),
    table.summary td:nth-child(6) { text-align: right; }
    
    /* ========== AIコメント ========== */
    .ai-row td { background: transparent; text-align: left; padding: 2px 10px 12px; border-bottom: 1px solid #e5e7eb; }
    .ai-comment {
      background: linear-gradient(135deg, #faf5ff 0%, #f3e8ff 100%);
      border-left: 4px solid #8b5cf6;
      padding: 8px 14px;
      white-space: pre-line;
      font-size: 13px;
      line-height: 1.6;
      text-align: left;
      border-radius: 0 10px 10px 0;
      color: #374151;
    }
    .ai-comment ol, .ai-comment ul { margin: 4px 0 0 20px; padding: 0; }
    .ai-comment li { margin: 2px 0; }
    .ai-comment p { margin: 0 0 4px; }
    .ai-comment.empty {
      background: #fafafa;
      border-left-color: #e5e7eb;
      color: #9ca3af;
      padding: 6px 14px;
    }
    .ai-title { 
      display: block;
      font-size: 12px; 
      font-weight: 700; 
      color: #7c3aed;
      margin-bottom: 4px;
    }
    .ai-comment.empty .ai-title { color: #9ca3af; }
    .ai-content { 
      display: block;
      padding-left: 8px;
    }
    .ai-content br { display: block; margin-bottom: 2px; }
    
    /* ========== その他 ========== */
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .muted { 
      color: #64748b; 
      font-size: 13px;
      padding: 12px 0 4px;
      border-top: 1px solid #e2e8f0;
      margin-top: 16px;
    }
    
    /* ========== レスポンシブ ========== */
    @media (max-width: 768px) {
      main { padding: 16px; }
      table { font-size: 13px; }
      .grid { grid-template-columns: 1fr; }
      .brand-logo { height: 40px; }
      .brand-title { font-size: 20px; }
      .card { padding: 16px; border-radius: 12px; }
    }
    @media (max-width: 640px) {
      table.summary thead { display: none; }
      table.summary, table.summary tbody, table.summary tr { display: block; width: 100%; }
      table.summary tr:not(.ai-row) {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 12px;
        margin: 0 0 12px 0;
        box-shadow: 0 1px 4px rgba(0,0,0,0.04);
      }
      table.summary td {
        display: flex;
        justify-content: space-between;
        gap: 10px;
        padding: 6px 0;
        border-bottom: none;
        text-align: right;
      }
      table.summary td::before {
        content: attr(data-label);
        font-weight: 600;
        color: #64748b;
        flex: 0 0 40%;
        text-align: left;
        font-size: 12px;
      }
      table.summary td.lot-cell {
        display: block;
        padding-top: 10px;
      }
      table.summary td.lot-cell::before {
        display: block;
        margin-bottom: 8px;
      }
      .lot-list { width: 100%; }
      .lot-list li { white-space: normal; }
      table.summary tr.ai-row { padding: 0; margin: 0 0 12px 0; }
      table.summary tr.ai-row td {
        display: block;
        padding: 0;
        text-align: left;
      }
      table.summary tr.ai-row td::before { content: none; }
      .ai-comment { width: 100%; box-sizing: border-box; }
    }
    footer { 
      text-align: center; 
      padding: 16px; 
      color: #94a3b8; 
      font-size: 12px;
      background: #f8fafc;
      border-top: 1px solid #e2e8f0;
    }
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <img class="brand-logo" src="{{ logo_data_uri }}" alt="ARAI logo"/>
      <div class="brand-text">
        <div class="brand-title">Defect Dashboard</div>
        <div class="brand-subtitle">検査日: {{ run_date }}</div>
      </div>
    </div>
  </header>
  <main>
    <div class="card">
      {% if worst_today_summary %}
      <div class="section-header worst">
        <span class="icon">⚠</span>
        <span>41期ワースト製品（{{ run_date_short }}分）</span>
        <span class="section-sub">重点監視対象</span>
      </div>
      <table class="summary">
        <thead>
          <tr>
            <th>品番</th>
            <th>品名</th>
            <th>客先名</th>
            <th>数量合計</th>
            <th>総不具合数合計</th>
            <th>不良率合計</th>
            <th>ロット一覧（不良率高い順）</th>
          </tr>
        </thead>
        <tbody>
          {% for row in worst_today_summary %}
          <tr>
            <td class="left key" data-label="品番"><span class="tag-badge"></span>{{ row["品番"] }}</td>
            <td class="left name" data-label="品名">{{ row.get("品名","") }}</td>
            <td class="left customer" data-label="客先名">{{ row.get("客先名","") }}</td>
            <td class="num" data-label="数量合計">{{ "{:,.0f}".format(row["数量合計"]) }}</td>
            <td class="num" data-label="総不具合数合計">{{ "{:,.0f}".format(row["総不具合数合計"]) }}</td>
            <td class="num" data-label="不良率合計">
              {% set rate = row["不良率合計"] %}
              <span class="rate-text {{ 'red' if rate > 0 else '' }}">{{ "{:.2%}".format(rate) }}</span>
            </td>
            <td class="left lot-cell" data-label="ロット一覧">
              <ul class="lot-list">
                {% for lot in row["ロット一覧"] %}
                  {% set lot_has_ng = (lot["不良率"]|float) > 0.01 %}
                  <li class="{{ '' if lot_has_ng else 'no-defect' }}">
                    <span class="lot-tag">{{ lot["号機"] }}</span>
                    <span class="lot-date">{{ lot["ロット日"] if lot["ロット日"] else "" }}</span>
                    <span class="lot-qty {{ 'red' if lot_has_ng else '' }}">数量{{ "{:,.0f}".format(lot["数量"]) }}</span>
                    <span class="lot-ng {{ 'red' if lot_has_ng else '' }}">不良{{ "{:,.0f}".format(lot["総不具合数"]) }}</span>
                    <span class="lot-rate {{ 'red' if lot_has_ng else '' }}">({{ "{:.2%}".format(lot["不良率"]) }})</span>
                    {% if lot["不具合内訳"] and lot["不具合内訳"] != "-" %}
                      <span class="lot-breakdown {{ 'red' if lot_has_ng else '' }}">：{{ lot["不具合内訳"] }}</span>
                    {% endif %}
                  </li>
                {% endfor %}
              </ul>
            </td>
          </tr>
          {% set hinban_key = row['品番'] | string | trim %}
          {% set has_ai = ai_comments.get(hinban_key) %}
          <tr class="ai-row">
            <td colspan="7">
              <div class="ai-comment {{ 'empty' if not has_ai else '' }}"><span class="ai-title">{% if has_ai %}✨ {% endif %}AI分析</span><span class="ai-content">{% if has_ai %}{{ has_ai }}{% else %}-{% endif %}</span></div>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      <div style="height:12px"></div>
      {% endif %}

      <div class="section-header normal">
        <span class="icon">📋</span>
        <span>{{ run_date_short }}サマリー</span>
        <span class="section-sub">不具合ロット検査結果一覧<span class="section-note">（不具合なしロットは非表示）</span></span>
      </div>
      <table class="summary">
        <thead>
          <tr>
            <th>品番</th>
            <th>品名</th>
            <th>客先名</th>
            <th>数量合計</th>
            <th>総不具合数合計</th>
            <th>不良率合計</th>
            <th>ロット一覧（不良率高い順）</th>
          </tr>
        </thead>
        <tbody>
          {% for row in today_summary %}
          <tr>
            <td class="left key" data-label="品番"><span class="tag-badge"></span>{{ row["品番"] }}</td>
            <td class="left name" data-label="品名">{{ row.get("品名","") }}</td>
            <td class="left customer" data-label="客先名">{{ row.get("客先名","") }}</td>
            <td class="num" data-label="数量合計">{{ "{:,.0f}".format(row["数量合計"]) }}</td>
            <td class="num" data-label="総不具合数合計">{{ "{:,.0f}".format(row["総不具合数合計"]) }}</td>
            <td class="num" data-label="不良率合計">
              {% set rate = row["不良率合計"] %}
              <span class="rate-text {{ 'red' if rate > 0 else '' }}">{{ "{:.2%}".format(rate) }}</span>
            </td>
            <td class="left lot-cell" data-label="ロット一覧">
              <ul class="lot-list">
                {% for lot in row["ロット一覧"] %}
                  {% set lot_has_ng = (lot["不良率"]|float) > 0.01 %}
                  <li class="{{ '' if lot_has_ng else 'no-defect' }}">
                    <span class="lot-tag">{{ lot["号機"] }}</span>
                    <span class="lot-date">{{ lot["ロット日"] if lot["ロット日"] else "" }}</span>
                    <span class="lot-qty {{ 'red' if lot_has_ng else '' }}">数量{{ "{:,.0f}".format(lot["数量"]) }}</span>
                    <span class="lot-ng {{ 'red' if lot_has_ng else '' }}">不良{{ "{:,.0f}".format(lot["総不具合数"]) }}</span>
                    <span class="lot-rate {{ 'red' if lot_has_ng else '' }}">({{ "{:.2%}".format(lot["不良率"]) }})</span>
                    {% if lot["不具合内訳"] and lot["不具合内訳"] != "-" %}
                      <span class="lot-breakdown {{ 'red' if lot_has_ng else '' }}">：{{ lot["不具合内訳"] }}</span>
                    {% endif %}
                  </li>
                {% endfor %}
              </ul>
            </td>
          </tr>
          {% set hinban_key = row['品番'] | string | trim %}
          {% set has_ai = ai_comments.get(hinban_key) %}
          <tr class="ai-row">
            <td colspan="7">
              <div class="ai-comment {{ 'empty' if not has_ai else '' }}"><span class="ai-title">{% if has_ai %}✨ {% endif %}AI分析</span><span class="ai-content">{% if has_ai %}{{ has_ai }}{% else %}-{% endif %}</span></div>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      <div class="muted">ワースト製品: {{ worst_lot_count }}ロット（不具合なし含む） / サマリー: {{ normal_lot_count }}ロット（不良率1%超）</div>
    </div>

  </main>
  <footer>Generated by defect_dashboard_generator.py</footer>
</body>
</html>
"""


def load_template(cfg: Config) -> Template:
    if cfg.template_path:
        tpath = Path(cfg.template_path)
        env = Environment(loader=FileSystemLoader(str(tpath.parent)))
        return env.get_template(tpath.name)
    return Environment().from_string(INLINE_TEMPLATE)


# -----------------------------
# メイン処理
# -----------------------------

def generate_dashboard(run_date: datetime, cfg: Config) -> Path:
    if load_dotenv is not None:
        load_dotenv()
    setup_logging(cfg.output_dir)

    appearance_df = read_access_table(cfg.appearance_db_path, cfg.appearance_table)
    defect_df = read_access_table(cfg.defect_db_path, cfg.defect_table)
    product_master_df = read_product_master(cfg.defect_db_path)

    today_lots_df = extract_today_lots(appearance_df, run_date)
    today_defects_df = join_defects(today_lots_df, defect_df)

    today_summary, defects_breakdown = compute_today_summary(today_lots_df, today_defects_df)

    if not product_master_df.empty and "品番" in today_summary.columns:
        pm = product_master_df.rename(
            columns={"製品番号": "品番", "製品名": "品名", "客先名": "客先名"}
        )
        today_summary = today_summary.merge(pm, on="品番", how="left")
    else:
        today_summary["品名"] = ""
        today_summary["客先名"] = ""

    defects_3y = filter_last_3years(defect_df, run_date)
    target_hinbans = sorted(today_summary["品番"].astype(str).unique().tolist()) if "品番" in today_summary.columns else []
    lot_history = compute_lot_history(defects_3y, target_hinbans)

    # ワースト品番と通常品番を分離
    worst_set = set(FIXED_WORST_41ST_HINBANS)
    if "品番" in today_summary.columns:
        mask_worst = today_summary["品番"].astype(str).isin(worst_set)
        # ワースト品番: 不具合なしロットも含めて全て表示
        worst_today_summary = today_summary.loc[mask_worst].copy()
        # 通常品番: 不良率 > 1% のみ表示
        normal_today_summary_all = today_summary.loc[~mask_worst].copy()
        if "不良率" in normal_today_summary_all.columns:
            normal_today_summary = normal_today_summary_all[normal_today_summary_all["不良率"] > 0.01].copy()
        else:
            normal_today_summary = normal_today_summary_all.copy()
    else:
        worst_today_summary = pd.DataFrame()
        normal_today_summary = today_summary.copy()

    # ロット数の集計
    worst_lot_count = len(worst_today_summary) if not worst_today_summary.empty else 0
    normal_lot_count = len(normal_today_summary) if not normal_today_summary.empty else 0

    # GeminiでAIコメント生成（固定ワーストは専用プロンプト、その他は一般プロンプト）
    ai_comments: Dict[str, str] = {}
    ai_status: str = ""
    global _GEMINI_QUOTA_EXCEEDED
    _GEMINI_QUOTA_EXCEEDED = False

    if os.environ.get("GEMINI_API_KEY"):
        try:
            configure_gemini()
            prev_term = get_previous_term_info(run_date.date())

            all_today_hinbans = (
                sorted(set(today_summary["品番"].astype(str).tolist()))
                if "品番" in today_summary.columns else []
            )

            for hinban in all_today_hinbans:
                hinban = str(hinban).strip()
                today_rows_all = today_summary[today_summary["品番"].astype(str) == hinban]
                today_qty = int(today_rows_all["数量"].sum()) if "数量" in today_rows_all.columns else 0
                today_ng = int(today_rows_all["総不具合数"].sum()) if "総不具合数" in today_rows_all.columns else 0
                part_name = (
                    today_rows_all["品名"].astype(str).dropna().iloc[0]
                    if "品名" in today_rows_all.columns and len(today_rows_all) else ""
                )
                customer = (
                    today_rows_all["客先名"].astype(str).dropna().iloc[0]
                    if "客先名" in today_rows_all.columns and len(today_rows_all) else ""
                )
                today_rate = (today_ng / today_qty * 100) if today_qty else 0.0
                today_defect_kinds = " / ".join(
                    [s for s in today_rows_all["不具合内訳"].astype(str).tolist() if s and s != "-"]
                ) if "不具合内訳" in today_rows_all.columns else ""

                history_rows = lot_history.get(hinban, [])
                trend_table_str = build_trend_summary_from_history(history_rows)
                defect_kind_summary_str = build_defect_kind_summary(defects_3y, hinban)

                if hinban in worst_set:
                    info = FIXED_WORST_41ST_INFO.get(hinban, {})
                    prompt = build_worst_part_prompt_for_term(
                        term_info=prev_term,
                        part_number=hinban,
                        part_name=info.get("品名", part_name),
                        customer=info.get("客先名", customer),
                        major_defects=info.get("主な不具合", ""),
                        trend_table=trend_table_str,
                        defect_kind_summary=defect_kind_summary_str,
                        today_qty=today_qty,
                        today_ng=today_ng,
                        today_rate=today_rate,
                        today_defect_kinds=today_defect_kinds,
                    )
                else:
                    prompt = build_general_part_prompt(
                        part_number=hinban,
                        part_name=part_name,
                        customer=customer,
                        trend_table=trend_table_str,
                        defect_kind_summary=defect_kind_summary_str,
                        today_qty=today_qty,
                        today_ng=today_ng,
                        today_rate=today_rate,
                        today_defect_kinds=today_defect_kinds,
                    )

                comment = generate_worst_part_comment(prompt)
                if comment:
                    ai_comments[hinban] = comment
                if _GEMINI_QUOTA_EXCEEDED:
                    ai_status = "Gemini API のクォータ上限に達したため、以降のAIコメント生成を停止しました。"
                    break
                # レートリミット対策: API呼び出し間に4秒の遅延を追加
                time.sleep(4)
        except Exception as e:
            ai_status = f"Gemini コメント生成に失敗しました（{e.__class__.__name__}）。"
            logging.warning("Gemini comment generation skipped: %s", e)
    else:
        ai_status = "Gemini未設定のためAIコメントを生成できません。（.env に GEMINI_API_KEY を設定してください）"
        logging.info("GEMINI_API_KEY not set; AI comments disabled.")

    # 品番単位にまとめて「ロット一覧」を作る（不良率高い順）
    def group_by_hinban(df: pd.DataFrame) -> List[Dict[str, object]]:
        if df.empty or "品番" not in df.columns:
            return []
        rows: List[Dict[str, object]] = []
        for hinban, sub in df.groupby("品番"):
            sub_sorted = sub.sort_values("不良率", ascending=False)
            lot_list: List[Dict[str, object]] = []
            for _, r in sub_sorted.iterrows():
                # ロット日（指示日）をフォーマット
                lot_date_val = r.get("指示日", "")
                if pd.notna(lot_date_val) and lot_date_val != "":
                    if hasattr(lot_date_val, "strftime"):
                        lot_date_str = lot_date_val.strftime("%m/%d")
                    else:
                        lot_date_str = str(lot_date_val)
                else:
                    lot_date_str = ""
                lot_list.append({
                    "号機": str(r.get("号機", "")),
                    "ロット日": lot_date_str,
                    "数量": float(r.get("数量", 0)),
                    "総不具合数": float(r.get("総不具合数", 0)),
                    "不良率": float(r.get("不良率", 0)),
                    "不具合内訳": str(r.get("不具合内訳", "-")),
                })
            qty_total = float(sub_sorted.get("数量", 0).sum()) if "数量" in sub_sorted.columns else 0.0
            ng_total = float(sub_sorted.get("総不具合数", 0).sum()) if "総不具合数" in sub_sorted.columns else 0.0
            rate_total = (ng_total / qty_total) if qty_total else 0.0
            first = sub_sorted.iloc[0]
            rows.append({
                "品番": str(hinban),
                "品名": str(first.get("品名", "")),
                "客先名": str(first.get("客先名", "")),
                "数量合計": qty_total,
                "総不具合数合計": ng_total,
                "不良率合計": rate_total,
                "ロット一覧": lot_list,
            })
        # 品番単位の並びも不良率合計高い順
        rows.sort(key=lambda x: x.get("不良率合計", 0), reverse=True)
        return rows

    worst_today_grouped = group_by_hinban(worst_today_summary)
    normal_today_grouped = group_by_hinban(normal_today_summary)

    template = load_template(cfg)
    html = template.render(
        run_date=run_date.strftime("%Y-%m-%d"),
        run_date_short=f"{run_date.month}/{run_date.day}",
        logo_text=cfg.logo_text,
        logo_data_uri=f"data:image/png;base64,{LOGO_BASE64}",
        today_summary=normal_today_grouped,
        worst_today_summary=worst_today_grouped,
        today_lot_count=int(today_lots_df["生産ロットID"].nunique()),
        today_defect_count=int(len(today_defects_df)),
        worst_lot_count=worst_lot_count,
        normal_lot_count=normal_lot_count,
        breakdown_columns=[],
        breakdown_rows=[],
        ai_comments=ai_comments,
        ai_status=ai_status,
    )

    out_path = Path(cfg.output_dir) / f"defect_dashboard_{run_date:%Y-%m-%d}.html"
    out_path.write_text(html, encoding="utf-8")
    logging.info("dashboard written: %s", out_path)
    return out_path


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate defect dashboard HTML")
    p.add_argument("--run-date", type=str, help="YYYY-MM-DD (default: yesterday)")
    p.add_argument("--config", type=str, help="path to JSON config")
    return p.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    # デフォルトは昨日の日付
    run_date = datetime.now() - timedelta(days=1)
    if args.run_date:
        run_date = datetime.strptime(args.run_date, "%Y-%m-%d")
    cfg = load_config(args.config)
    try:
        generate_dashboard(run_date, cfg)
    except Exception as e:
        logging.exception("failed to generate dashboard: %s", e)
        raise


if __name__ == "__main__":
    main()
