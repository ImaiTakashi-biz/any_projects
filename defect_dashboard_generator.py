"""
本日検査品 不具合分析ダッシュボード自動生成スクリプト

要件定義書_defect_dashboard_generator.md に基づく実装。
2つのAccess DB（外観検査集計 / 不具合情報）から本日対象ロットの不具合を集計し、
過去3年の推移と合わせてSaaS風HTMLダッシュボードを生成する。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
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
    "iVBORw0KGgoAAAANSUhEUgAAAYMAAABQCAYAAAD7uRknAAAe+ElEQVR4Ae2dBXQcRxL3+5jJFyf2eWd6pejIx8zMzMwQOmaGDWine6XEji945BzfBb4wH/vy7oXBn4/0JWtrumdk5TnsOKyv/rM9evJkFUnW9Gh2tvPeb/WeX6Sd7unqquqqrmKV+68xde/6mvYjgR/oa4Enor8YWFWpB3Ed0HgbVYIL9T0vUB8H9SB8+U601NMNrFgcyxubHor1xltb35XImdBhImNBtIZgVQJrrCryZGSHdQMflWKPkYmH1FvxfoALdSvwZRSDelM/wcCqxtBovBpwGf8OkHCemyD1xV6gLutV/EBdQuPYADwZnTkTGtvJCUKvTwjUkVzqwz0Rfacm42+ukurdXlO/vd5sP2HwUO2vbmy6P8Eci8cbGX+y31Rfojlfk8iY0DdDxji9F8gYzX2NYBUAhtbLIVMptPZOSGQr0H8vrdyI6LzOM6pTZj67L+N3E6wb+KgUK0f1bmZzAFMzgVY0sMphrOTUC6LxtoEn9O0Ye18QRNcbK/W8ZA5kLBKLaES/GnOz/PDJhxJs8TgGxZYXQ8a4VGfu/A70pYmMDbefTrAqUAvC1xu5Muh/duRLxaWVBZnIQZt+Xjrz2Y2XzbqBj0qA46HHjd6w254jE0/2hRrrkN0swrUGVjVqh1y/DNRp4QKfrOIO0UeSBdBUX/GE/pov4j/QXJzkB+qWqikDLtRtHW8wmoSVSuPeSFxmPKST4TXwQI36LfVmnzaz5Y3JhxJs/jjqjfYDIWdkLX8cMuYJvWXnTUjFkDGa930JVgXqrfaKVK4Ab4ZvTGRLqPdCtvxmuBeNfZ0v9U88qa+mn9sKXPfbPBHRO1Cn4Bk4yTieqSb0B/GM9DxvnfnspNiGCNYNfFQBKIP7JmfmQr94tonLxg76CbjtmB8sFljLxnKY6itwXAhrKQgbEBgIOcHmjyONFWAddZ9jdW02dlB1Vjb0gz2p3+4F+pPGIw+LWtNQBPDGaiTTeIZBoX2C7Qr4qASw8nCGyUV04D1tBoC39DPBs46+6H4E6wewYHFM4jfj1fXW5NM9GR3JA/1bW1YMl8lxgfBF+C1svPOFB+Gn8Xuw4OHBeEKdAWsLLN5z0Dv8ILrRl2pjEoMI4h/Rz/29YPy1iCksP3zTQwk2O45VB299Wl1G36Z5+8Ms83z7zNhBaolWGJKvqfutpPVTH9VP8IX6KK35z9v2EDyhb+zIRLiuPjz+ft7a/Ew8A+ScYLsCPqrBdKwgOm6uiYQGBenE9SMcVoyMvkTzMWZHGURn4uwYAkKw+YJjCBPXeb0PayfQUAptYOE5x2BVkQL6HGIKtUPCZQSbHQeX8St4oE+kubs4O59dYwdSPZ9g/UC9yeiPbllt3UPoGEdtHAPjO1cfu+n+BFsM+OhlzPFQuz4gw+ciWEITtGluraqOAqua6tEE60OgDJ4BQYVQl0kZpO8TFiV+f4Csdnh8YDojItA7QB7nrb6I4iSGItRaX+rX4OiINr0HEWwahzkeatdpvvaiuf83zVc0x9yGvgjXEj8gWD/ApqbuVV/ffuBjDtaeR2uKOMeiMrgY31EX46/Dd7L3HHsfgi0GfPQ005ak0O9fwGS2QZr+1s9wZN0UoQxyIPXo0vsj+WUh6e3m732rq4fggJytwPukOQrmOa87jJxtIFgfgbl6ZCevX63FXFiSrxPxHfVh9XSC5QE+ehkz8WGDWLuQtCtQD6LXYzL7Of/cE9EJxjq+tezKAGfQAFY8yM9DULfh73ARnTHTQ3CpqIQBShJyhk1onvN6u0lS+DdkDLEqgvUD9pUBUL/wZLTvY6R+PMHyAB89DYQ2zf1dcBDGpGHBBSZYP+IH+ifEpR4Cq2VXBgZsTMC2hzCdbeSYzrXHEdEC5/VqyJhJdWZ9QDHKQEaHzj97qNLZRGmgsV3HufcuB2tkEmxeTwHV3QjWj3i4sQtFGujre0cZtFcAv6mOArk9t/EQklvNrSiotcLXJ+exjb/cl2D9CDY2yBnuDUDOdiGr60bIGBd6hGD9wFBj28MR4/KC8ABr2URSj2J91g4KVxEsD/DRk2CR7kKsIEvY77ED5IPD4oNL3zPKwIC/D+xlauhvIlMDCoFg/QgUQSZWsFBu77fYAdLck1RpcxfDEgHeS57eKz56Ej+YGPRFFHChj1jEhF6PTdAT+m3wMIbWjT2AYP1EbyuDdh10Yh5A35HT82/vrIvwp7VW9tZm3/H6TlaQ2tXMmDv7LXaAlPV6c5wSHcK9nTIoAH/06mfhrJvYlIPLdQBy7pet2/ZwgvUTPa0MDFgHIPcgOOYlW+mxz8Bxx8KOh1zsIJvh6JSBJZDhUWvAUove0anJodViJ7Ym9LG+1MdQCt0qRplFyBcmWB9QCWWAi4bAE/kGwb1AX+4H+te1ZvROgvUTSK+FnPlBKKAIPKEXO7c38UAfjhvfkDEDqyJOGRQE8p0TCyPQIu9qlx4VucMZcR8FDCuhDODVAVwis1H50Vx6Y/0EjnTMufdJOc3nnZ3sr2gjZMzAKolTBgXRbCd55lzo3+VY5+NmL4i21ylViwv9QvREIFgfUAllMNBSHwVc6Mmc679s71jF6nvwSFNrth9ArABylqZs58BdkDFfRG3ImIFVEacMiuIg9XxrtcSD5CJTw3QNY31AJZQBvgfYqy+v1mJNIIONYP1AGivIe13g70HGDKyKOGVgF2S+PBwFoDyaYFhrKJVrQRmcShyLLk79EjuohGcwHD4VIH5kp0KkOgJrgjyp3QlWZXAMiwt3XKhDTKxge87zeTNkDFQ1duCUgWVwFwDnwp7Qh9nulIV0wt6JHbiYAd4VSButWGA9xmJSWVmVwThhtduqWZXGDkBlYwdOGdgBZWFhPcBaQcaPL+NTbU1wGjvgMvxc78QOXMxg5dH6wcAP9GanDHYNVL2EnMHzTix3Ef1fK3NpYgegorEDpwxsAesc1gOsdVgTaQ0dqwT6mB6KHbiYgWERPQ+cZ4Bb1mvSoo/62rQAoE2qGTtwysAasM6R8okOQlAEXqBvxmRY5ixfxOtrdA5db0xRbZrGvQlWQSqhDGAwAOcZ7DqIh0DOkETRMbjUrdZbNjb1TwFkzMAqgFMGtoB1bioAHoNJKBK4zPj+dKFWERczcMqge6zAPmnsADJmYBXAKYO8gTWOTXhQTDwF1oMv1eldAlE7rLqzreiLmPAKpxRWQhmskmM1YCubiFONIqQ0V7GgITwqyBnKRMAbRlmPbKE5I2O5ewrwQADWiYFVAKcMcsa02YvrvLX1XbPECnbY7jPKpToTVqFpqsKqiLtn0N/3DFC+HWPDGGe7F5CpTWSD9QZWAZwyyJvH0SLFYLmMPzczVuBNd1CCIgjRu/Y0exMenQ+F4Mno2eWNHbiYwWBTvRnkfQM5tYoJUW9MrkBpYoJViT2DySFTonp95ghnB9aEJ9VGkrOz0HPXptEFKhI7cMogbwbFlhdjgRrrfCoFl82weaGpScei0S+uZuzAxQxcbSL78CD8dKII5N2Oh9pYF8guyvQzsEZFYgdOGeQZK3h5YwqZIa9GJzIu9J8yLnuEvr3o9GPOcJ+PiorA1sSjyxMKdzllUNaqpfEPQM7KAEx4Ul/uCbUfwapEKmekDD5n5CxTCj7aRBzHm+or9YPaz+dSfT+RsUBbyzKCjBlYD+OUQe6xglm7BEUX4gIaldgdItjKUb0bLBssXBc76M+YAaxYgCPEnD3Cs+BxoPERwarEjFjB8fOJk6SeOrHBxQ5czKAQhprxcj46+Qwuou9lzjBvNZbJ3/3ReDUf2TxAdJSBjNZwqX9iceI3wHqC1YIXDYuKYFWgl5UBmhEBJBGAnJQBlMAdZr39Hx7oTxLPIFiVWCm078nxZ3tNdUZ3OQsFPO9l1M+XYPh/EzkT+lxbcgYZA6uP3XR/A+tBnDLICy7jV/BAn+iLTMBKqrhjNYTfIlgKo425qNhBnc6Osy+g1+llZZDGCvKPEUVbOimW0T4EqyKQI7z3bAaWiR2sR9tGgqVkPXabwPMHq6k8BsF6DacMFgmsbUwiH1ZvIkE8mwZ7RTagBcvEF9EXCZYCZVBU7KCOYBqEpBHXCVYFelEZYGMCaFcK8qyb4wf6Tl9E/6Kf5+LWO8GqRCpn/nDYSHoVyGinlFHe+bc19O5eT7AU9PWFnNUC/TnbsQN+cPwK0KfKwCkDlM7NZi1kF2lyhtmaXEGwDIXEDmAtYuNE4w+CVYFeVAbowAUsvN/tBObhW7BMK+MBZppD4b0gDrbQOxVFxQ6wHkEq672GUwY5lJ2A1Q2rpNsZJg/UX2GZ7NmY2J1gGYqKHfwb1pRHz1lc7MDFDJD5AvwjrnsU8OioAuRZVrkTb1AxD/CO1X6IS61YFy8nWJVA3AtyxoXKlp3YkawBqZozYwUZCokdQMYAPHDIGcF6DKcMFgOs7WRTImGcT6wALFXsIPsi7OFiBihhntYe8gO1BuT+Pju3a9v1VvRlk6m2jGBVZLZYAe/IXmOOshCFxg6wmeJ5yqEQnDKwj1MGThk4ZOGThlgLLQqIOSTXFDyWAsLm9Evw0XjQxsV0AxLuAH0fXAwgtJLit5IjysoxDi1QTrIewFkAO1MSkXEcRfhVCkILURQeE0hdgGnSPHaGu9Fb2+X5XBHr+ceMiydWMP96X6ZSJnmWNYWrO/8YT6Sm0kfGoiY1NT9yLYQkHbTMjYgFDvNXK23cIltNsB9gzI2cMb4TKClRSnDHY1VoBNqFsWitfSryXYYigqdgAvBtZXL9ZiLz6bSH0c35f22rUBNiW8735WBvCok7Uv1XHdYgXpmiXPYYhguwqUAb6n1gpfbzt2gFvneObS3+9xymB+oLG4qTZ5uik5fKcZ3HYi5IE6HGWJh9ZNPYBgiyH1LEgQDgE23VhYX7BcCNZbWDsmOmcgGH/twAHqaRCOFLSOXNVUjyNl/QHMm6XeuncmoFWjVOejiCGsZPaeqfsQrB+o0/yiMQ3Nw792kjOpt2HeayL8MBRGHsXgIGN+sGUQMsap8ZQ9jy/aCDkrvdHllMH8wABwAAAAD"
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
過去3年データと本日の不具合データです。（対象期: {term_label}）

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

【本日の不具合】
検査数={today_qty}, 不良数={today_ng}, 不良率={today_rate:.2f}%
本日の不具合: {today_defect_kinds}
---

以下の形式で簡潔にまとめてください：

① 今日の品質状態の一言評価  
② 過去傾向と照らして「偶発か再発兆候か」の判断  
③ 製造が今日すぐ実施すべき対策を 1〜2 行

※ 文章は **必ず3〜6行以内**
※ 詳しい理屈や長い説明は禁止
※ 読み手が迷わず理解できる表現にすること
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
過去3年データと本日の不具合データです。

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

【本日の不具合】
検査数={today_qty}, 不良数={today_ng}, 不良率={today_rate:.2f}%
本日の不具合: {today_defect_kinds}
---

以下の形式で簡潔にまとめてください：

① 今日の品質状態の一言評価  
② 過去傾向と照らして「偶発か再発兆候か」の判断  
③ 製造が今日すぐ実施すべき対策を 1〜2 行

※ 文章は **必ず3〜6行以内**
※ 詳しい理屈や長い説明は禁止
※ 読み手が迷わず理解できる表現にすること
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
        logging.info("appearance rows for today: %s", len(today_df))
    else:
        today_df = appearance_df.copy()
        logging.warning("no date column in appearance table; using all rows")

    if "生産ロットID" not in today_df.columns:
        raise KeyError("appearance table must include 生産ロットID")
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
    logging.info("defect rows for today lots: %s", len(joined))
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
    defect_cols = detect_defect_columns(today_defects_df)

    # 数量は外観側（あれば）→不具合側へフォールバック
    qty_col = "数量" if "数量" in today_lots_df.columns else ("数量" if "数量" in today_defects_df.columns else None)
    if qty_col:
        if set(group_keys).issubset(set(today_lots_df.columns)):
            qty_by_hinban = today_lots_df.groupby(group_keys, as_index=False)[qty_col].sum()
        else:
            qty_by_hinban = today_defects_df.groupby(group_keys, as_index=False)[qty_col].sum()
    else:
        qty_by_hinban = today_defects_df[group_keys].drop_duplicates()
        qty_by_hinban["数量"] = 0

    if "総不具合数" in today_defects_df.columns:
        total_def_by_hinban = today_defects_df.groupby(group_keys, as_index=False)["総不具合数"].sum()
    else:
        total_def_by_hinban = today_defects_df.groupby(group_keys, as_index=False)[defect_cols].sum()
        total_def_by_hinban["総不具合数"] = total_def_by_hinban[defect_cols].sum(axis=1)
        total_def_by_hinban = total_def_by_hinban[group_keys + ["総不具合数"]]

    summary = qty_by_hinban.merge(total_def_by_hinban, on=group_keys, how="outer").fillna(0)
    summary["不良率"] = summary.apply(
        lambda r: (r["総不具合数"] / r[qty_col]) if qty_col and r[qty_col] else 0.0,
        axis=1,
    )
    summary = summary.sort_values("不良率", ascending=False).reset_index(drop=True)

    # 区分別集計（見やすさ重視で1列にまとめる）
    if defect_cols:
        defects_breakdown = today_defects_df.groupby(group_keys, as_index=False)[defect_cols].sum()
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
    body { font-family: system-ui, sans-serif; margin: 0; background:#f4f7fb; color:#1a1f36; }
    header { background: radial-gradient(1200px circle at 0% 0%, #5db3ff 0%, #0b5ed7 45%, #083a96 100%); padding: 18px 22px; color: white; position: relative; overflow:hidden; }
    header:after { content:''; position:absolute; inset:-40% -10% auto auto; width:420px; height:420px; background: rgba(255,255,255,0.08); border-radius:50%; transform: rotate(12deg); }
    .header-inner { display:flex; align-items:center; gap:14px; position:relative; z-index:1; }
    .brand-logo { height:44px; width:auto; background: rgba(255,255,255,.9); padding:6px 8px; border-radius:10px; }
    .brand-text { display:flex; flex-direction:column; gap:2px; }
    .brand-title { font-weight: 900; font-size: 20px; letter-spacing: .4px; line-height:1.2; }
    .brand-subtitle { opacity: .95; font-weight:600; font-size:13px; }
    main { padding: 18px 22px; max-width: 1200px; margin: 0 auto; }
    .card { background: white; border-radius: 12px; padding: 16px 18px; box-shadow: 0 1px 4px rgba(16,24,40,.06); margin-bottom: 16px;}
    h2 { margin: 0 0 10px; font-size: 18px; }
    table { width:100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 9px 8px; border-bottom: 1px solid #e6eaf2; text-align: right; vertical-align: top; }
    th { text-align: left; background:#f8fafc; position: sticky; top:0; font-weight: 700; color:#344054; }
    tbody tr:nth-child(even):not(.ai-row) { background:#fcfdff; }
    td.left { text-align: left; }
    td.key, td.name, td.customer, td.num { color:#101828; font-weight:700; }
    td.key { font-size:15px; letter-spacing:.2px; }
    td.name { font-size:14px; }
    td.customer { font-size:13.5px; }
    td.machine { font-weight:600; color:#1a1f36; }
    td.num { font-variant-numeric: tabular-nums; }
    .tag-badge { display:inline-flex; align-items:center; justify-content:center; width:20px; height:20px; margin-right:6px; border-radius:6px; background:#ffec99; color:#7f2d00; font-size:13px; font-weight:900; box-shadow: inset 0 0 0 1px #ffd43b; }
    .lot-list {
      margin: 0;
      padding: 6px 10px 6px 22px;
      font-size: 12.5px;
      line-height: 1.5;
      background: #eef4ff;
      border: 1px solid #dbe4ff;
      border-radius: 6px;
    }
    .lot-list li { margin: 2px 0; }
    .lot-tag { font-weight:700; color:#0b5ed7; }
    .lot-metrics { color:#344054; }
    .lot-metrics.red { color:#c92a2a; font-weight:600; }
    /* サマリテーブルのヘッダ/データ位置を一致させる（新レイアウト） */
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
    .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-weight:600; font-size:12px;}
    .pill.blue { background:#e7f5ff; color:#0b5ed7; }
    .pill.red { background:#ffe3e3; color:#c92a2a; }
    .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .muted { color:#667085; font-size:12px; }
    .section-header {
      display:flex;
      align-items:center;
      gap:8px;
      padding:8px 10px;
      margin:2px 0 8px;
      border-radius:10px;
      font-weight:800;
      font-size:16px;
      letter-spacing:.2px;
      color:#101828;
      background:#f1f5ff;
      border:1px solid #dbe4ff;
    }
    .section-header .icon { font-size:18px; }
    .section-header.worst { background:#fff4e6; border-color:#ffe8cc; color:#7f2d00; }
    .section-header.normal { background:#eef8f3; border-color:#d3f9d8; color:#0f5132; }
    .section-sub { font-size:11.5px; font-weight:600; color:inherit; opacity:.75; margin-left:auto; }
    .ai-row td { background:#f9fbff; text-align:left; padding:2px 8px; }
    .ai-comment {
      background:#f8fafc;
      border-left:3px solid #0b5ed7;
      padding:4px 10px;
      white-space:pre-line;
      font-size:12.5px;
      line-height:1.55;
      text-align:left;
      border-radius:6px;
      color:#101828;
    }
    .ai-comment ol, .ai-comment ul { margin:4px 0 0 18px; padding:0; }
    .ai-comment li { margin:2px 0; }
    .ai-comment p { margin:0 0 4px; }
    .ai-comment.empty {
      background:#ffffff;
      border-left-color:#d0d5dd;
      color:#667085;
    }
    .ai-title { font-size:12px; font-weight:700; margin:0 0 1px; color:#0b5ed7; letter-spacing:.2px;}
    .ai-meta { font-size:11px; color:#98a2b3; margin-left:6px; font-weight:500; }
    @media (max-width: 768px) {
      main { padding: 12px; }
      table { font-size: 13px; }
      .grid { grid-template-columns: 1fr; }
      .brand-logo { height:36px; }
      .brand-title { font-size:18px; }
    }
    @media (max-width: 640px) {
      table.summary thead { display:none; }
      table.summary, table.summary tbody, table.summary tr { display:block; width:100%; }
      table.summary tr:not(.ai-row) {
        background:#ffffff;
        border:1px solid #e6eaf2;
        border-radius:10px;
        padding:6px 8px;
        margin:0 0 8px 0;
      }
      table.summary td {
        display:flex;
        justify-content:space-between;
        gap:8px;
        padding:4px 0;
        border-bottom:none;
        text-align:right;
      }
      table.summary td::before {
        content: attr(data-label);
        font-weight:600;
        color:#667085;
        flex:0 0 42%;
        text-align:left;
      }
      table.summary td.lot-cell {
        display:block;
        padding-top:6px;
      }
      table.summary td.lot-cell::before {
        display:block;
        margin-bottom:4px;
      }
      .lot-list { width:100%; word-break: break-word; }
      .lot-metrics { word-break: break-word; }
      /* AIコメント行はカード外で全幅・左寄せ */
      table.summary tr.ai-row { padding:0; margin:0 0 10px 0; }
      table.summary tr.ai-row td {
        display:block;
        padding:4px 0;
        text-align:left;
      }
      table.summary tr.ai-row td::before { content: none; }
      .ai-comment { width:100%; box-sizing:border-box; }
    }
    footer { text-align:center; padding: 12px; color:#98a2b3; font-size:12px; }
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <img class="brand-logo" src="{{ logo_data_uri }}" alt="ARAI logo"/>
      <div class="brand-text">
        <div class="brand-title">{{ logo_text }} Defect Dashboard</div>
        <div class="brand-subtitle">検査日: {{ run_date }}</div>
      </div>
    </div>
  </header>
  <main>
    <div class="card">
      {% if worst_today_summary %}
      <div class="section-header worst">
        <span class="icon">⚠</span>
        <span>41期ワースト製品（本日分）</span>
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
            <td class="left key" data-label="品番"><span class="tag-badge">🏷</span>{{ row["品番"] }}</td>
            <td class="left name" data-label="品名">{{ row.get("品名","") }}</td>
            <td class="left customer" data-label="客先名">{{ row.get("客先名","") }}</td>
            <td class="num" data-label="数量合計">{{ "{:,.0f}".format(row["数量合計"]) }}</td>
            <td class="num" data-label="総不具合数合計">{{ "{:,.0f}".format(row["総不具合数合計"]) }}</td>
            <td>
              {% set rate = row["不良率合計"] %}
              <span class="pill {{ 'blue' if rate == 0 else 'red' }}">{{ "{:.2%}".format(rate) }}</span>
            </td>
            <td class="left lot-cell" data-label="ロット一覧">
              <ul class="lot-list">
                {% for lot in row["ロット一覧"] %}
                  {% set lot_has_ng = (lot["総不具合数"]|float) > 0 or (lot["不良率"]|float) > 0 %}
                  <li>
                    <span class="lot-tag">{{ lot["号機"] }}</span>
                    <span class="lot-metrics {{ 'red' if lot_has_ng else '' }}">
                      数量{{ "{:,.0f}".format(lot["数量"]) }},
                      不良{{ "{:,.0f}".format(lot["総不具合数"]) }}
                      ({{ "{:.2%}".format(lot["不良率"]) }})
                      {% if lot["不具合内訳"] and lot["不具合内訳"] != "-" %}
                        ：{{ lot["不具合内訳"] }}
                      {% endif %}
                    </span>
                  </li>
                {% endfor %}
              </ul>
            </td>
          </tr>
          {% set hinban_key = row['品番'] | string | trim %}
          {% set has_ai = ai_comments.get(hinban_key) %}
          <tr class="ai-row">
            <td colspan="7">
              <div class="ai-comment {{ 'empty' if not has_ai else '' }}">
                <div class="ai-title">AI分析コメント{% if not has_ai %}<span class="ai-meta">未生成</span>{% endif %}</div>
                {% if has_ai %}
                  {{ has_ai }}
                {% else %}
                  {{ ai_status if ai_status else "AIコメントは生成されていません。（Gemini未設定／クォータ超過／対象データ不足など）" }}
                {% endif %}
              </div>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      <div style="height:12px"></div>
      {% endif %}

      <div class="section-header normal">
        <span class="icon">📋</span>
        <span>本日サマリー</span>
        <span class="section-sub">検査結果一覧</span>
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
            <td class="left key" data-label="品番"><span class="tag-badge">🏷</span>{{ row["品番"] }}</td>
            <td class="left name" data-label="品名">{{ row.get("品名","") }}</td>
            <td class="left customer" data-label="客先名">{{ row.get("客先名","") }}</td>
            <td class="num" data-label="数量合計">{{ "{:,.0f}".format(row["数量合計"]) }}</td>
            <td class="num" data-label="総不具合数合計">{{ "{:,.0f}".format(row["総不具合数合計"]) }}</td>
            <td>
              {% set rate = row["不良率合計"] %}
              <span class="pill {{ 'blue' if rate == 0 else 'red' }}">{{ "{:.2%}".format(rate) }}</span>
            </td>
            <td class="left lot-cell" data-label="ロット一覧">
              <ul class="lot-list">
                {% for lot in row["ロット一覧"] %}
                  {% set lot_has_ng = (lot["総不具合数"]|float) > 0 or (lot["不良率"]|float) > 0 %}
                  <li>
                    <span class="lot-tag">{{ lot["号機"] }}</span>
                    <span class="lot-metrics {{ 'red' if lot_has_ng else '' }}">
                      数量{{ "{:,.0f}".format(lot["数量"]) }},
                      不良{{ "{:,.0f}".format(lot["総不具合数"]) }}
                      ({{ "{:.2%}".format(lot["不良率"]) }})
                      {% if lot["不具合内訳"] and lot["不具合内訳"] != "-" %}
                        ：{{ lot["不具合内訳"] }}
                      {% endif %}
                    </span>
                  </li>
                {% endfor %}
              </ul>
            </td>
          </tr>
          {% set hinban_key = row['品番'] | string | trim %}
          {% set has_ai = ai_comments.get(hinban_key) %}
          <tr class="ai-row">
            <td colspan="7">
              <div class="ai-comment {{ 'empty' if not has_ai else '' }}">
                <div class="ai-title">AI分析コメント{% if not has_ai %}<span class="ai-meta">未生成</span>{% endif %}</div>
                {% if has_ai %}
                  {{ has_ai }}
                {% else %}
                  {{ ai_status if ai_status else "AIコメントは生成されていません。（Gemini未設定／クォータ超過／対象データ不足など）" }}
                {% endif %}
              </div>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      <div class="muted">対象ロット数: {{ today_lot_count }} / 不具合レコード数: {{ today_defect_count }}</div>
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

    worst_set = set(FIXED_WORST_41ST_HINBANS)
    if "品番" in today_summary.columns:
        mask_worst_today = today_summary["品番"].astype(str).isin(worst_set)
        worst_today_summary = today_summary.loc[mask_worst_today].copy()
        normal_today_summary = today_summary.loc[~mask_worst_today].copy()
    else:
        worst_today_summary = pd.DataFrame()
        normal_today_summary = today_summary

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
                lot_list.append({
                    "号機": str(r.get("号機", "")),
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
        logo_text=cfg.logo_text,
        logo_data_uri=f"data:image/png;base64,{LOGO_BASE64}",
        today_summary=normal_today_grouped,
        worst_today_summary=worst_today_grouped,
        today_lot_count=int(today_lots_df["生産ロットID"].nunique()),
        today_defect_count=int(len(today_defects_df)),
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
    p.add_argument("--run-date", type=str, help="YYYY-MM-DD (default: today)")
    p.add_argument("--config", type=str, help="path to JSON config")
    return p.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    run_date = datetime.now()
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
