# -*- coding: utf-8 -*-
"""
爬数据.py —— 阶段一：抓取原始数据（法规库流水线）
====================================================
把「法规库」所需的原始数据全部抓到本地磁盘。本文件是**单文件自包含**的，
按顺序一次跑完下面 6 步；任何一步失败只跳过该步，不影响后续步骤。

    步骤 1  爬取 NFRA（国家金融监督管理总局）政策规章规范性文件
    步骤 2  爬取人民银行（行政法规 / 部门规章 / 规范性文件）
    步骤 3  爬取 iweicha 监管文件（金监局 / 人行 / 外汇局，纯文字版）
    步骤 4  汇总 iweicha 索引（生成 iweicha/_index.csv）
    步骤 5  补取原文链接（接管浏览器点「原文链接」按钮，回写 _meta.json）
    步骤 6  补全「正文在附件里」的条目：
              6.1 下载附件（PDF / doc / xls …）
              6.2 文字型 PDF → pypdf 提取正文
              6.3 扫描型 PDF → OCR（本地 RapidOCR 优先，失败/缺失则走远程服务）

输出目录（均在项目根下）：
    政策规章规范性文件/{docId}_{标题}/      detail.json list_record.json content.html content.txt _meta.json
    人民银行/{栏目}/{articleId}_{标题}/     detail.json content.html content.txt _meta.json attachments/
    国家金融监督管理总局/{栏目}/{id}_{标题}/ 同上
    iweicha/{机构}/{年份}/{fileId}_{标题}/  detail.html content.txt _meta.json
    iweicha/_index.csv                     汇总索引

附件正文回写位置：
    {原始条目目录}/_body.txt   从附件（PDF 文字层 / OCR）提取的正文，
                               由「处理数据.py」在合并时按需取用。

用法：
    python 爬数据.py                    # 全流程（断点续传，已完成的自动跳过）
    python 爬数据.py --only nfra        # 只跑某一步（nfra/pbc/iweicha/ocr/…）
    python 爬数据.py --skip-link        # 跳过「补取原文链接」（省时间）
    python 爬数据.py --dry-run          # 只盘点，不实际抓取
    python 爬数据.py --workers 6        # 各步并发数（默认见各处）

降级策略（重要）：
    重依赖缺失或外部服务不可用时，**自动跳过并打印提示**，流程继续。
    playwright  缺失 → 跳过步骤 5
    pypdf       缺失 → 跳过 6.2（文字型 PDF 提取）
    pymupdf/rapidocr 缺失 → 6.3 自动改走远程 OCR
    远程 OCR 服务不可用 → 跳过 6.3 并提示
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html as html_lib
import http.cookiejar
import json
import os
import random
import re
import ssl
import sys
import time
import traceback
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

# 控制台中文输出（Windows）
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ssl._create_default_https_context = ssl._create_unverified_context

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# 附件扩展名
ATT_PAT = re.compile(r"\.(pdf|docx?|xlsx?|wps|et|zip|rar|7z|pptx?|rtf)\b", re.I)


def _project_root() -> Path:
    """向上查找项目根（含 法规库.db 或 .workbuddy 的目录），脚本移动位置也能跑。"""
    p = Path(__file__).resolve().parent
    for _ in range(6):
        if (p / "法规库.db").exists() or (p / ".workbuddy").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return p


ROOT = _project_root()

# 数据源根目录
NFRA_ROOT = ROOT / "国家金融监督管理总局"
PBC_ROOT = ROOT / "人民银行"
IW_ROOT = ROOT / "iweicha"

# 目录约定：栏目名 → 其下的条目目录
NFRA_COLS = ["政策规章规范性文件", "法律法规"]
PBC_COLS = ["行政法规", "部门规章", "规范性文件"]


# ============================================================================
# 通用工具
# ============================================================================
INVALID_FN = re.compile(r'[\\/:*?"<>|\r\n\t]')
MULTI_SPACE = re.compile(r"\s+")


def safe_name(s: str, max_len: int = 120) -> str:
    """清洗为合法目录名。Windows 不允许目录名以空格或点结尾。"""
    if not s:
        return "未命名"
    s = INVALID_FN.sub("_", s)
    s = MULTI_SPACE.sub(" ", s).strip().rstrip(".")
    if len(s) > max_len:
        s = s[:max_len].strip().rstrip(".")
    return s or "未命名"


class HtmlToText(HTMLParser):
    """HTML → 纯文本（保留段落分隔）。"""

    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1
        elif tag in ("br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = max(0, self.skip - 1)
        elif tag in ("p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5"):
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n+", "\n\n", text)
        return text.strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    p = HtmlToText()
    try:
        p.feed(html)
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)
    return p.get_text()


def text_len(html: str) -> int:
    """去标签去空白后的纯文本长度"""
    t = re.sub(r"<[^>]+>", "", html or "")
    t = re.sub(r"\s+", "", t)
    return len(t)


def http_get(url: str, headers: dict | None = None, timeout: int = 40,
             retries: int = 3, encoding: str | None = None) -> bytes:
    """带重试的 GET，返回原始 bytes。"""
    last_err = None
    hdrs = {"User-Agent": UA}
    if headers:
        hdrs.update(headers)
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            return urllib.request.urlopen(req, timeout=timeout).read()
        except Exception as e:
            last_err = e
            time.sleep(1 + i * 2)
    raise last_err


def load_json(p: Path, default=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def dump_json(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================================
# 重依赖探测（用于自动降级）
# ============================================================================
def has_module(name: str) -> bool:
    import importlib.util
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


def step_skip(step: str, why: str) -> None:
    print(f"  [跳过] {step} —— {why}", flush=True)


# ============================================================================
# 步骤 1：爬取 NFRA 政策规章规范性文件
# ============================================================================
NFRA_HOST = "https://www.nfra.gov.cn"
NFRA_LIST_API = "/cbircweb/DocInfo/SelectDocByItemIdAndChild"
NFRA_DETAIL_API = "/cbircweb/DocInfo/SelectByDocId"
NFRA_PAGE_SIZE = 18
NFRA_ITEM_ID = 928
NFRA_ITEM_NAME = "政策规章规范性文件"
NFRA_OUT = NFRA_ROOT / "政策规章规范性文件"
NFRA_LIST_JSON = NFRA_OUT / "_list.json"
NFRA_INDEX_CSV = NFRA_OUT / "_index.csv"

NFRA_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]
NFRA_TIMEOUT = 30
NFRA_MAX_RETRY = 4
NFRA_WORKERS_DEFAULT = 1        # session 限速后回到最稳配置
NFRA_LIST_WORKERS = 1           # 列表分页同样保守，避免 403
NFRA_DELAY_MIN = 1.0            # 每次请求最小秒数（jitter 下界）
NFRA_DELAY_MAX = 2.0            # 每次请求最大秒数（jitter 上界）

# 完整导航链 Referer：⚠️ 必须用「长链」，短链 itemId=928 会路由到 nginx 兜底
# 返回假 200（空数据）；长链才路由到 X-WEB 真后端。
NFRA_NAV_REFERER = (
    f"{NFRA_HOST}/cn/view/pages/ItemList.html"
    f"?itemPId=923&itemId={NFRA_ITEM_ID}&itemUrl=ItemListRightList.html"
    f"&itemName=%E6%94%BF%E7%AD%96%E8%A7%84%E7%AB%A0%E8%A7%84%E8%8C%83%E6%80%A7%E6%96%87%E4%BB%B6"
    f"&itemsubPId=926"
)

# NFRA 详情 API 必须带 ASP.NET_SessionId cookie 才不会被 403。
# 用 cookiejar 维持一次会话：启动时 GET 主页一次拿 cookie，
# 之后所有请求都通过同一个 opener，cookie 自动带上。
NFRA_COOKIE_JAR = http.cookiejar.CookieJar()
NFRA_OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(NFRA_COOKIE_JAR))
NFRA_OPENER.addheaders = []     # 由 Request.headers 控制 UA / Referer


def nfra_init_session() -> None:
    """先 GET 一次列表页主页，触发服务器下发 ASP.NET_SessionId cookie。幂等。"""
    list_url = (
        f"{NFRA_HOST}/cn/view/pages/ItemList.html"
        f"?itemPId=923&itemId={NFRA_ITEM_ID}&itemUrl=ItemListRightList.html"
        f"&itemName=%E6%94%BF%E7%AD%96%E8%A7%84%E7%AB%A0%E8%A7%84%E8%8C%83%E6%80%A7%E6%96%87%E4%BB%B6"
        f"&itemsubPId=926"
    )
    try:
        req = urllib.request.Request(list_url, headers={
            "User-Agent": random.choice(NFRA_UA_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
        with NFRA_OPENER.open(req, timeout=NFRA_TIMEOUT) as resp:
            resp.read()
        cookies = list(NFRA_COOKIE_JAR)
        print(f"      session cookie：{len(cookies)} 个"
              f"（{', '.join(c.name for c in cookies) or '空'}）", flush=True)
    except Exception as e:
        print(f"      ⚠ 初始化 session cookie 失败（继续）：{e}", flush=True)


def nfra_get_json(url: str, params: dict, timeout: int = NFRA_TIMEOUT,
                  max_retry: int = NFRA_MAX_RETRY,
                  delay_min: float = NFRA_DELAY_MIN,
                  delay_max: float = NFRA_DELAY_MAX) -> dict:
    """GET → JSON，带抖动、重试、403/429/503 长退避（尊重 Retry-After）。"""
    full = url + "?" + urllib.parse.urlencode(params)
    last_err = None
    for attempt in range(max_retry):
        time.sleep(random.uniform(delay_min, delay_max))
        headers = {
            "User-Agent": random.choice(NFRA_UA_POOL),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": NFRA_NAV_REFERER,
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        try:
            req = urllib.request.Request(full, headers=headers)
            with NFRA_OPENER.open(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
        except urllib.error.HTTPError as e:
            last_err = e
            code = getattr(e, "code", None)
            if code in (403, 429, 503):
                wait = 60 + 30 * attempt
                try:
                    ra = (e.headers.get("Retry-After") if e.headers else None)
                    if ra:
                        wait = max(wait, int(ra))
                except Exception:
                    pass
                print(f"      ⚠ HTTP {code} 限速，等待 {wait}s 后重试…", flush=True)
                time.sleep(wait)
            else:
                time.sleep(5 * (2 ** attempt))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(5 * (2 ** attempt))
    raise RuntimeError(f"GET 失败 {full}: {last_err}")


# ---------------------------- 阶段1：列表分页 ----------------------------
def nfra_fetch_list(total_pages: int | None = None) -> list[dict]:
    """抓取所有列表项，写盘 NFRA_LIST_JSON 做快照。"""
    print(f"[1/6] 抓取 NFRA 列表（{total_pages or '全量'} 页 / 每页 {NFRA_PAGE_SIZE} 条）…",
          flush=True)
    first = nfra_get_json(NFRA_HOST + NFRA_LIST_API, {
        "itemId": NFRA_ITEM_ID, "pageSize": NFRA_PAGE_SIZE, "pageIndex": 1,
    })
    if first.get("rptCode") != 200:
        raise RuntimeError(f"列表接口异常：{str(first)[:200]}")
    total = first["data"]["total"]
    rows = list(first["data"]["rows"])
    pages = total_pages or ((total + NFRA_PAGE_SIZE - 1) // NFRA_PAGE_SIZE)
    print(f"      列表 total={total}，将抓 {pages} 页", flush=True)

    def one(p: int):
        try:
            r = nfra_get_json(NFRA_HOST + NFRA_LIST_API, {
                "itemId": NFRA_ITEM_ID, "pageSize": NFRA_PAGE_SIZE, "pageIndex": p,
            })
            return p, r.get("data", {}).get("rows") or []
        except Exception as e:
            return p, ("__ERR__", str(e))

    failures = []
    with ThreadPoolExecutor(max_workers=NFRA_LIST_WORKERS) as pool:
        futures = {pool.submit(one, p): p for p in range(2, pages + 1)}
        done = 0
        for fut in as_completed(futures):
            p, payload = fut.result()
            done += 1
            if isinstance(payload, tuple) and payload and payload[0] == "__ERR__":
                failures.append((p, payload[1]))
                print(f"      ✗ 第 {p} 页失败：{payload[1][:80]}", flush=True)
            else:
                rows.extend(payload)
            if pages > 1 and (done % 20 == 0 or done == pages - 1):
                print(f"      列表进度：{done}/{pages - 1} 页 累计 {len(rows)} 条 "
                      f"失败 {len(failures)}", flush=True)

    # 失败页：单线程细水长流串行补救
    if failures:
        print(f"      ⚠ {len(failures)} 页并发失败，改单线程重试…", flush=True)
        for p, err in failures:
            for attempt in range(4):
                try:
                    r = nfra_get_json(NFRA_HOST + NFRA_LIST_API, {
                        "itemId": NFRA_ITEM_ID, "pageSize": NFRA_PAGE_SIZE, "pageIndex": p,
                    }, delay_min=4.0, delay_max=8.0)
                    rows.extend(r.get("data", {}).get("rows") or [])
                    print(f"      ✓ 第 {p} 页补救成功（第 {attempt + 1} 次）", flush=True)
                    break
                except Exception as e:
                    wait = 60 + 60 * attempt
                    print(f"      … 第 {p} 页第 {attempt + 1} 次失败：{str(e)[:60]}，"
                          f"等待 {wait}s", flush=True)
                    time.sleep(wait)
            else:
                print(f"      ✗✗ 第 {p} 页多次重试仍失败，跳过", flush=True)
            time.sleep(random.uniform(5, 10))

    seen, uniq = set(), []
    for r in rows:
        did = r.get("docId")
        if did and did not in seen:
            seen.add(did)
            uniq.append(r)

    NFRA_OUT.mkdir(parents=True, exist_ok=True)
    NFRA_LIST_JSON.write_text(json.dumps({
        "total": total,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "rows": uniq,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"      列表落盘：{NFRA_LIST_JSON.name}（{len(uniq)} 条）", flush=True)
    return uniq


# ---------------------------- 阶段2：详情抓取 ----------------------------
class _StripTags(HTMLParser):
    """极简 HTML → 文本：保留段落换行、去掉所有标签。"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1
        if tag in ("p", "br", "div", "li", "tr", "td", "th", "h1", "h2", "h3"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.skip:
            self.skip -= 1
        if tag in ("p", "div", "li", "tr"):
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


def _strip_tags_to_text(html: str) -> str:
    if not html:
        return ""
    p = _StripTags()
    try:
        p.feed(html)
        p.close()
    except Exception:
        return re.sub(r"<[^>]+>", " ", html)
    text = "".join(p.parts)
    lines = [MULTI_SPACE.sub(" ", ln).strip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln).strip()


def nfra_fetch_detail(doc_id: int):
    r = nfra_get_json(NFRA_HOST + NFRA_DETAIL_API, {"docId": doc_id})
    if r.get("rptCode") == 200 and r.get("data"):
        return r["data"]
    return None


def nfra_folder_for(doc_id, title: str) -> str:
    return f"{doc_id}_{safe_name(title)}"


def nfra_save_one(row: dict) -> tuple[int, str, bool]:
    """落盘单条。返回 (docId, title, ok)。"""
    doc_id = row["docId"]
    title = (row.get("docTitle") or row.get("docSubtitle") or "").strip() or f"doc_{doc_id}"
    folder = NFRA_OUT / nfra_folder_for(doc_id, title)
    detail_p = folder / "detail.json"
    list_p = folder / "list_record.json"
    html_p = folder / "content.html"
    txt_p = folder / "content.txt"
    meta_p = folder / "_meta.json"

    # 断点续爬
    if detail_p.exists():
        d = load_json(detail_p, {})
        if d.get("docClob"):
            return (doc_id, title, True)

    folder.mkdir(parents=True, exist_ok=True)
    src_url = (f"{NFRA_HOST}/cn/view/pages/ItemDetail.html"
               f"?docId={doc_id}&itemId={NFRA_ITEM_ID}")
    list_record = dict(row)
    list_record.update({"source_url": src_url, "doc_id": doc_id,
                        "item_id": NFRA_ITEM_ID, "item_name": NFRA_ITEM_NAME})
    list_p.write_text(json.dumps(list_record, ensure_ascii=False, indent=1),
                      encoding="utf-8")

    try:
        detail = nfra_fetch_detail(doc_id)
    except Exception as e:
        dump_json(meta_p, {"docId": doc_id, "ok": False, "error": str(e),
                           "ts": datetime.now().isoformat(timespec="seconds")})
        return (doc_id, title, False)

    if not detail:
        dump_json(meta_p, {"docId": doc_id, "ok": False, "error": "no data",
                           "ts": datetime.now().isoformat(timespec="seconds")})
        return (doc_id, title, False)

    detail_p.write_text(json.dumps(detail, ensure_ascii=False, indent=1),
                        encoding="utf-8")
    clob = detail.get("docClob") or ""
    html_p.write_text(clob, encoding="utf-8", errors="replace")
    txt = _strip_tags_to_text(clob)
    txt_p.write_text(txt, encoding="utf-8")
    dump_json(meta_p, {
        "docId": doc_id, "title": title, "ok": True,
        "has_docClob": bool(clob), "publishDate": row.get("publishDate"),
        "content_len": len(txt),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    })
    return (doc_id, title, True)


# ---------------------------- 阶段3：索引 ----------------------------
def nfra_write_index(rows: list[dict]) -> None:
    NFRA_OUT.mkdir(parents=True, exist_ok=True)
    with NFRA_INDEX_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(["docId", "title", "publishDate", "source_url", "ok",
                    "has_docClob", "content_len", "folder"])
        for r in rows:
            did = r["docId"]
            t = (r.get("docTitle") or r.get("docSubtitle") or "").strip()
            meta_p = NFRA_OUT / nfra_folder_for(did, t) / "_meta.json"
            m = load_json(meta_p, {}) if meta_p.exists() else {}
            w.writerow([
                did, t, r.get("publishDate"),
                f"{NFRA_HOST}/cn/view/pages/ItemDetail.html?docId={did}&itemId={NFRA_ITEM_ID}",
                "Y" if m.get("ok") else "N",
                "Y" if m.get("has_docClob") else "N",
                m.get("content_len", 0),
                nfra_folder_for(did, t),
            ])
    print(f"      索引：{NFRA_INDEX_CSV.name}", flush=True)


def run_nfra(workers: int = NFRA_WORKERS_DEFAULT, pages: int | None = None,
             dry: bool = False) -> dict:
    NFRA_OUT.mkdir(parents=True, exist_ok=True)
    if dry:
        n = len(list(NFRA_OUT.glob("*/_meta.json")))
        print(f"[1/6] NFRA 盘点：已有 {n} 条", flush=True)
        return {"step": "nfra", "dry": True, "existing": n}

    print("[1/6] 初始化 NFRA session cookie …", flush=True)
    nfra_init_session()

    if NFRA_LIST_JSON.exists():
        rows = load_json(NFRA_LIST_JSON, {}).get("rows", [])
        print(f"      复用 {NFRA_LIST_JSON.name}（{len(rows)} 条）", flush=True)
    else:
        rows = nfra_fetch_list(pages)
    if pages:
        rows = rows[: pages * NFRA_PAGE_SIZE]

    total = len(rows)
    ok, fail = 0, []
    t0 = time.time()
    last_report = time.time()
    print(f"      抓取详情 {total} 条 · 并发 {workers}", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(nfra_save_one, r): r for r in rows}
        done = 0
        for fut in as_completed(futs):
            try:
                did, title, succeeded = fut.result()
            except Exception as e:
                did, title, succeeded = -1, "?", False
                fail.append((did, str(e)[:120]))
            done += 1
            if succeeded:
                ok += 1
            else:
                fail.append((did, title))
            if time.time() - last_report > 5 or done == total:
                rate = done / max(1, time.time() - t0)
                eta = (total - done) / max(rate, 0.001)
                print(f"      {done}/{total} 成功 {ok} 失败 {len(fail)} "
                      f"{rate:.1f}条/秒 剩余≈{eta/60:.1f}分", flush=True)
                last_report = time.time()

    nfra_write_index(rows)
    print(f"[1/6] NFRA 完成：成功 {ok}/{total}，失败 {len(fail)}", flush=True)
    if fail:
        with (NFRA_OUT / "_fail.csv").open("w", encoding="utf-8-sig", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(["docId", "title_or_err"])
            w.writerows(fail)
    return {"step": "nfra", "rows": total, "ok": ok, "fail": len(fail)}


# ============================================================================
# 步骤 2：爬取人民银行
# ============================================================================
PBC_HOST = "https://www.pbc.gov.cn"
PBC_TFS_ROOT = "/tiaofasi/144941"

# 三个栏目：(colId, 名称)
PBC_COLUMNS = [
    (144953, "行政法规"),
    (144957, "部门规章"),
    (3581332, "规范性文件"),
]

PBC_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
]
PBC_TIMEOUT = 30
PBC_MAX_RETRY = 3
PBC_WORKERS_DEFAULT = 10
PBC_LIST_WORKERS = 4
PBC_DELAY_MIN = 0.05
PBC_DELAY_MAX = 0.2


def pbc_http_get(url: str, params: dict | None = None,
                 max_retry: int = PBC_MAX_RETRY,
                 delay_min: float = PBC_DELAY_MIN,
                 delay_max: float = PBC_DELAY_MAX) -> str:
    """GET 一次，返回 text（自动处理 GBK/UTF-8）。失败抛异常。"""
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    last_err = None
    for attempt in range(max_retry):
        time.sleep(random.uniform(delay_min, delay_max))
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": random.choice(PBC_UA_POOL),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            })
            with urllib.request.urlopen(req, timeout=PBC_TIMEOUT) as resp:
                data = resp.read()
                ct = resp.headers.get("Content-Type", "")
                if "charset=gbk" in ct.lower() or "charset=gb2312" in ct.lower():
                    return data.decode("gbk", errors="replace")
                head = data[:1024].decode("ascii", errors="replace")
                m = re.search(r'charset\s*=\s*["\']?([\w-]+)', head, re.I)
                if m and m.group(1).lower() in ("gbk", "gb2312"):
                    return data.decode(m.group(1), errors="replace")
                return data.decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"GET 失败 {url}: {last_err}")


# ---------------------------- 列表解析 ----------------------------
# 列表行 HTML 真实结构（行政法规 144953）：
#   <a href="/tiaofasi/144941/144953/2811354/index.html"
#      onclick="void(0)" target="_blank"
#      title="存款保险条例（国务院令 第660号）"
#      istitle="true">存款保险条例（国务院令 第660号）</a>
#   </font><span class="hui12">2015-03-31</span>
PBC_LIST_A_HREF_RE = re.compile(
    r'<a\s+href="/tiaofasi/\d+/(\d+)/([0-9a-z]+)/index\.html"', re.IGNORECASE)
PBC_LIST_TITLE_ATTR_RE = re.compile(r'title="([^"]+)"')
PBC_LIST_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
PBC_LIST_LINK_TEXT_RE = re.compile(r'<a\s+href="[^"]+"\s+[^>]*>([^<]+)</a>', re.IGNORECASE)


def pbc_parse_list_page(html: str, col_id: int) -> list[dict]:
    """从列表页 HTML 抽 (articleId, title, date, url)。"""
    rows, seen = [], set()
    for m in PBC_LIST_A_HREF_RE.finditer(html):
        if int(m.group(1)) != col_id:
            continue
        article_id = m.group(2)
        if article_id in seen:
            continue
        seen.add(article_id)
        a_end = html.find("</a>", m.end())
        if a_end == -1:
            continue
        window = html[m.start(): a_end + 200]
        tm = PBC_LIST_TITLE_ATTR_RE.search(window)
        if tm:
            title = tm.group(1).strip()
        else:
            lt = PBC_LIST_LINK_TEXT_RE.search(window)
            title = lt.group(1).strip() if lt else ""
        dm = PBC_LIST_DATE_RE.search(window)
        date = dm.group(1) if dm else ""
        full = re.search(r'href="(/tiaofasi/[^"]+)"', window)
        url = PBC_HOST + full.group(1) if full else ""
        rows.append({"article_id": article_id, "title": title,
                     "date": date, "url": url})
    return rows


def pbc_find_portlet_id(html: str) -> str | None:
    """从列表首页找翻页 portlet id（翻页 URL 第一段）。"""
    m = re.search(r"queryArticleByCondition\([^,]+,\s*['\"]?/tiaofasi/\d+/\d+/([a-f0-9]+)-\d+\.html", html)
    return m.group(1) if m else None


def pbc_find_total_pages(html: str) -> int:
    """从首页找总页数。"""
    nums = [int(m.group(1)) for m in re.finditer(
        r"queryArticleByCondition\([^,]+,\s*['\"]?/tiaofasi/\d+/\d+/[a-f0-9]+-(\d+)\.html", html)]
    if nums:
        return max(nums)
    m = re.search(r"尾页[^']*['\"]([^'\"]+)", html)
    if m:
        nm = re.search(r"-(\d+)\.html", m.group(1))
        if nm:
            return int(nm.group(1))
    return 1


# ---------------------------- 详情页解析 ----------------------------
# 详情页正文真实结构：
#   <td class="content" colspan="2" align="left" valign="top">
#     <div id="zoom"> ... 法规正文 ... </div>
#   </td>
PBC_CONTENT_CONTAINER_PATTERNS = [
    (re.compile(r'<div[^>]*\bid\s*=\s*["\']zoom["\'][^>]*>', re.I), "zoom"),
    (re.compile(r'<div[^>]*\bclass\s*=\s*["\'][^"\']*\bcontent\b[^"\']*["\'][^>]*>', re.I), "content"),
    (re.compile(r'<div[^>]*\bid\s*=\s*["\'][^"\']*[Cc]ontent[^"\']*["\'][^>]*>', re.I), "content-id"),
    (re.compile(r'<div[^>]*\bid\s*=\s*["\'][^"\']*article[^"\']*["\'][^>]*>', re.I), "article"),
    (re.compile(r'<div[^>]*\bclass\s*=\s*["\'][^"\']*\bTRS_Editor\b[^"\']*["\'][^>]*>', re.I), "TRS_Editor"),
]


def pbc_extract_balanced_div(html: str, start_pos: int):
    """从 start_pos（指向 <div ...>）往后抓匹配的 </div>，返回 (end_pos, inner_html)。"""
    pos, depth, n = start_pos, 0, len(html)
    while pos < n:
        m_open = re.search(r'<div\b[^>]*>', html[pos:], re.I)
        m_close = re.search(r'</div\s*>', html[pos:], re.I)
        if not m_close:
            return None
        if m_open and m_open.start() < m_close.start():
            depth += 1
            pos += m_open.end()
        else:
            depth -= 1
            pos += m_close.end()
            if depth == 0:
                return pos, html[start_pos: pos]
    return None


def pbc_extract_content(html: str) -> tuple[str, str, str]:
    """返回 (title, content_html, content_text)。"""
    m = re.search(r"<title>([^<]+)</title>", html, re.I)
    title = m.group(1).strip() if m else ""

    content_html = ""
    found_start = -1
    for pat, _name in PBC_CONTENT_CONTAINER_PATTERNS:
        m2 = pat.search(html)
        if m2:
            found_start = m2.start()
            break

    if found_start >= 0:
        open_tag_end = html.find(">", found_start) + 1
        result = pbc_extract_balanced_div(html, found_start)
        if result:
            _, full = result
            inner = full[open_tag_end - found_start: -len("</div>")]
            content_html = inner.strip()
        else:
            body_end = html.lower().find("</body>")
            content_html = html[open_tag_end: body_end if body_end > 0 else len(html)].strip()

    text = re.sub(r"<style[^>]*>.*?</style>", "", content_html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>|</div>|</tr>|</li>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = (text.replace("&nbsp;", " ").replace("&amp;", "&")
                .replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&#39;", "'"))
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return title, content_html, text.strip()


# ---------------------------- 抓取逻辑 ----------------------------
def pbc_fetch_list(col_id: int, col_name: str) -> list[dict]:
    """抓某个栏目的所有条目。"""
    print(f"      [列表] {col_name} (colId={col_id})", flush=True)
    base = f"{PBC_HOST}{PBC_TFS_ROOT}/{col_id}"
    p1 = pbc_http_get(f"{base}/index.html")

    rows = pbc_parse_list_page(p1, col_id)
    print(f"        第 1 页：{len(rows)} 条", flush=True)

    portlet_id = pbc_find_portlet_id(p1)
    total_pages = pbc_find_total_pages(p1)
    print(f"        portlet_id={portlet_id} total_pages={total_pages}", flush=True)

    if total_pages > 1 and portlet_id:
        with ThreadPoolExecutor(max_workers=PBC_LIST_WORKERS) as pool:
            futures = {pool.submit(pbc_http_get, f"{base}/{portlet_id}-{n}.html"): n
                       for n in range(2, total_pages + 1)}
            for fut in as_completed(futures):
                n = futures[fut]
                try:
                    rows.extend(pbc_parse_list_page(fut.result(), col_id))
                except Exception as e:
                    print(f"        第 {n} 页失败：{e}", flush=True)

    seen, uniq = set(), []
    for r in rows:
        if r["article_id"] in seen:
            continue
        seen.add(r["article_id"])
        uniq.append(r)
    print(f"        总计（去重）：{len(uniq)} 条", flush=True)
    return uniq


def pbc_folder(article_id: str, title: str) -> str:
    return f"{article_id}_{safe_name(title)}"


def pbc_fetch_one(col_id: int, col_name: str, row: dict) -> tuple[bool, dict]:
    """抓一条详情。返回 (ok, meta)。"""
    aid = row["article_id"]
    title = row.get("title", "")
    folder = PBC_ROOT / col_name / pbc_folder(aid, title)
    meta_path = folder / "_meta.json"

    if meta_path.exists():
        meta = load_json(meta_path, {})
        if meta.get("ok") and meta.get("has_content"):
            return True, meta

    url = f"{PBC_HOST}{PBC_TFS_ROOT}/{col_id}/{aid}/index.html"
    try:
        html = pbc_http_get(url)
    except Exception as e:
        return False, {"article_id": aid, "ok": False, "error": str(e)[:200]}
    if not html or len(html) < 500:
        return False, {"article_id": aid, "ok": False, "error": "empty/too short"}

    title_real, content_html, content_text = pbc_extract_content(html)

    folder.mkdir(parents=True, exist_ok=True)
    (folder / "detail.html").write_text(html, encoding="utf-8", errors="replace")
    (folder / "content.html").write_text(content_html, encoding="utf-8")
    (folder / "content.txt").write_text(content_text, encoding="utf-8")

    meta = {
        "article_id": aid, "col_id": col_id, "col_name": col_name,
        "title": title_real or title, "list_title": title,
        "date": row.get("date"), "url": url, "ok": True,
        "has_content": bool(content_html.strip()),
        "content_len": len(content_html),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    dump_json(meta_path, meta)
    return True, meta


PBC_INDEX_ACC = []      # 累积 (row, meta)，最后统一写 _index.csv


def pbc_write_index(all_rows: list[tuple[dict, dict]]) -> None:
    PBC_ROOT.mkdir(parents=True, exist_ok=True)
    with (PBC_ROOT / "_index.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["colName", "colId", "articleId", "title", "date",
                    "url", "ok", "has_content", "content_len", "folder"])
        for row, meta in all_rows:
            w.writerow([
                row["col_name"], row["col_id"], row["article_id"],
                row.get("title", ""), row.get("date", ""), row.get("url", ""),
                "Y" if meta.get("ok") else "N",
                "Y" if meta.get("has_content") else "N",
                meta.get("content_len", 0),
                pbc_folder(row["article_id"], row.get("title", "")),
            ])


def run_pbc(workers: int = PBC_WORKERS_DEFAULT, dry: bool = False) -> dict:
    PBC_ROOT.mkdir(parents=True, exist_ok=True)
    all_rows: list[tuple[dict, dict]] = []
    tot_ok = tot_fail = 0

    for col_id, col_name in PBC_COLUMNS:
        print(f"[2/6] 爬取人行 · {col_name} …", flush=True)
        if dry:
            n = len(list((PBC_ROOT / col_name).glob("*/_meta.json"))) \
                if (PBC_ROOT / col_name).is_dir() else 0
            print(f"      [dry] 已有 {n} 条", flush=True)
            continue

        list_json = PBC_ROOT / f"_list_{col_name}.json"
        if list_json.exists():
            rows = load_json(list_json, [])
            for r in rows:
                r["col_id"], r["col_name"] = col_id, col_name
            print(f"        复用 {list_json.name}（{len(rows)} 条）", flush=True)
        else:
            rows = pbc_fetch_list(col_id, col_name)
            for r in rows:
                r["col_id"], r["col_name"] = col_id, col_name
            dump_json(list_json, rows)

        col_root = PBC_ROOT / col_name
        col_root.mkdir(parents=True, exist_ok=True)
        skip, todo = 0, []
        for r in rows:
            mp = col_root / pbc_folder(r["article_id"], r.get("title", "")) / "_meta.json"
            m = load_json(mp, {}) if mp.exists() else {}
            if m.get("ok") and m.get("has_content"):
                all_rows.append((r, m))
                skip += 1
            else:
                todo.append(r)
        print(f"        跳过 {skip} 条，待抓 {len(todo)} 条 · 并发 {workers}", flush=True)

        success = failed = done = 0
        t0 = time.time()
        col_metas: list[tuple[dict, dict]] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(pbc_fetch_one, col_id, col_name, r): r for r in todo}
            for fut in as_completed(futures):
                r = futures[fut]
                try:
                    ok, meta = fut.result()
                except Exception as e:
                    ok, meta = False, {"article_id": r["article_id"], "ok": False,
                                       "error": str(e)[:200]}
                col_metas.append((r, meta))
                success += 1 if ok else 0
                failed += 0 if ok else 1
                done += 1
                if done % 10 == 0 or done == len(todo):
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    remain = (len(todo) - done) / rate if rate > 0 else 0
                    print(f"        {done + skip}/{len(rows)} 跳过 {skip} 成功 {success} "
                          f"失败 {failed} {rate:.1f}条/秒 剩余≈{remain / 60:.1f}分", flush=True)
        all_rows.extend(col_metas)
        tot_ok += success
        tot_fail += failed
        print(f"        {col_name} 完成：成功 {success}，失败 {failed}", flush=True)

    if not dry:
        pbc_write_index(all_rows)
        print(f"[2/6] 人行索引写入（{len(all_rows)} 条）", flush=True)
    print(f"[2/6] 人行完成：成功 {tot_ok}，失败 {tot_fail}", flush=True)
    return {"step": "pbc", "ok": tot_ok, "fail": tot_fail}


# ============================================================================
# 步骤 3：爬取 iweicha
# ============================================================================
IW_BASE = "http://iweicha.com/smp/smp10.aspx"
IW_SYS = "MS599"
IW_TYPES = {1: "金监局", 2: "人行", 3: "外汇局"}
IW_STATUS_PAT = re.compile(
    r"\((有效|废止|失效|已废止|已失效|部分废止|部分失效|部分有效|已被修订|已修改|已修订)\)\s*$")


def iw_get(url: str, timeout: int = 30, retries: int = 3) -> str:
    raw = http_get(url, headers={"User-Agent": UA}, timeout=timeout, retries=retries)
    return raw.decode("utf-8", "replace")


def iw_list_url(t: int, year: str, index: int) -> str:
    q = urllib.parse.urlencode({
        "sys_code": IW_SYS, "def": "ffs.xml", "type": t,
        "fun_type": "1-002-2", "version": "0",
        "form_id": "1-002-2-1", "smp_type": "display_table",
        "smp_tb_name": "dirdata", "fld": "file_year",
        "con": year, "smp_index": index,
    })
    return f"{IW_BASE}?{q}"


def iw_detail_url(t: int, file_id: str) -> str:
    q = urllib.parse.urlencode({
        "sys_code": IW_SYS, "def": "ffs.xml", "type": t,
        "fun_type": "1-001-1", "version": "0",
        "form_id": "1-002-2-1", "fld": "file_id",
        "con": file_id, "smp_index": "0",
        "smp_tb_name": "ffiledata",
    })
    return f"{IW_BASE}?{q}"


def iw_get_years(t: int) -> list[tuple[str, int]]:
    """从年度分类页取年份列表（新→旧）。'__none__' 表示无年份。"""
    q = urllib.parse.urlencode({
        "sys_code": IW_SYS, "def": "ffs.xml", "type": t,
        "fun_type": "1-002-2", "version": "0",
    })
    html = iw_get(f"{IW_BASE}?{q}")
    years, seen = [], set()
    for m in re.finditer(r'fld=file_year&con=([^&"]*)&smp_index=0[^>]*>(.*?)</a>',
                         html, re.S):
        con = urllib.parse.unquote(m.group(1))
        txt = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        mm = re.search(r"\((\d+)\)", txt)
        cnt = int(mm.group(1)) if mm else 0
        key = "__none__" if con == "" else con
        if key in seen or not (con == "" or re.match(r"^\d{4}$", con)):
            continue
        seen.add(key)
        years.append((key, cnt))
    return years


def iw_parse_list(html: str) -> list[tuple[str, str]]:
    items, seen = [], set()
    for m in re.finditer(
            r"<a[^>]*class=['\"]dir_topic_css['\"][^>]*href=['\"]([^'\"]*fld=file_id[^'\"]*)['\"][^>]*>(.*?)</a>",
            html, re.S):
        href = m.group(1)
        title = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        fm = re.search(r"con=(\d+)", href)
        if not fm:
            continue
        fid = fm.group(1)
        if fid not in seen:
            seen.add(fid)
            items.append((fid, title))
    return items


def iw_parse_detail(html: str) -> dict:
    """解析 iweicha 详情页 → {title, status, content}。"""
    out = {"title": "", "status": "", "content": ""}
    title_raw = ""
    m = re.search(r'id=["\']lbl_file_name["\'][^>]*>(.*?)</span>', html, re.S)
    if m:
        title_raw = re.sub(r"<[^>]+>", "", m.group(1)).strip()
    if not title_raw:
        for cap in re.finditer(r"<caption[^>]*>(.*?)</caption>", html, re.S):
            t = re.sub(r"<[^>]+>", "", cap.group(1)).strip()
            t = MULTI_SPACE.sub(" ", t)
            if t and t not in ("监管文件", "目录", "全文", "文件内容"):
                title_raw = t
                break
    title_raw = MULTI_SPACE.sub(" ", title_raw).strip()
    sm = IW_STATUS_PAT.search(title_raw)
    if sm:
        out["status"] = sm.group(1)
        title_raw = title_raw[: sm.start()].strip()
    out["title"] = title_raw

    i = html.find("文件内容")
    if i >= 0:
        rest = html[i:]
        j = rest.find("Copyright")
        if j >= 0:
            rest = rest[:j]
        content = re.sub(r"<br\s*/?>", "\n", rest, flags=re.I)
        content = re.sub(r"</td>", "\n", content, flags=re.I)
        content = re.sub(r"</tr>", "\n", content, flags=re.I)
        content = re.sub(r"<[^>]+>", "", content)
        content = content.replace("文件内容", "", 1)
        lines = [ln.strip() for ln in content.split("\n")]
        out["content"] = "\n".join(ln for ln in lines if ln)
    return out


def iw_fetch_detail(t: int, fid: str, title: str, year: str, dry: bool = False) -> dict:
    html = iw_get(iw_detail_url(t, fid))
    info = iw_parse_detail(html)
    if not info["title"]:
        info["title"] = title
    safe_title = safe_name(info["title"], 80)
    org = IW_TYPES[t]
    year_dir = "未知年份" if not year or year == "__none__" else year
    dp = IW_ROOT / org / year_dir / f"{fid}_{safe_title}"
    if not dry:
        dp.mkdir(parents=True, exist_ok=True)
        (dp / "detail.html").write_text(html, encoding="utf-8", errors="replace")
        (dp / "content.txt").write_text(info["content"], encoding="utf-8")
        dump_json(dp / "_meta.json", {
            "file_id": fid, "org": org, "type": t,
            "year": "" if year == "__none__" else year,
            "title": info["title"], "status": info["status"],
            "content_len": len(info["content"]),
            "url": iw_detail_url(t, fid),
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
    info["content_len"] = len(info["content"])
    return info


def iw_collect_all(t: int, year: int | None = None) -> list[tuple[str, str, str]]:
    years = iw_get_years(t)
    if year is not None:
        years = [(str(year), 0)]
    all_items, seen_fid = [], set()
    for y, _ in years:
        yval = "" if y == "__none__" else y
        idx = 0
        while idx <= 60:
            html = iw_get(iw_list_url(t, yval, idx))
            items = iw_parse_list(html)
            new = [it for it in items if it[0] not in seen_fid]
            if not new:
                break
            for fid, title in new:
                all_items.append((fid, title, y))
                seen_fid.add(fid)
            if len(items) < 50:
                break
            idx += 1
    return all_items


def run_iweicha(types=(1, 2), workers: int = 8, year: int | None = None,
                dry: bool = False) -> dict:
    stats = {}
    for t in types:
        org = IW_TYPES.get(t, str(t))
        print(f"[3/6] 爬取 iweicha · {org} …", flush=True)
        items = iw_collect_all(t, year)
        print(f"      共 {len(items)} 条", flush=True)
        if dry:
            stats[org] = {"total": len(items), "dry": True}
            continue
        done_fid = set()
        od = IW_ROOT / org
        if od.is_dir():
            for yd in od.iterdir():
                if yd.is_dir():
                    for d in yd.iterdir():
                        if d.is_dir():
                            done_fid.add(d.name.split("_")[0])
        todo = [it for it in items if it[0] not in done_fid]
        print(f"      待抓 {len(todo)} 条（跳过已完成 {len(items)-len(todo)}）", flush=True)
        ok = fail = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(iw_fetch_detail, t, fid, title, y): (fid, title)
                    for fid, title, y in todo}
            for i, fut in enumerate(as_completed(futs), 1):
                try:
                    fut.result()
                    ok += 1
                except Exception as e:
                    fail += 1
                    print(f"      失败 {futs[fut][0]}: {type(e).__name__} {e}", flush=True)
        print(f"      {org} 成功 {ok} 失败 {fail}", flush=True)
        stats[org] = {"total": len(items), "ok": ok, "fail": fail}
    return {"step": "iweicha", "orgs": stats}


# ============================================================================
# 步骤 4：汇总 iweicha 索引
# ============================================================================
def run_iw_index() -> dict:
    print("[4/6] 汇总 iweicha 索引 …", flush=True)
    rows = []
    for p in IW_ROOT.glob("*/*/*/_meta.json"):
        parts = p.parts
        if len(parts) < 5:
            continue
        m = load_json(p, {})
        rows.append({
            "file_id": m.get("file_id", ""),
            "org": m.get("org", parts[-3]),
            "year": m.get("year", parts[-2]),
            "title": m.get("title", ""),
            "status": m.get("status", ""),
            "content_len": m.get("content_len", 0),
            "url": m.get("url", ""),
            "original_url": m.get("original_url", ""),
            "dir_path": str(p.parent),
        })
    if not rows:
        print("      iweicha/ 下没有找到 _meta.json", flush=True)
        return {"step": "iw_index", "rows": 0}
    org_order = {"金监局": 1, "人行": 2, "外汇局": 3}
    rows.sort(key=lambda r: (org_order.get(r["org"], 99),
                             -(int(r["year"]) if str(r["year"]).isdigit() else 0),
                             int(r["file_id"]) if str(r["file_id"]).isdigit() else 999999))
    with (IW_ROOT / "_index.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["file_id", "org", "year", "title", "status",
                                          "content_len", "url", "original_url", "dir_path"])
        w.writeheader()
        w.writerows(rows)

    # 状态汇总
    from collections import Counter
    c = Counter(r["status"] or "(空)" for r in rows)
    lines = [f"iweicha 汇总报告", "=" * 60, f"总条目数：{len(rows)}",
             f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}", "", "=== 按时效状态 ==="]
    for s, n in c.most_common():
        lines.append(f"  {s:8s}: {n:5d} 条  ({n / len(rows) * 100:5.1f}%)")
    (IW_ROOT / "_status_summary.txt").write_text("\n".join(lines), encoding="utf-8")

    print(f"      索引写入 {IW_ROOT / '_index.csv'}（{len(rows)} 行）", flush=True)
    return {"step": "iw_index", "rows": len(rows)}


# ============================================================================
# 步骤 5：补取原文链接（接管浏览器点击「原文链接」）
# ============================================================================
IW_SRC_BTN = "#btn_1-001-1_src_url"


def run_fetch_links(workers: int = 4, org: str | None = None,
                    limit: int = 0, dry: bool = False) -> dict:
    print("[5/6] 补取原文链接 …", flush=True)
    if not has_module("playwright"):
        step_skip("补取原文链接", "未安装 playwright（pip install playwright && playwright install chromium）")
        return {"step": "links", "skipped": "no-playwright"}

    targets = []
    for p in IW_ROOT.glob("*/*/*/_meta.json"):
        m = load_json(p, {})
        if not m or m.get("original_url") or not m.get("url"):
            continue
        if org and m.get("org") != org:
            continue
        targets.append((p, m))
    targets.sort(key=lambda t: -(int(t[1].get("year")) if str(t[1].get("year")).isdigit() else 0))
    if limit:
        targets = targets[:limit]
    print(f"      待补 {len(targets)} 条", flush=True)
    if not targets or dry:
        return {"step": "links", "todo": len(targets), "dry": dry}

    from playwright.sync_api import sync_playwright

    def worker(chunk, wid):
        ok = miss = 0
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel="chrome", headless=True)
            ctx = browser.new_context(user_agent=UA)
            page = ctx.new_page()
            page.on("dialog", lambda d: d.dismiss())
            for p, m in chunk:
                fid = m.get("file_id", "?")
                try:
                    page.goto(m["url"], timeout=60000, wait_until="commit")
                    for _ in range(50):
                        if page.locator(IW_SRC_BTN).count() > 0:
                            break
                        page.wait_for_timeout(200)
                    try:
                        page.click(IW_SRC_BTN, timeout=8000)
                    except Exception:
                        pass
                    new_url = ""
                    for _ in range(75):
                        u = page.url
                        if u.startswith(("http://", "https://")) and "iweicha.com" not in u:
                            new_url = u
                            break
                        for pg in ctx.pages[1:]:
                            pu = pg.url
                            if pu.startswith(("http://", "https://")) and "iweicha.com" not in pu:
                                new_url = pu
                                break
                        if new_url:
                            break
                        page.wait_for_timeout(200)
                    for pg in ctx.pages[1:]:
                        try:
                            pg.close()
                        except Exception:
                            pass
                    if new_url:
                        m["original_url"] = new_url
                        m["original_url_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                        dump_json(p, m)
                        ok += 1
                    else:
                        miss += 1
                except Exception:
                    miss += 1
            browser.close()
        return ok, miss

    chunks = [targets[i::workers] for i in range(workers)]
    chunks = [c for c in chunks if c]
    t0 = time.time()
    tot_ok = tot_miss = 0
    with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
        futs = [ex.submit(worker, ch, i) for i, ch in enumerate(chunks)]
        for f in as_completed(futs):
            o, m_ = f.result()
            tot_ok += o
            tot_miss += m_
    print(f"      成功 {tot_ok}，未跳转 {tot_miss}（{time.time()-t0:.0f}s）", flush=True)
    return {"step": "links", "ok": tot_ok, "miss": tot_miss}


# ============================================================================
# 步骤 6：补全「正文在附件里」的条目
#   6.1 下载附件   6.2 文字型 PDF 提取   6.3 扫描型 PDF OCR
#   —— 直接扫原始目录，不依赖「待入库」（这是与旧脚本的关键差异）
# ============================================================================
ATT_MISSING_THRESHOLD = 300      # content.html 纯文本短于此值且含附件链接 → 视为正文缺失


def iter_source_dirs():
    """遍历两个源的原始条目目录 → yield (domain, col, dir_path)"""
    for base, cols, domain in [
        (PBC_ROOT, PBC_COLS, PBC_HOST),
        (NFRA_ROOT, NFRA_COLS, NFRA_HOST),
    ]:
        if not base.is_dir():
            continue
        for col in cols:
            cp = base / col
            if not cp.is_dir():
                continue
            for d in sorted(cp.iterdir()):
                if d.is_dir():
                    yield domain, col, d


def scan_attachment_targets():
    """扫描原始目录，返回正文缺失且含附件的条目。
    每项：{domain, col, dir_path, attachments:[(完整URL, 文件名)]}
    """
    items = []
    for domain, col, dp in iter_source_dirs():
        ch = dp / "content.html"
        if not ch.exists():
            continue
        html = ch.read_text(encoding="utf-8", errors="replace")
        if text_len(html) >= ATT_MISSING_THRESHOLD:
            continue
        atts = []
        for href in re.findall(r'<a[^>]+href=["\']([^"\']+)["\']', html, re.I):
            if not ATT_PAT.search(href):
                continue
            href = href.strip()
            if href.startswith("http"):
                full = href
            elif href.startswith("//"):
                full = "https:" + href
            elif href.startswith("/"):
                full = domain + href
            else:
                full = domain + "/" + href
            if full not in [u for u, _ in atts]:
                atts.append((full, att_filename(full)))
        if atts:
            items.append({"domain": domain, "col": col, "dir_path": dp,
                          "attachments": atts})
    return items


def att_filename(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    name = urllib.parse.unquote(path.split("/")[-1])
    name = re.sub(r'[\\/:*?"<>|]', "_", name).strip()
    if not name:
        name = "attachment_" + hashlib.md5(url.encode()).hexdigest()[:8]
    if not ATT_PAT.search(name):
        m = ATT_PAT.search(path)
        name += (m.group(0) if m else "")
    return name


def att_download_one(item, dry: bool = False) -> str:
    """下载一个条目的全部附件到 {dir}/attachments/，写 _manifest.json。"""
    ad = item["dir_path"] / "attachments"
    mfp = ad / "_manifest.json"
    manifest = load_json(mfp, {}) or {}
    results = []
    for url, fname in item["attachments"]:
        if manifest.get(fname, {}).get("ok"):
            continue
        if dry:
            results.append(f"[dry] {fname}")
            continue
        ad.mkdir(parents=True, exist_ok=True)
        dest = ad / fname
        try:
            data = http_get(url, headers={"Referer": item["domain"] + "/"}, timeout=60)
            if data[:5].lower() in (b"<html", b"<!doc"):
                manifest[fname] = {"url": url, "ok": False, "err": "返回HTML而非附件"}
                results.append(f"[HTML] {fname}")
                continue
            dest.write_bytes(data)
            manifest[fname] = {"url": url, "ok": True, "size": len(data),
                               "sha1": hashlib.sha1(data).hexdigest()}
            results.append(f"[OK {len(data)//1024}KB] {fname}")
        except Exception as e:
            manifest[fname] = {"url": url, "ok": False,
                               "err": f"{type(e).__name__}: {str(e)[:80]}"}
            results.append(f"[FAIL {type(e).__name__}] {fname}")
    if not dry:
        ad.mkdir(parents=True, exist_ok=True)
        dump_json(mfp, manifest)
    return f"{item['col']}/{item['dir_path'].name[:30]}: {'; '.join(results)}"


def pdf_kind(data: bytes) -> str:
    """粗判 PDF 是文字型还是扫描型。"""
    fonts = len(re.findall(rb"/Font\b", data))
    images = len(re.findall(rb"/Image\b", data))
    tounicode = b"ToUnicode" in data
    text_ops = len(re.findall(rb"(Tj|TJ)\b", data))
    if fonts > 0 and (tounicode or text_ops > 20):
        return "文字型"
    if images > 3 and fonts == 0:
        return "扫描型"
    if text_ops > 50:
        return "文字型"
    return "未知"


def att_extract_text(fp: Path) -> str | None:
    """用 pypdf 提取文字型 PDF；依赖缺失或失败返回 None。"""
    try:
        from pypdf import PdfReader
    except ImportError:
        return None
    try:
        rd = PdfReader(str(fp))
        t = "\n".join((pg.extract_text() or "") for pg in rd.pages)
        t = re.sub(r"[ \t\u3000]+", " ", t)
        t = re.sub(r"\n{3,}", "\n\n", t)
        return t.strip()
    except Exception:
        return None


def att_ocr_local(fp: Path, dpi: int = 200) -> str | None:
    """本地 RapidOCR 识别扫描 PDF；依赖缺失返回 None。"""
    if not (has_module("rapidocr_onnxruntime") and has_module("pymupdf")
            and has_module("numpy")):
        return None
    try:
        import numpy as np
        import pymupdf
        from rapidocr_onnxruntime import RapidOCR
        ocr = RapidOCR()
        doc = pymupdf.open(str(fp))
        lines = []
        try:
            for page in doc:
                pix = page.get_pixmap(dpi=dpi)
                arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                    pix.height, pix.width, pix.n)
                if pix.n == 4:
                    arr = arr[:, :, :3]
                img = np.ascontiguousarray(arr[:, :, ::-1])
                res, _ = ocr(img)
                if res:
                    lines.extend((it[1] or "").strip() for it in res if (it[1] or "").strip())
        finally:
            doc.close()
        return "\n".join(lines)
    except Exception:
        return None


# ---------------------------- 远程 OCR 服务 ----------------------------
OCR_JOB_URL = "https://paddleocr.aistudio-app.com/api/v2/ocr/jobs"
OCR_TOKEN = "6e481027b79abea04ecda306a128aca46f02a0ad"
OCR_MODEL = "PP-OCRv6"
OCR_HEADERS = {"Authorization": f"bearer {OCR_TOKEN}"}
OCR_OPTIONAL = {"useDocOrientationClassify": False, "useDocUnwarping": False,
                "useTextlineOrientation": False}


def att_ocr_remote(fp: Path, max_wait: int = 900) -> str | None:
    """远程 PaddleOCR 识别；requests 缺失或请求失败返回 None。"""
    if not has_module("requests"):
        return None
    try:
        import requests
    except ImportError:
        return None
    try:
        with open(fp, "rb") as f:
            r = requests.post(OCR_JOB_URL, headers=OCR_HEADERS,
                              data={"model": OCR_MODEL,
                                    "optionalPayload": json.dumps(OCR_OPTIONAL)},
                              files={"file": f}, timeout=300)
        if r.status_code != 200:
            return None
        job = r.json()["data"]["jobId"]
        t0 = time.time()
        while time.time() - t0 < max_wait:
            rr = requests.get(f"{OCR_JOB_URL}/{job}", headers=OCR_HEADERS, timeout=60)
            if rr.status_code != 200:
                time.sleep(5)
                continue
            d = rr.json()["data"]
            st = d.get("state")
            if st == "failed":
                return None
            if st == "done":
                jl = requests.get(d["resultUrl"]["jsonUrl"], timeout=300)
                jl.raise_for_status()
                pages = []
                for line in jl.text.strip().split("\n"):
                    if not line.strip():
                        continue
                    res = json.loads(line)["result"]
                    for o in res.get("ocrResults", []):
                        pr = o.get("prunedResult")
                        if isinstance(pr, str):
                            try:
                                pr = json.loads(pr)
                            except Exception:
                                pr = {}
                        texts = (pr or {}).get("rec_texts") or []
                        pages.append("\n".join(t for t in texts if t and t.strip()))
                return "\n".join(pages).strip()
            time.sleep(5)
    except Exception:
        return None
    return None


def run_attachments(workers: int = 6, dry: bool = False,
                    no_ocr: bool = False, dpi: int = 200) -> dict:
    """
    步骤 6：下载附件 → 文字型 PDF 提文 → 扫描型 PDF OCR。
    结果写入 {原始条目目录}/_body.txt（爬数据阶段产物，供处理阶段取用）。
    """
    print("[6/6] 补全「正文在附件里」的条目 …", flush=True)
    items = scan_attachment_targets()
    n_att = sum(len(it["attachments"]) for it in items)
    print(f"      正文缺失且带附件：{len(items)} 条 / {n_att} 个附件", flush=True)
    if dry:
        return {"step": "attachments", "items": len(items), "dry": True}

    # 6.1 下载
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(att_download_one, it, False) for it in items]
        for f in as_completed(futs):
            f.result()
            ok += 1
    print(f"      6.1 附件下载完成（{ok}/{len(items)} 条处理）", flush=True)

    # 6.2 / 6.3 提取 + OCR
    pypdf_ok = has_module("pypdf")
    if not pypdf_ok:
        step_skip("文字型 PDF 提取", "未安装 pypdf（pip install pypdf）")
    local_ocr = has_module("rapidocr_onnxruntime") and has_module("pymupdf")
    if not local_ocr and not no_ocr:
        print("      本地 OCR 依赖缺失，扫描件将改走远程服务", flush=True)

    stat = {"文字型提取": 0, "本地OCR": 0, "远程OCR": 0, "无附件": 0, "OCR失败": 0}
    tasks = []
    for it in items:
        ad = it["dir_path"] / "attachments"
        if not ad.is_dir():
            stat["无附件"] += 1
            continue
        tasks.append(it)

    def work(it):
        ad = it["dir_path"] / "attachments"
        texts, need_ocr = [], []
        for fp in sorted(ad.glob("*")):
            if fp.name.startswith("_") or not fp.is_file():
                continue
            if fp.suffix.lower() != ".pdf":
                continue
            data = fp.read_bytes()
            kind = pdf_kind(data)
            if kind == "文字型" and pypdf_ok:
                t = att_extract_text(fp)
                if t and len(t) > 100:
                    texts.append(t)
                    stat["文字型提取"] += 1
                    continue
            elif kind == "扫描型" or kind == "未知":
                need_ocr.append(fp)
        # OCR 兜底
        if need_ocr and not no_ocr:
            for fp in need_ocr:
                t = att_ocr_local(fp, dpi)
                if t and len(t) > 100:
                    texts.append(t)
                    stat["本地OCR"] += 1
                    continue
                if not local_ocr:
                    t2 = att_ocr_remote(fp)
                    if t2 and len(t2) > 100:
                        texts.append(t2)
                        stat["远程OCR"] += 1
                    else:
                        stat["OCR失败"] += 1
        body = "\n\n".join(texts).strip()
        if body and len(body) >= 100:
            (it["dir_path"] / "_body.txt").write_text(body, encoding="utf-8")
        return len(body)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, it) for it in tasks]
        for i, f in enumerate(as_completed(futs), 1):
            try:
                f.result()
            except Exception as e:
                print(f"      提取异常 {type(e).__name__}: {str(e)[:80]}", flush=True)
            if i % 20 == 0 or i == len(tasks):
                print(f"      {i}/{len(tasks)}", flush=True)

    print(f"      6.2/6.3 结果：{stat}", flush=True)
    return {"step": "attachments", "items": len(items), **stat}


# ============================================================================
# 主流程
# ============================================================================
STEPS = ["nfra", "pbc", "iweicha", "iweicha-index", "links", "attachments"]


def main():
    ap = argparse.ArgumentParser(description="阶段一：抓取原始数据")
    ap.add_argument("--only", default=None,
                    help=f"只跑某一步：{','.join(STEPS)}（可逗号分隔）")
    ap.add_argument("--skip-link", action="store_true", help="跳过补取原文链接")
    ap.add_argument("--skip-ocr", action="store_true", help="跳过 OCR")
    ap.add_argument("--dry-run", action="store_true", help="只盘点，不抓取")
    ap.add_argument("--workers", type=int, default=0, help="统一并发数（0=各步默认）")
    ap.add_argument("--types", type=int, nargs="+", default=[1, 2],
                    help="iweicha 机构：1金监局 2人行 3外汇局")
    ap.add_argument("--year", type=int, default=None, help="iweicha 只爬某年")
    args = ap.parse_args()

    dry = args.dry_run
    w = args.workers or 0

    only = None
    if args.only:
        only = {s.strip() for s in args.only.split(",") if s.strip()}

    def want(s):
        return only is None or s in only

    print("=" * 72)
    print(f"合规知识库 · 阶段一 爬数据")
    print(f"项目根：{ROOT}")
    print(f"{'[dry-run] ' if dry else ''}步骤：{sorted(only) if only else STEPS}")
    print("=" * 72)

    results = []
    t0 = time.time()

    try:
        if want("nfra"):
            results.append(run_nfra(workers=w or 4, dry=dry))
    except Exception as e:
        print(f"[1/6] NFRA 失败（已跳过）：{type(e).__name__} {e}", flush=True)

    try:
        if want("pbc"):
            results.append(run_pbc(workers=w or 6, dry=dry))
    except Exception as e:
        print(f"[2/6] 人行失败（已跳过）：{type(e).__name__} {e}", flush=True)

    try:
        if want("iweicha"):
            results.append(run_iweicha(types=tuple(args.types), workers=w or 8,
                                       year=args.year, dry=dry))
    except Exception as e:
        print(f"[3/6] iweicha 失败（已跳过）：{type(e).__name__} {e}", flush=True)

    try:
        if want("iweicha-index"):
            results.append(run_iw_index())
    except Exception as e:
        print(f"[4/6] 汇总索引失败（已跳过）：{type(e).__name__} {e}", flush=True)

    if args.skip_link:
        print("[5/6] 跳过补取原文链接（--skip-link）", flush=True)
    else:
        try:
            if want("links"):
                results.append(run_fetch_links(workers=w or 4, dry=dry))
        except Exception as e:
            print(f"[5/6] 补取原文链接失败（已跳过）：{type(e).__name__} {e}", flush=True)

    try:
        if want("attachments"):
            results.append(run_attachments(workers=w or 6, dry=dry,
                                           no_ocr=args.skip_ocr, dpi=200))
    except Exception as e:
        print(f"[6/6] 附件处理失败（已跳过）：{type(e).__name__} {e}", flush=True)

    print("=" * 72)
    print(f"阶段一完成，耗时 {time.time()-t0:.0f}s")
    for r in results:
        print(f"  {r}")
    print("=" * 72)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
