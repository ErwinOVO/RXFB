# -*- coding: utf-8 -*-
"""
处理数据.py —— 阶段二：清洗与合并（法规库流水线）
====================================================
把阶段一（爬数据.py）抓下来的两个源合并去重，产出规范的「待入库」结构，
并做正文体检、生成排除清单。**单文件自包含**，按顺序一次跑完。

    步骤 1  整理待入库：合并 iweicha 与官网，去重，按机构落盘
    步骤 2  补详情正文：对官网残缺正文，从 detail.json 的 docClob 补全
    步骤 3  正文体检：分级统计正文完整性，输出问题清单
    步骤 4  生成排除清单：把无法补救的条目列为不入库

输入：
    人民银行/{栏目}/{id}_{标题}/           content.txt content.html detail.json _meta.json
    国家金融监督管理总局/{栏目}/{id}_{标题}/  同上
    iweicha/{机构}/{年份}/{id}_{标题}/      content.txt _meta.json
    iweicha/_index.csv                     阶段一汇总的索引
    {原始条目目录}/_body.txt               阶段一从附件/OCR 提取的正文（若有）

输出（均写入 待入库_v2/）：
    待入库_v2/人民银行/{source_id}_{标题}/content.txt + _meta.json
    待入库_v2/国家金融监督管理总局/{source_id}_{标题}/content.txt + _meta.json
    待入库_v2/{机构}/_index.csv             供「入库.py」读取
    待入库_v2/_正文体检.csv                  正文质量明细
    待入库_v2/_排除清单.csv                  不入库清单

用法：
    python 处理数据.py                  # 全流程
    python 处理数据.py --dry-run        # 只分析，不写文件
    python 处理数据.py --only merge     # 只跑某一步（merge/docclob/health/exclude）
    python 处理数据.py --tu 待入库_v2    # 指定输出目录
"""
from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _project_root() -> Path:
    p = Path(__file__).resolve().parent
    for _ in range(6):
        if (p / "法规库.db").exists() or (p / ".workbuddy").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return p


ROOT = _project_root()
PBC_ROOT = ROOT / "人民银行"
NFRA_ROOT = ROOT / "国家金融监督管理总局"
IW_ROOT = ROOT / "iweicha"

ATT_PAT = re.compile(r"\.(pdf|docx?|xlsx?|wps|et|zip|rar|7z|pptx?|rtf)\b", re.I)


def load_json(p: Path, default=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def dump_json(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================================
# 标题归一化（用于跨源匹配，比入库判重更激进）
# ============================================================================
ORG_PAT = (r"^(中国银行业监督管理委员会|中国银监会|中国保险监督管理委员会|中国保监会|"
           r"中国银行保险监督管理委员会|中国银保监会|中国人民银行|国家金融监督管理总局|"
           r"银监会|保监会|银保监会|人民银行|金融监管总局|国务院办公厅|国务院|"
           r"全国人民代表大会常务委员会|财政部|国家外汇管理局|中国证券监督管理委员会|中国证监会)")
OFFICE = r"(办公厅|办公室|秘书局|公告)?"


def norm(t: str) -> str:
    """标题归一化（用于跨源匹配）"""
    if not t:
        return ""
    t = t.replace("\u3000", " ")
    t = re.sub(r"[（(][^）)]*[）)]", "", t)
    t = re.sub(r"[\s《》【】\[\]、,，。;；:：\-—－_/]", "", t)
    t = re.sub(r"令\d{4}年第\d+号", "", t)
    t = re.sub(r"公告\d{4}年第\d+号", "", t)
    t = re.sub(r"[〔\[（(]\d{4}[〕\]）)]?第?\d+号", "", t)
    t = re.sub(r"第\d+号", "", t)
    t = re.sub(r"^\d+(\.\d+)*", "", t)
    t = re.sub(ORG_PAT + OFFICE, "", t)
    t = re.sub(ORG_PAT + OFFICE, "", t)
    t = re.sub(r"^(关于印发|关于发布|关于施行|关于修订|印发|发布)", "", t)
    t = re.sub(r"(的通知|的公告|的函|的批复|的意见|的办法|的规定|的规范|的指引|的细则|的暂行规定)$", "", t)
    return t


def _grams(s: str, n: int = 4) -> set:
    """标题切成 n-gram 字符集合，用于相似度匹配（与顺序无关）"""
    s = re.sub(r"[\s《》（）()〔〕\[\]、，。；:：—\-－_/]+", "", s or "")
    return {s[i:i + n] for i in range(max(0, len(s) - n + 1))}


def _jac(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def safe_name(s: str, limit: int = 70) -> str:
    """清洗为合法目录名。Windows 不允许目录名以空格或点结尾。"""
    s = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", s or "")
    s = s.strip().strip(".")
    s = s[:limit].strip().strip(".")
    return s or "untitled"


def read_csv(p: Path) -> list[dict]:
    if not p.exists():
        return []
    with open(p, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def step_skip(step: str, why: str) -> None:
    print(f"      [跳过] {step} —— {why}", flush=True)


def read_body(d: Path | None) -> tuple[str, str]:
    """
    读取某条目目录下的正文，返回 (正文, 来源标记)。

    两个候选：
        content.txt   爬取时抓到的**页面正文**（正文在附件里时，这里只剩附件文件名）
        _body.txt     阶段一从附件 PDF / OCR 提取的正文

    取舍规则（保守，不会让附件正文顶掉完整页面正文）：
        1. 页面正文「正常」（>=120 字且不像附件名）→ 用页面正文
        2. 页面正文缺失/像附件名/极短，而附件正文更长 → 用附件正文
        3. 都没有 → 空
    """
    if d is None:
        return "", ""
    d = Path(d)
    ct = d / "content.txt"
    bt = d / "_body.txt"
    page = (ct.read_text(encoding="utf-8", errors="replace") if ct.exists() else "").strip()
    body = (bt.read_text(encoding="utf-8", errors="replace") if bt.exists() else "").strip()

    page_ok = len(page) >= 120 and not _looks_like_attachment_name(page)
    if page_ok:
        return page, "content.txt"
    if len(body) > len(page):
        return body, "_body.txt"
    if page:
        return page, "content.txt"
    return body, ("_body.txt" if body else "")


def _looks_like_attachment_name(text: str) -> bool:
    """判断正文是否只是「附件文件名」：很短、含扩展名、行数很少"""
    lines = [l for l in (text or "").split("\n") if l.strip()]
    return bool(ATT_PAT.search(text or "")) and len(lines) <= 3


# ============================================================================
# 步骤 1：整理待入库
# ============================================================================
def load_iweicha(org_name: str) -> list[dict]:
    rows = read_csv(IW_ROOT / "_index.csv")
    out = []
    for r in rows:
        if r.get("org") != org_name:
            continue
        d = Path(r["dir_path"]) if r.get("dir_path") else None
        if d and not d.is_absolute():
            d = ROOT / d
        out.append({
            "sys": "iweicha", "sid": r.get("file_id", ""), "org": org_name,
            "title": r.get("title", ""), "year": r.get("year", ""),
            "status": (r.get("status") or "").strip(),
            "content_len": int(r["content_len"]) if str(r.get("content_len", "")).isdigit() else 0,
            "detail_url": r.get("url", ""),
            "original_url": (r.get("original_url") or "").strip(),
            "dir": d, "pub_date": "", "doc_number": "",
        })
    return out


def load_nfra() -> list[dict]:
    """官网金监总局：政策规章规范性文件 + 法律法规"""
    base = NFRA_ROOT / "政策规章规范性文件"
    out = []
    for r in read_csv(base / "_index.csv"):
        folder = r.get("folder", "")
        d = base / folder if folder else None
        pd = (r.get("publishDate") or "")[:10]
        out.append({
            "sys": "nfra", "sid": r.get("docId", ""), "org": "国家金融监督管理总局",
            "title": r.get("title", ""), "year": pd[:4], "status": "",
            "content_len": 0,
            "detail_url": r.get("source_url", ""),
            "original_url": r.get("source_url", ""),
            "dir": d, "pub_date": pd, "doc_number": "",
        })
    # 法律法规 18 条（手工入库的早期数据）
    lf = NFRA_ROOT / "法律法规"
    if lf.is_dir():
        for name in os.listdir(lf):
            d = lf / name
            if not d.is_dir():
                continue
            out.append({
                "sys": "nfra", "sid": name.split("_")[0], "org": "国家金融监督管理总局",
                "title": re.sub(r"^\d+_", "", name), "year": "", "status": "",
                "content_len": 0, "detail_url": "", "original_url": "",
                "dir": d, "pub_date": "", "doc_number": "",
            })
    return out


def load_pbc() -> list[dict]:
    out = []
    for r in read_csv(PBC_ROOT / "_index.csv"):
        col = r.get("colName", "")
        folder = r.get("folder", "")
        d = PBC_ROOT / col / folder if col and folder else None
        dt = r.get("date", "") or ""
        cl = r.get("content_len", "")
        out.append({
            "sys": "pbc", "sid": r.get("articleId", ""), "org": "人民银行",
            "title": r.get("title", ""), "year": dt[:4], "status": "",
            "content_len": int(cl) if str(cl).isdigit() else 0,
            "detail_url": r.get("url", ""), "original_url": r.get("url", ""),
            "dir": d, "pub_date": dt, "doc_number": "",
        })
    return out


def match(iw_list: list[dict], off_list: list[dict]):
    """返回 (merged, stats)。

    ⚠️ iweicha 侧「逐条保留」——不能按归一化 key 去重，
       否则同源内标题归一化后相同的不同文件会被误合并丢掉。
       官网侧用 used 集合保证一条官网记录只被匹配一次。
    """
    off_norm = [(norm(r["title"]), _grams(r["title"]), r) for r in off_list]
    used, merged, matched = set(), [], 0
    by_rule = {"精确": 0, "子串": 0, "相似": 0}

    for iw in iw_list:
        k = norm(iw["title"])
        g = _grams(iw["title"])
        off, rule = None, ""
        # 1) 精确匹配
        if k:
            for ck, cg, cand in off_norm:
                if ck == k and id(cand) not in used:
                    off, rule = cand, "精确"
                    break
        # 2) 子串双向（长度 >= 8，取长度差最小）
        if off is None and len(k) >= 8:
            best, best_gap = None, 10 ** 9
            for ck, cg, cand in off_norm:
                if id(cand) in used or len(ck) < 8:
                    continue
                if k in ck or ck in k:
                    gap = abs(len(k) - len(ck))
                    if gap < best_gap:
                        best, best_gap = cand, gap
            if best:
                off, rule = best, "子串"
        # 3) 相似度匹配（4-gram Jaccard ≥ 0.5）
        if off is None and g:
            best, best_s = None, 0.5
            for ck, cg, cand in off_norm:
                if id(cand) in used or not cg:
                    continue
                s = _jac(g, cg)
                if s > best_s:
                    best_s, best = s, cand
            if best:
                off, rule = best, "相似"

        if off is not None:
            used.add(id(off))
            matched += 1
            by_rule[rule] += 1

        base = dict(iw)
        if off is not None:
            use_iw = iw["content_len"] >= 100
            base["content_from"] = "iweicha" if use_iw else "官网"
            if not use_iw:
                base["content_len"] = off["content_len"]
                base["dir"] = off["dir"]
            base["src_sys"] = "iweicha+官网"
            base["pub_date"] = base.get("pub_date") or off.get("pub_date", "")
            if not base.get("original_url"):
                base["original_url"] = off.get("original_url", "")
            base["_off"] = off
        else:
            base["content_from"] = "iweicha"
            base["src_sys"] = "iweicha"
            base["_off"] = None
        merged.append(base)

    # 官网独有
    for cand in off_list:
        if id(cand) in used:
            continue
        r = dict(cand)
        r["content_from"] = "官网"
        r["src_sys"] = "官网"
        r["_off"] = None
        merged.append(r)

    stats = {"iw_total": len(iw_list), "off_total": len(off_list),
             "matched": matched, "merged": len(merged),
             "off_only": len(off_list) - matched, "by_rule": by_rule}
    return merged, stats


def emit(rows: list[dict], org_name: str, out_root: Path, dry_run: bool):
    """落盘一个机构的合并结果。

    ⚠️ 性能：不整体 rmtree 后重建（3186 个目录在 Windows 上要十几分钟，
       rmtree 遇杀毒扫描会卡死）。改为「覆盖写 + 事后清理多余目录」。
    """
    out_dir = out_root / org_name
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    index_rows, copied, skipped = [], 0, 0
    src_counter = Counter()
    written_dirs = set()
    for r in rows:
        # 正文来源：content.txt 优先，其次阶段一提取的 _body.txt
        content, src = read_body(r.get("dir"))
        if not content:
            skipped += 1
        src_counter[src or "(空)"] += 1

        sid = str(r.get("sid") or "")
        dir_name = f"{sid}_{safe_name(r.get('title', ''))}"
        written_dirs.add(dir_name)
        if not dry_run:
            d = out_dir / dir_name
            d.mkdir(parents=True, exist_ok=True)
            (d / "content.txt").write_text(content, encoding="utf-8")
            dump_json(d / "_meta.json", {
                "source_id": f"{r.get('sys')}:{sid}",
                "org": org_name,
                "title": r.get("title", ""),
                "doc_number": r.get("doc_number", ""),
                "pub_date": r.get("pub_date", ""),
                "status": r.get("status", ""),
                "content_len": len(content),
                "content_from": r.get("content_from", ""),
                "content_src": src,
                "source_sys": r.get("src_sys", ""),
                "detail_url": r.get("detail_url", ""),
                "original_url": r.get("original_url", ""),
            })
        copied += 1
        index_rows.append({
            "source_id": f"{r.get('sys')}:{sid}", "org": org_name,
            "title": r.get("title", ""), "doc_number": r.get("doc_number", ""),
            "pub_date": r.get("pub_date", ""), "status": r.get("status", ""),
            "content_len": len(content),
            "content_from": r.get("content_from", ""),
            "source_sys": r.get("src_sys", ""),
            "detail_url": r.get("detail_url", ""),
            "original_url": r.get("original_url", ""),
            "dir_name": dir_name,
        })

    # 清理本轮未产出的陈旧目录（上一轮「官网独有、本轮已匹配上」的残留）
    removed = 0
    if not dry_run:
        for child in out_dir.iterdir():
            if child.is_dir() and child.name not in written_dirs:
                try:
                    shutil.rmtree(child, ignore_errors=True)
                    removed += 1
                except Exception:
                    pass

    index_rows.sort(key=lambda x: (x["pub_date"] or "", x["source_id"]), reverse=True)
    if not dry_run and index_rows:
        with (out_dir / "_index.csv").open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(index_rows[0].keys()))
            w.writeheader()
            w.writerows(index_rows)
    return copied, skipped, index_rows, src_counter, removed


def refresh_content_len(rows: list[dict]) -> None:
    """
    用真实可读正文刷新 content_len。
    阶段一可能把正文写到 _body.txt（附件/OCR 提取），索引里的 content_len
    只反映 content.txt，直接拿它做「选哪个源的正文」判断会误判。
    """
    for r in rows:
        content, _src = read_body(r.get("dir"))
        r["content_len"] = len(content.strip())


def run_merge(out_root: Path, dry: bool = False) -> dict:
    print("[1/4] 整理待入库（合并去重）…", flush=True)
    iw_r, iw_j = load_iweicha("人行"), load_iweicha("金监局")
    off_p, off_n = load_pbc(), load_nfra()
    # 用真实正文长度刷新（含阶段一提取的 _body.txt）
    for lst in (iw_r, iw_j, off_p, off_n):
        refresh_content_len(lst)
    print(f"      载入 iweicha人行 {len(iw_r)} | iweicha金监局 {len(iw_j)} | "
          f"官网PBOC {len(off_p)} | 官网NFRA {len(off_n)}", flush=True)

    total = 0
    for org, iws, offs, outname in [
        ("人民银行", iw_r, off_p, "人民银行"),
        ("国家金融监督管理总局", iw_j, off_n, "国家金融监督管理总局"),
    ]:
        merged, st = match(iws, offs)
        by_from = Counter(r["content_from"] for r in merged)
        n_url = sum(1 for r in merged if r.get("original_url"))
        print(f"      【{org}】iweicha {st['iw_total']} + 官网 {st['off_total']} "
              f"=> 匹配 {st['matched']} 对 => 合并 {st['merged']} 条", flush=True)
        print(f"        匹配方式 {st['by_rule']} | 正文来源 {dict(by_from)} | "
              f"有原文链接 {n_url}/{st['merged']}", flush=True)
        copied, skipped, _, src_counter, removed = emit(merged, outname, out_root, dry)
        print(f"        {'[dry] 待写入' if dry else '已写入'} {copied} 条"
              f"（正文为空 {skipped}）| 正文来源明细 {dict(src_counter)}"
              f"{f' | 清理陈旧目录 {removed}' if removed else ''}", flush=True)
        total += copied
    return {"step": "merge", "total": total, "dry": dry}


# ============================================================================
# 步骤 2：补详情正文（官网 docClob）
# ============================================================================
MSO = re.compile(r"MicrosoftInternetExplorer\d*|DocumentNotSpecified|[\d.]+\s*磅|Web0", re.I)


def clean_docclob(html: str) -> str:
    """清洗 Word 导出 HTML，返回纯文本"""
    if not html:
        return ""
    s = html
    s = re.sub(r"<!--.*?-->", " ", s, flags=re.S)
    s = re.sub(r"<(script|style|xml)[^>]*>.*?</\1>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</(p|div|tr|h[1-6]|li)>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html_lib.unescape(s)
    s = re.sub(r"@font-face\s*\{[^}]*\}", " ", s)
    s = MSO.sub(" ", s)
    s = re.sub(r"[ \t\u3000]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)
    return s.strip()


def cn_ratio(s: str) -> float:
    if not s:
        return 0.0
    return sum(1 for c in s if "\u4e00" <= c <= "\u9fff") / len(s)


def build_src_map() -> dict:
    """{tag:source_id -> (col, dir_path)}，用于回溯官网原始目录"""
    mp = {}
    for base, tag in [(PBC_ROOT, "pbc"), (NFRA_ROOT, "nfra")]:
        if not base.is_dir():
            continue
        for col in os.listdir(base):
            cp = base / col
            if not cp.is_dir():
                continue
            for d in os.listdir(cp):
                if os.path.isdir(cp / d) and "_" in d:
                    mp[f"{tag}:{d.split('_')[0]}"] = (col, cp / d)
    return mp


def run_docclob(out_root: Path, dry: bool = False) -> dict:
    print("[2/4] 补详情正文（官网 docClob）…", flush=True)
    hc = out_root / "_正文体检.csv"
    if not hc.exists():
        step_skip("补详情正文", "缺少 _正文体检.csv（本步需在体检之后跑，或先跑 --only health）")
        return {"step": "docclob", "skipped": "no-health-csv"}
    rows = [r for r in read_csv(hc)
            if r["source_sys"] == "官网" and r["问题"] in ("极短(<100)", "空")]
    print(f"      官网「极短/空」条目 {len(rows)}", flush=True)
    mp = build_src_map()

    wrote = 0
    for r in rows:
        hit = mp.get(r["source_id"])
        if not hit:
            continue
        col, d = hit
        dj = d / "detail.json"
        if not dj.exists():
            continue
        raw = (load_json(dj, {}) or {}).get("docClob") or ""
        txt = clean_docclob(raw)
        target = out_root / r["org"] / r["dir_name"]
        cp = target / "content.txt"
        cur = cp.read_text(encoding="utf-8", errors="replace").strip() if cp.exists() else ""
        better = len(txt) > len(cur) + 30 and cn_ratio(txt) > 0.45
        if better:
            print(f"      ✓ [{col}] {r['title'][:46]}  {len(cur)}→{len(txt)} 字", flush=True)
            if not dry and target.is_dir():
                cp.write_text(txt, encoding="utf-8")
                mfp = target / "_meta.json"
                if mfp.exists():
                    meta = load_json(mfp, {}) or {}
                    meta["content_len"] = len(txt)
                    meta["content_from"] = "官网docClob"
                    meta["content_src"] = "detail.json.docClob"
                    dump_json(mfp, meta)
                wrote += 1
    print(f"      {'[dry] 可补' if dry else '已补'} {wrote} 条", flush=True)
    return {"step": "docclob", "wrote": wrote, "dry": dry}


# ============================================================================
# 步骤 3：正文体检
# ============================================================================
def classify_text(text: str) -> str:
    t = (text or "").strip()
    n = len(t)
    if n == 0:
        return "空"
    if n < 120:
        lines = [l for l in t.split("\n") if l.strip()]
        if ATT_PAT.search(t) and len(lines) <= 3:
            return "仅附件名"
        return "极短(<100)"
    if n < 500:
        return "偏短(100-500)"
    return "正常(>=500)"


def run_health(out_root: Path) -> dict:
    print("[3/4] 正文体检 …", flush=True)
    detail = []
    if not out_root.is_dir():
        print("      待入库目录不存在", flush=True)
        return {"step": "health", "rows": 0}

    for org_dir in sorted(out_root.iterdir()):
        if not org_dir.is_dir():
            continue
        org = org_dir.name
        rows = read_csv(org_dir / "_index.csv")
        stat = defaultdict(Counter)
        for r in rows:
            p = org_dir / r["dir_name"] / "content.txt"
            txt = p.read_text(encoding="utf-8", errors="replace") if p.exists() else ""
            cat = classify_text(txt)
            stat[r["source_sys"]][cat] += 1
            if cat in ("仅附件名", "空", "极短(<100)"):
                detail.append({
                    "org": org, "source_id": r["source_id"],
                    "source_sys": r["source_sys"], "content_from": r["content_from"],
                    "title": r["title"], "content_len": len(txt.strip()),
                    "问题": cat,
                    "正文前80字": re.sub(r"\s+", " ", txt.strip())[:80],
                    "detail_url": r["detail_url"], "original_url": r["original_url"],
                    "dir_name": r["dir_name"],
                })

        print(f"      【{org}】共 {len(rows)} 条", flush=True)
        allcat = Counter()
        for sysname in ["iweicha", "iweicha+官网", "官网"]:
            if sysname not in stat:
                continue
            c = stat[sysname]
            allcat.update(c)
            tot = sum(c.values())
            bad = c["仅附件名"] + c["空"] + c["极短(<100)"]
            print(f"        {sysname:16s} 共{tot:5d} | 仅附件名 {c['仅附件名']:4d} | "
                  f"空 {c['空']:3d} | 极短 {c['极短(<100)']:4d} | "
                  f"偏短 {c['偏短(100-500)']:4d} | 正常 {c['正常(>=500)']:5d}"
                  f"  ←问题率 {bad*100//max(tot,1)}%", flush=True)

    out = out_root / "_正文体检.csv"
    fields = ["org", "source_id", "source_sys", "content_from", "title", "content_len",
              "问题", "正文前80字", "detail_url", "original_url", "dir_name"]
    detail.sort(key=lambda x: (x["org"], x["source_sys"], x["content_len"]))
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(detail)
    print(f"      问题明细：{out.name}（{len(detail)} 条）", flush=True)
    for k, v in Counter((d["org"], d["source_sys"]) for d in detail).most_common():
        print(f"        {k[0]:22s} {k[1]:16s} {v:5d}", flush=True)
    return {"step": "health", "rows": len(detail)}


# ============================================================================
# 步骤 4：生成排除清单
# ============================================================================
EXCLUDE_REASON = {
    ("极短(<100)", "iweicha"): "iweicha正文仅标题+页码，官网也没有",
    ("仅附件名", "iweicha+官网"): "两个源都只有附件名",
    ("仅附件名", "官网"): "官网只有附件名/扫描件已尽力",
    ("极短(<100)", "官网"): "官网正文残缺或本身极短",
    ("空", "官网"): "官网正文为空",
}


def run_exclude(out_root: Path) -> dict:
    print("[4/4] 生成排除清单 …", flush=True)
    hc = out_root / "_正文体检.csv"
    if not hc.exists():
        step_skip("生成排除清单", "缺少 _正文体检.csv")
        return {"step": "exclude", "skipped": "no-health-csv"}
    rows = read_csv(hc)
    out = []
    for r in rows:
        key = (r["问题"], r["source_sys"])
        out.append({
            "org": r["org"], "source_id": r["source_id"],
            "source_sys": r["source_sys"], "title": r["title"],
            "content_len": r["content_len"], "问题": r["问题"],
            "排除原因": EXCLUDE_REASON.get(key, f"{r['问题']} / {r['source_sys']}"),
            "dir_name": r["dir_name"],
        })
    out.sort(key=lambda x: (x["org"], x["source_sys"], int(x["content_len"] or 0)))
    p = out_root / "_排除清单.csv"
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys()) if out else
                           ["org", "source_id", "source_sys", "title", "content_len",
                            "问题", "排除原因", "dir_name"])
        w.writeheader()
        w.writerows(out)

    print(f"      排除清单：{p.name}（{len(out)} 条）", flush=True)
    for k, v in Counter(r["问题"] for r in out).most_common():
        print(f"        {k:14s} {v:4d}", flush=True)

    # 入库后剩余
    print("      入库后剩余条目：", flush=True)
    tot = 0
    for org_dir in sorted(out_root.iterdir()):
        if not org_dir.is_dir():
            continue
        f = org_dir / "_index.csv"
        if not f.exists():
            continue
        n = len(read_csv(f))
        exc = sum(1 for r in out if r["org"] == org_dir.name)
        print(f"        {org_dir.name:22s} {n:5d} - {exc:3d} = {n-exc:5d}", flush=True)
        tot += n - exc
    print(f"        {'合计':22s} {'':5s}   {'':3s}   {tot:5d}", flush=True)
    return {"step": "exclude", "rows": len(out), "remain": tot}


# ============================================================================
# 主流程
# ============================================================================
STEPS = ["merge", "docclob", "health", "exclude"]


def main():
    ap = argparse.ArgumentParser(description="阶段二：清洗与合并")
    ap.add_argument("--only", default=None, help=f"只跑某一步：{','.join(STEPS)}")
    ap.add_argument("--dry-run", action="store_true", help="只分析，不写文件")
    ap.add_argument("--tu", default="待入库_v2", help="输出目录（默认 待入库_v2）")
    args = ap.parse_args()

    out_root = ROOT / args.tu
    dry = args.dry_run
    only = {s.strip() for s in args.only.split(",")} if args.only else None

    def want(s):
        return only is None or s in only

    print("=" * 72)
    print("合规知识库 · 阶段二 处理数据")
    print(f"项目根：{ROOT}")
    print(f"输出目录：{out_root}")
    print(f"{'[dry-run] ' if dry else ''}步骤：{sorted(only) if only else STEPS}")
    print("=" * 72)

    # 合并必须先跑（体检依赖合并产物）
    if not dry and want("merge"):
        out_root.mkdir(parents=True, exist_ok=True)

    results = []
    if want("merge"):
        results.append(run_merge(out_root, dry))
    # ⚠️ 顺序：体检 → 补 docClob → 排除清单
    #    体检产出 _正文体检.csv，补 docClob 按它定位目标；排除清单基于体检结果。
    if want("health"):
        if (out_root / "人民银行" / "_index.csv").exists():
            results.append(run_health(out_root))
        else:
            step_skip("正文体检", "缺少待入库索引，请先跑 merge 步骤")
    if want("docclob"):
        results.append(run_docclob(out_root, dry))
    if want("exclude"):
        # 补完 docClob 后正文可能变长，重体检一次让排除清单反映最新质量
        if want("docclob") and not dry and (out_root / "_正文体检.csv").exists():
            print("[复检] 补正文后重新体检 …", flush=True)
            results.append(run_health(out_root))
        results.append(run_exclude(out_root))

    print("=" * 72)
    print("阶段二完成")
    for r in results:
        print(f"  {r}")
    print("=" * 72)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(130)
