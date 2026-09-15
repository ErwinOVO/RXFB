# -*- coding: utf-8 -*-
"""
入库.py —— 阶段三：灌库与回填（法规库流水线）
====================================================
把阶段二（处理数据.py）产出的「待入库_v2」灌进 法规库.db，
并把迁移、回填、分类、索引等收尾动作一次跑完。**单文件自包含**。

    步骤 1  schema 迁移：v1→v2 结构升级（hierarchy/issuer/regulation_issuer + 加列）
    步骤 2  分类迁移：reg_category 加 is_primary、新增「央行专项」7 专题、专题排序
    步骤 3  主入库：把 _index.csv 逐条灌进 regulation（判重、机构、层级、分类、指纹）
    步骤 4  补早期18条：补齐最早期手工入库条目的 source_id/scope/content_hash
    步骤 5  补文号：从标题抽文号，回填空的 doc_number
    步骤 6  补发布日期：从 iweicha 详情页/正文落款解析 pub_date
    步骤 7  校正发布日期：修正 pub_date 与文号年份冲突的条目
    步骤 8  补生效日期：从正文附则挖施行日期，回填 effective_date
    步骤 9  重跑分类：用 category_rules.json 重建 reg_category（主/辅分类）
    步骤 10 重建索引：从各条目 _meta.json 重建 待入库 目录的 _index.csv

设计要点：
  - 依赖前置步骤的产物（如补文号依赖主入库；重跑分类依赖迁移后的 category 表），
    因此**顺序固定、默认全跑**；每步内部幂等，重复执行安全。
  - 每步独立 try/except：单步失败只跳过该步，不中断整个流程。
  - `法规库.db` 结构迁移前会自动备份为 `法规库_backup_<时间戳>.db`。

用法：
    python 入库.py                       # 全流程（会写库）
    python 入库.py --dry-run             # 只分析，不写库
    python 入库.py --only regs,classify  # 只跑某几步
    python 入库.py --tu 待入库_v2         # 指定待入库目录
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime, timedelta
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
DB = ROOT / "法规库.db"
IW = ROOT / "iweicha"
CATEGORY_RULES = ROOT / "data" / "taxonomy" / "category_rules.json"

# 运行时全局（与旧脚本保持一致：由 load_dicts 填充）
H_ID: dict = {}
I_ID: dict = {}
ISSUER_ALIAS: dict = {}
CAT_ID: dict = {}
_org_cache: dict = {}


def step_skip(step: str, why: str) -> None:
    print(f"      [跳过] {step} —— {why}", flush=True)


def has_module(name: str) -> bool:
    import importlib.util
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


# ============================================================================
# 步骤 1：schema 迁移 v1 → v2
# ============================================================================

# 现有 hierarchy TEXT 值 → hierarchy 表二级叶子节点 name（迁移映射）
HIER_TEXT_TO_LEAF = {
    "法律": "法律",
    "行政法规": "行政法规",
    "司法解释": "两高司法解释",
    "部门规章": "部门规章",
    "地方法规": "省级地方法规",
    "地方性法规": "省级地方法规",
    "行业规范": "行业规范（法规）",
}


def backup_db() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = ROOT / f"法规库_backup_{ts}.db"
    shutil.copy2(DB, dst)
    print(f"      [备份] {DB.name} → {dst.name}", flush=True)


def add_column(conn, table, col, ddl) -> bool:
    """幂等加列。"""
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]
    if col in cols:
        return False
    conn.execute(f'ALTER TABLE "{table}" ADD COLUMN {ddl}')
    return True


def create_hierarchy(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS hierarchy (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        parent_id   INTEGER REFERENCES hierarchy(id),
        level       INTEGER DEFAULT 1,
        sort_order  INTEGER DEFAULT 0
    )""")
    rows = [
        (1, '法律', None, 1, 1), (2, '行政法规', None, 1, 2), (3, '司法解释', None, 1, 3),
        (4, '部门规章', None, 1, 4), (5, '地方法规', None, 1, 5), (6, '行业规范', None, 1, 6),
        (11, '法律', 1, 2, 1), (12, '全国人大文件', 1, 2, 2), (13, '重大问题决定', 1, 2, 3),
        (14, '条约批准', 1, 2, 4), (15, '法律释义', 1, 2, 5),
        (21, '行政法规', 2, 2, 1), (22, '国务院其他文件', 2, 2, 2),
        (31, '两高司法解释', 3, 2, 1), (32, '地方性司法解释', 3, 2, 2), (33, '两高其他文件', 3, 2, 3),
        (34, '两高白皮书及案例', 3, 2, 4), (35, '地方司法白皮书及案例', 3, 2, 5),
        (41, '规范性文件及通知', 4, 2, 1), (42, '部门规章', 4, 2, 2),
        (51, '省级地方法规', 5, 2, 1), (52, '省级地方政府规章', 5, 2, 2), (53, '其他地方文件', 5, 2, 3),
        (61, '行业规范（法规）', 6, 2, 1),
    ]
    for r in rows:
        conn.execute("INSERT OR IGNORE INTO hierarchy(id,name,parent_id,level,sort_order) "
                     "VALUES(?,?,?,?,?)", r)


def create_issuer(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS issuer (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT NOT NULL UNIQUE,
        full_name    TEXT,
        org_type     TEXT,
        admin_level  TEXT,
        former_names TEXT,
        status       TEXT DEFAULT '现行',
        sort_order   INTEGER DEFAULT 0
    )""")
    rows = [
        (1, '全国人民代表大会常务委员会', '全国人民代表大会常务委员会', '人大', '国家级', '', '现行', 1),
        (2, '国务院', '中华人民共和国国务院', '国务院', '国家级', '', '现行', 2),
        (13, '国务院办公厅', '国务院办公厅', '国务院', '国家级', '国办', '现行', 3),
        (3, '国家金融监督管理总局', '国家金融监督管理总局', '部委', '部委级', '中国银保监会,中国银监会,中国保监会', '现行', 10),
        (4, '中国人民银行', '中国人民银行', '部委', '部委级', '人民银行', '现行', 11),
        (5, '中国证券监督管理委员会', '中国证券监督管理委员会', '部委', '部委级', '证监会', '现行', 12),
        (6, '国家外汇管理局', '国家外汇管理局', '部委', '部委级', '外汇局', '现行', 13),
        (7, '财政部', '中华人民共和国财政部', '部委', '部委级', '', '现行', 14),
        (8, '国家发展和改革委员会', '国家发展和改革委员会', '部委', '部委级', '发改委', '现行', 15),
        (9, '国家税务总局', '国家税务总局', '部委', '部委级', '税务总局', '现行', 16),
        (10, '司法部', '中华人民共和国司法部', '部委', '部委级', '', '现行', 17),
        (11, '最高人民法院', '最高人民法院', '两高', '国家级', '', '现行', 20),
        (12, '最高人民检察院', '最高人民检察院', '两高', '国家级', '', '现行', 21),
        (20, '中国银行保险监督管理委员会', '中国银行保险监督管理委员会', '部委', '部委级', '银保监会', '已撤销', 30),
        (21, '中国银行业监督管理委员会', '中国银行业监督管理委员会', '部委', '部委级', '银监会', '已撤销', 31),
        (22, '中国保险监督管理委员会', '中国保险监督管理委员会', '部委', '部委级', '保监会', '已撤销', 32),
    ]
    for r in rows:
        conn.execute("INSERT OR IGNORE INTO issuer(id,name,full_name,org_type,admin_level,"
                     "former_names,status,sort_order) VALUES(?,?,?,?,?,?,?,?)", r)


def create_reg_issuer(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS regulation_issuer (
        reg_id      INTEGER NOT NULL REFERENCES regulation(id),
        issuer_id   INTEGER NOT NULL REFERENCES issuer(id),
        is_primary  INTEGER DEFAULT 0,
        PRIMARY KEY (reg_id, issuer_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reg_issuer ON regulation_issuer(issuer_id)")


def resolve_issuer_id(conn, text):
    """issuer 文本 → issuer.id；精确匹配 name/former_names，找不到则自动新建。"""
    text = (text or "").strip()
    if not text:
        return None
    row = conn.execute("SELECT id FROM issuer WHERE name=?", (text,)).fetchone()
    if row:
        return row[0]
    # 匹配 former_names（逗号分隔）
    for rid, fn in conn.execute("SELECT id, former_names FROM issuer WHERE former_names IS NOT NULL"):
        if fn and text in [x.strip() for x in fn.split(",")]:
            return rid
    # 自动新建（org_type 待人工补）
    cur = conn.execute("INSERT INTO issuer(name, status, sort_order) VALUES(?, '现行', 99)", (text,))
    print(f"        [issuer] 自动新建：{text}（org_type 待人工补）", flush=True)
    return cur.lastrowid


def resolve_hierarchy_id(conn, text):
    """hierarchy 文本 → hierarchy.id（二级叶子）；找不到返回 None。"""
    text = (text or "").strip()
    if not text:
        return None
    leaf = HIER_TEXT_TO_LEAF.get(text, text)
    row = conn.execute("SELECT id FROM hierarchy WHERE name=? AND level=2", (leaf,)).fetchone()
    if row:
        return row[0]
    row = conn.execute("SELECT id FROM hierarchy WHERE name=?", (text,)).fetchone()
    return row[0] if row else None


def migrate_data(conn) -> None:
    """迁移现有 regulation 数据。"""
    regs = conn.execute("SELECT id, issuer, hierarchy FROM regulation").fetchall()
    hier_ok = iss_ok = 0
    for rid, issuer_txt, hier_txt in regs:
        # hierarchy → hierarchy_id
        if hier_txt:
            hid = resolve_hierarchy_id(conn, hier_txt)
            if hid:
                conn.execute("UPDATE regulation SET hierarchy_id=? WHERE id=?", (hid, rid))
                hier_ok += 1
        # issuer → regulation_issuer
        if issuer_txt:
            iid = resolve_issuer_id(conn, issuer_txt)
            if iid:
                conn.execute("INSERT OR IGNORE INTO regulation_issuer(reg_id,issuer_id,is_primary) "
                             "VALUES(?,?,1)", (rid, iid))
                iss_ok += 1
    print(f"        [迁移] hierarchy_id 回填 {hier_ok} 条，regulation_issuer 建链 {iss_ok} 条", flush=True)


def run_schema(dry: bool = False) -> dict:
    print("[1/10] schema 迁移（v1 → v2）…", flush=True)
    if not DB.exists():
        step_skip("schema 迁移", f"未找到 {DB}")
        return {"step": "schema", "skipped": True}
    if dry:
        print("      [dry-run] 不备份、不改库", flush=True)
        return {"step": "schema", "dry": True}

    backup_db()
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # 1) regulation 加列
    added = []
    added += [add_column(conn, "regulation", "source_id", "source_id TEXT")]
    added += [add_column(conn, "regulation", "content_hash", "content_hash TEXT")]
    added += [add_column(conn, "regulation", "scope_sub", "scope_sub TEXT")]
    added += [add_column(conn, "regulation", "hierarchy_id", "hierarchy_id INTEGER REFERENCES hierarchy(id)")]

    # 2) reg_relation 加列（表可能已存在）
    has_rel = cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reg_relation'").fetchone()
    if has_rel:
        added += [add_column(conn, "reg_relation", "clause_reg", "clause_reg TEXT")]
        added += [add_column(conn, "reg_relation", "clause_related", "clause_related TEXT")]
        added += [add_column(conn, "reg_relation", "source", "source TEXT")]
        added += [add_column(conn, "reg_relation", "confidence", "confidence REAL")]

    # 3) 新建表 + 种子
    create_hierarchy(conn)
    create_issuer(conn)
    create_reg_issuer(conn)

    # 4) category 补 176 融资担保
    #    ⚠️ 幂等守卫：后续「分类体系迁移」会把「融资担保」并入「地方金融」。
    #       若已并入过（地方金融已存在），就不能再补回来，否则两步会互相打架：
    #       一补一删，每次重跑都重复往返。仅当「地方金融」尚未建立（首次迁移）时才补。
    if cur.execute("SELECT 1 FROM category WHERE name='地方金融'").fetchone():
        pass  # 已并入地方金融，无需补
    elif not cur.execute("SELECT 1 FROM category WHERE id=176").fetchone():
        cur.execute("INSERT OR IGNORE INTO category(id,name,parent_id,sort_order) VALUES(176,'融资担保',11,0)")
        print("        [category] 补入 融资担保(176)", flush=True)

    conn.commit()

    # 5) 迁移数据
    migrate_data(conn)

    # 6) 补索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reg_source_id ON regulation(source_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reg_hid ON regulation(hierarchy_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reg_issuer ON regulation_issuer(issuer_id)")
    conn.commit()

    n_new = sum(1 for a in added if a)
    print(f"        [列变更] regulation/reg_relation 本次新增 {n_new} 列", flush=True)
    conn.close()
    return {"step": "schema", "new_cols": n_new}


# ============================================================================
# 步骤 2：分类体系迁移 v2
# ============================================================================

# 「央行专项」组的 7 个专题（央行特有业务条线，法询体系里没有）
CENTRAL_BANK = [
    ("支付结算",     "支付机构、清算、账户管理、票据交换、电子支付"),
    ("征信管理",     "征信业、信用评级、信用信息基础数据库"),
    ("货币金银",     "人民币发行、残缺污损币、反假币、纪念币、现金管理"),
    ("国库管理",     "国库经收、代理银行、集中收付"),
    ("银行卡与清算", "银行卡业务、清算机构、卡组织"),
    ("存款保险",     "存款保险条例、保费、风险处置"),
    ("金融基础设施", "登记托管、清算结算系统、重要支付系统"),
]

# 专题排序（按法询截图顺序）
CAT_ORDER = {
    "大资管": ["信托", "理财", "保险资管", "私募基金", "大资管综合"],
    "内控合规": ["反洗钱", "内审规定", "消费者保护", "案件防控", "数据与个保法"],
    "金融市场": ["票据法规", "资金业务", "债券市场", "地方金融"],
    "银行业务": ["财富管理", "不良资产", "专精特新", "房地产", "小微贷款和普惠金融",
               "互联网贷款", "银行业综合监管"],
    "证券期货": ["全面注册制", "证券基金监管", "期货", "投行业务", "REITS"],
    "国际业务": ["外汇跨境", "跨境人民币", "QFLP政策", "利用外资", "ODI", "QDLP"],
    "其他综合": ["海南自贸政策", "民法典", "金融标准", "ESG", "劳动人事",
               "贸易融资与供应链金融"],
    "政府投融资": ["土地制度", "地方债", "国企合规", "绿色融资标准", "政府采购", "城市更新"],
    "科创产业": ["人工智能", "民营小微"],
    "财税会计": ["税务合规", "会计准则", "外贸外资税收"],
    "保险": ["保险业常用法规"],
}


def run_category_migration(dry: bool = False) -> dict:
    print("[2/10] 分类体系迁移（v2）…", flush=True)
    if not DB.exists():
        step_skip("分类迁移", f"未找到 {DB}")
        return {"step": "category", "skipped": True}

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # ---------- 1. reg_category 加 is_primary ----------
    cols = [r[1] for r in cur.execute("PRAGMA table_info(reg_category)")]
    if "is_primary" not in cols:
        print("        [1] reg_category 添加 is_primary 列", flush=True)
        if not dry:
            cur.execute("ALTER TABLE reg_category ADD COLUMN is_primary INTEGER DEFAULT 0")
            con.commit()
    else:
        print("        [1] is_primary 已存在，跳过", flush=True)

    # ---------- 2. 新增「央行专项」组 ----------
    row = cur.execute("SELECT id FROM category WHERE name='央行专项'").fetchone()
    if row:
        gid = row[0]
        print(f"        [2] 「央行专项」组已存在 id={gid}", flush=True)
    else:
        mx = cur.execute("SELECT COALESCE(MAX(sort_order),0) FROM category WHERE parent_id IS NULL").fetchone()[0]
        print(f"        [2] 新建「央行专项」组 (sort_order={mx+10})", flush=True)
        if not dry:
            cur.execute("INSERT INTO category (name, parent_id, sort_order) VALUES (?,?,?)",
                        ("央行专项", None, mx + 10))
            gid = cur.lastrowid
            con.commit()
        else:
            gid = -1

    # ---------- 3. 新增 7 个专题 ----------
    added = 0
    for i, (name, _desc) in enumerate(CENTRAL_BANK, 1):
        ex = cur.execute("SELECT id FROM category WHERE name=?", (name,)).fetchone()
        if ex:
            continue
        added += 1
        if not dry:
            cur.execute("INSERT INTO category (name, parent_id, sort_order) VALUES (?,?,?)",
                        (name, gid, i * 10))
    if not dry:
        con.commit()
    print(f"        [3] 央行专项下新增 {added} 个专题（共 {len(CENTRAL_BANK)} 个）", flush=True)

    # ---------- 4. 融资担保 → 地方金融 ----------
    old = cur.execute("SELECT id FROM category WHERE name='融资担保'").fetchone()
    new = cur.execute("SELECT id FROM category WHERE name='地方金融'").fetchone()
    if old and new:
        oid, nid = old[0], new[0]
        n = cur.execute("SELECT COUNT(*) FROM reg_category WHERE cat_id=?", (oid,)).fetchone()[0]
        print(f"        [4] 「融资担保」(id={oid}) 有 {n} 条关联，并入「地方金融」(id={nid})", flush=True)
        if not dry:
            # 先删掉「既挂 176 又挂 114」的重复记录，避免主键冲突
            cur.execute("""DELETE FROM reg_category WHERE cat_id=?
                           AND reg_id IN (SELECT reg_id FROM reg_category WHERE cat_id=?)""",
                        (oid, nid))
            cur.execute("UPDATE reg_category SET cat_id=? WHERE cat_id=?", (nid, oid))
            cur.execute("DELETE FROM category WHERE id=?", (oid,))
            con.commit()
    else:
        print("        [4] 融资担保 / 地方金融 不齐，跳过", flush=True)

    # ---------- 5. 专题排序 ----------
    if not dry:
        for gname, kids in CAT_ORDER.items():
            g = cur.execute("SELECT id FROM category WHERE name=?", (gname,)).fetchone()
            if not g:
                continue
            for i, k in enumerate(kids, 1):
                cur.execute("UPDATE category SET sort_order=? WHERE name=? AND parent_id=?",
                            (i * 10, k, g[0]))
        con.commit()
        print("        [5] 专题排序已更新", flush=True)

    # ---------- 输出概览 ----------
    tot_g = tot_s = 0
    for pid, _pname in cur.execute(
            "SELECT id,name FROM category WHERE parent_id IS NULL ORDER BY sort_order,id").fetchall():
        kids = cur.execute("SELECT COUNT(*) FROM category WHERE parent_id=?", (pid,)).fetchone()[0]
        tot_g += 1
        tot_s += kids
    print(f"        分类表：{tot_g} 组 + {tot_s} 专题 = {tot_g + tot_s} 节点", flush=True)
    con.close()
    return {"step": "category", "groups": tot_g, "topics": tot_s}


# ============================================================================
# 步骤 3：主入库（法规入库_v2 逻辑）
# ============================================================================

# ---------------- 标题清洗与判重 ----------------
ORG_PAT = (r"^(中国银行业监督管理委员会|中国银监会|中国保险监督管理委员会|中国保监会|"
           r"中国银行保险监督管理委员会|中国银保监会|中国人民银行|国家金融监督管理总局|"
           r"银监会|保监会|银保监会|人民银行|金融监管总局|国务院办公厅|国务院|"
           r"全国人民代表大会常务委员会|财政部|国家外汇管理局|中国证券监督管理委员会|中国证监会)")
OFFICE = r"(办公厅|办公室|秘书局|公告)?"


def clean_title(t: str) -> str:
    """入库用标题：去换行、压缩空白"""
    if not t:
        return ""
    t = t.replace("\u3000", " ").replace("&nbsp;", " ")
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def norm_title(t: str) -> str:
    """判重归一化：删空白/书名号/括号内容/文号/机构名前缀"""
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


# ---------------- 文号提取 ----------------
DOCNO_PAT = re.compile(
    r"((?:[国发银金财证保汇发改办]|银监|银保监|金融监管总局|中国人民银行)+"
    r"[〔\[(（]\s*\d{4}\s*[〕\])）]\s*第?\s*\d+\s*号"
    r"|(?:令|公告)\s*[〔\[(（]?\s*\d{4}\s*[〕\])）]?\s*第?\s*\d+\s*号"
    r"|(?:主席令|国务院令)\s*第?\s*[\d一二三四五六七八九十百]+号)"
)


def extract_doc_no(title: str) -> str:
    if not title:
        return ""
    m = DOCNO_PAT.search(title)
    return m.group(1) if m else ""


# ---------------- 文号前缀 → 机构 ----------------
DOCNO_ISSUER = [
    (r"银发|银办发|银条法|银函", "中国人民银行"),
    (r"金规|金监发|金办发|金发|金融监管总局令", "国家金融监督管理总局"),
    (r"银保监规|银保监发|银保监办发", "中国银行保险监督管理委员会"),
    (r"银监发|银监办发|银监办通|银监会令", "中国银行业监督管理委员会"),
    (r"保监发|保监厅发|保监会令", "中国保险监督管理委员会"),
    (r"汇发|汇综发|汇复", "国家外汇管理局"),
    (r"证监发|证监会令|证监公告", "中国证券监督管理委员会"),
    (r"国务院令|国发|国办发|国办函", "国务院"),
    (r"主席令", "全国人民代表大会常务委员会"),
    (r"发改|国家发展改革委", "国家发展和改革委员会"),
    (r"财政部令|财税|财会", "财政部"),
    (r"税务总局公告|税总", "国家税务总局"),
    (r"司法部令", "司法部"),
]

# ---------------- 分类关键词（主入库时的初判，后续由重跑分类覆盖）----------------
CATEGORY_KEYWORDS = {
    "反洗钱": (100, ["反洗钱", "洗钱", "恐怖融资", "可疑交易", "客户身份识别", "受益所有人"]),
    "消费者保护": (100, ["消费者权益", "金融消费者", "适当性管理", "个人信息保护", "营销宣传"]),
    "数据与个保法": (100, ["数据安全", "个人信息保护", "数据治理", "外包风险"]),
    "外汇跨境": (90, ["外汇", "结售汇", "跨境", "经常项目", "资本项目"]),
    "理财": (90, ["理财", "资管产品", "代销", "净值型"]),
    "互联网贷款": (80, ["互联网贷款", "网络小额贷款", "助贷"]),
    "不良资产": (80, ["不良资产", "资产处置", "呆账核销"]),
    "房地产": (80, ["房地产", "住房贷款", "房地产开发贷款", "首付款", "商品房"]),
    "小微贷款和普惠金融": (80, ["普惠金融", "小微企业", "个体工商户", "创业担保", "支农", "支小"]),
    "案件防控": (80, ["案件防控", "涉刑案件", "案防", "非法集资", "从业人员禁止"]),
    "内审规定": (70, ["内部审计", "内控评价", "内部控制"]),
    "财富管理": (70, ["财富管理", "私人银行"]),
    "资金业务": (70, ["同业业务", "同业存单", "大额存单", "回购", "拆借"]),
    "票据法规": (70, ["票据", "商业汇票", "汇票", "本票", "支票"]),
    "债券市场": (70, ["债券", "债务融资工具", "银行间市场"]),
    "跨境人民币": (70, ["跨境人民币", "人民币国际化", "境外机构投资者", "合格境外"]),
    "信托": (70, ["信托"]),
    "金融标准": (60, ["金融标准", "行业标准", "推荐性标准"]),
    "民营小微": (60, ["民营经济", "民营企业"]),
    "保险业常用法规": (60, ["保险", "保险公司", "保险资金", "偿付能力"]),
    "银行业综合监管": (50, ["商业银行", "银行业", "银行监管", "资本管理", "风险管理", "授信"]),
    "证券基金监管": (50, ["证券公司", "证券投资基金", "基金托管", "证券期货"]),
    "地方金融": (50, ["地方金融", "小额贷款公司", "融资担保", "典当", "融资租赁"]),
    "会计准则": (50, ["会计准则", "会计处理", "财务报告"]),
    "税收合规": (50, ["税务", "税收", "增值税"]),
    "民法典": (40, ["民法典"]),
}


def classify(title: str, content: str):
    blob = (title or "") + " " + (content or "")[:1500]
    hits = []
    for cat, (w, kws) in CATEGORY_KEYWORDS.items():
        if any(kw in blob for kw in kws):
            hits.append((w, cat))
    hits.sort(reverse=True)
    if not hits:
        return None, []
    return CAT_ID.get(hits[0][1]), [CAT_ID.get(c) for _, c in hits[1:] if CAT_ID.get(c)]


# ---------------- 层级判定 ----------------
def judge_hierarchy(title: str, src_col: str, meta: dict):
    """返回 (hierarchy_id, 层级名)"""
    t = title or ""
    if "主席令" in t or t.startswith("中华人民共和国") and "法" in t[:12]:
        if "法" in t and "办法" not in t:
            return H_ID.get("法律"), "法律"
    if "国务院令" in t:
        return H_ID.get("行政法规"), "行政法规"
    if re.search(r"令\s*[〔\[(（]?\s*\d{4}", t) or "令20" in t:
        return H_ID.get("部门规章"), "部门规章"
    if src_col == "行政法规":
        return H_ID.get("行政法规"), "行政法规"
    if src_col == "部门规章":
        return H_ID.get("部门规章"), "部门规章"
    if src_col == "法律法规":
        # 金监局"法律法规"栏目里混着人大法律和国务院行政法规
        if "中华人民共和国" in t and "条例" not in t:
            return H_ID.get("法律"), "法律"
        return H_ID.get("行政法规"), "行政法规"
    # 政策规章规范性文件 / 规范性文件 → 部门规章下的规范性文件及通知
    return H_ID.get("规范性文件及通知"), "规范性文件及通知"


# ---------------- 机构识别 ----------------
# 已知部委/机构词表（只认这些，避免把「商业银行」「银监会办公厅」当机构）
KNOWN_MINISTRIES = {
    "海关总署", "国家市场监督管理总局", "市场监管总局", "教育部", "科技部",
    "工业和信息化部", "公安部", "民政部", "司法部", "人力资源和社会保障部",
    "自然资源部", "生态环境部", "住房和城乡建设部", "住房城乡建设部",
    "交通运输部", "水利部", "农业农村部", "商务部", "文化和旅游部",
    "应急管理部", "审计署", "国家统计局", "国家知识产权局",
    "国家互联网信息办公室", "国家网信办", "国家卫生健康委员会",
    "国务院国有资产监督管理委员会", "国家能源局", "国家数据局",
    "中国银行保险监督管理委员会", "中国证券监督管理委员会",
    "国家外汇管理局", "国家发展和改革委员会", "财政部", "国家税务总局",
    "最高人民法院", "最高人民检察院", "国家金融监督管理总局",
    "中国人民银行", "国家开发银行", "中国农业发展银行", "中国进出口银行",
}


def extract_org_names(title: str):
    """从标题前段切出机构名（只认词表，联合发文用）"""
    t = re.sub(r"[（(][^）)]*[）)]", "", title or "")
    t = re.split(r"公告|令|通知|关于|意见|办法|决定", t)[0]
    return [p for p in (x.strip() for x in re.split(r"[\s、,，]+", t)) if p in KNOWN_MINISTRIES]


def ensure_issuer(con, name: str):
    """机构不在表里就自动登记（联合发文常见）"""
    if name in I_ID:
        return I_ID[name]
    if name in _org_cache:
        return _org_cache[name]
    try:
        cur = con.execute(
            "INSERT INTO issuer (name, org_type, admin_level, status, sort_order) VALUES (?,?,?,?,?)",
            (name, "部委", "部委级", "现行", 900))
        iid = cur.lastrowid
        I_ID[name] = iid
        ISSUER_ALIAS[iid] = [name]
        _org_cache[name] = iid
        return iid
    except Exception:
        return None


def detect_issuers(title: str, org: str, con=None):
    """返回 [(issuer_id, is_primary)]"""
    t = title or ""
    out = []
    # 1) 文号前缀（最可靠）
    dn = extract_doc_no(t)
    if dn:
        for pat, name in DOCNO_ISSUER:
            if re.search(pat, dn):
                iid = I_ID.get(name)
                if iid:
                    out.append((iid, 1))
                break
    # 2) 标题里的机构名（联合发文）
    for iid, names in ISSUER_ALIAS.items():
        if any(n and n in t for n in names):
            if not any(iid == x for x, _ in out):
                out.append((iid, 0))
    # 3) 标题里未登记的机构（如「海关总署」「市场监管总局」）→ 自动补进 issuer 表
    if con is not None:
        for name in extract_org_names(t):
            if any(name in ns for ns in ISSUER_ALIAS.values()):
                continue
            iid = ensure_issuer(con, name)
            if iid and not any(iid == x for x, _ in out):
                out.append((iid, 0))
    # 4) 兜底：来源机构
    if not out:
        fallback = "中国人民银行" if org == "人民银行" else "国家金融监督管理总局"
        iid = I_ID.get(fallback)
        if iid:
            out.append((iid, 1))
    # 5) 排序：牵头机关优先，其余按在标题中出现的先后
    def _pos(iid):
        ps = [t.find(n) for n in ISSUER_ALIAS.get(iid, []) if n and t.find(n) >= 0]
        return min(ps) if ps else 9999
    out.sort(key=lambda x: (0 if x[1] else 1, _pos(x[0])))
    # 6) 若一个牵头机关都没有，把排最前的设为牵头（避免 is_primary 全空）
    if out and not any(p for _, p in out):
        out[0] = (out[0][0], 1)
    return out[:12]


# 机构常见简称（标题里出现这些也要能识别出来）
EXTRA_ALIAS = {
    "国家金融监督管理总局": ["金融监管总局", "金监总局"],
    "中国人民银行": ["人行"],
    "中国银行保险监督管理委员会": ["银保监会"],
    "中国银行业监督管理委员会": ["银监会"],
    "中国保险监督管理委员会": ["保监会"],
    "中国证券监督管理委员会": ["证监会"],
    "国家外汇管理局": ["外汇局"],
    "国家发展和改革委员会": ["发展改革委", "发改委"],
    "国家税务总局": ["税务总局"],
    "国务院办公厅": ["国办"],
}


def load_dicts(con) -> None:
    global H_ID, I_ID, ISSUER_ALIAS, CAT_ID
    for i, n in con.execute("SELECT id, name FROM hierarchy"):
        H_ID[n] = i
    for i, n, _fn in con.execute("SELECT id, name, COALESCE(former_names,'') FROM issuer"):
        I_ID[n] = i
        # ⚠️ 机构识别**不使用 former_names**（那是给检索做同义词展开用的）。
        #    否则标题里的「中国银监会」会同时算到「国家金融监督管理总局」和
        #    「中国银行业监督管理委员会」头上，一条文件挂两个机构。
        alias = [n] + EXTRA_ALIAS.get(n, [])
        ISSUER_ALIAS[i] = [a for a in dict.fromkeys(alias) if a]
    for i, n in con.execute("SELECT id, name FROM category"):
        CAT_ID[n] = i


def run_regs(tu: Path, dry: bool = False, verbose: bool = False) -> dict:
    print("[3/10] 主入库（灌 regulation）…", flush=True)
    if not tu.is_dir():
        step_skip("主入库", f"目录不存在: {tu}")
        return {"step": "regs", "skipped": True}

    excl = set()
    ep = tu / "_排除清单.csv"
    if ep.exists():
        for r in csv.DictReader(open(ep, encoding="utf-8-sig")):
            # ⚠️ 必须用 (机构, source_id) 组合键：
            #    iweicha 的 file_id 在人行/金监局之间不唯一，单用 source_id 会误伤
            excl.add((r["org"], r["source_id"]))
    print(f"        排除清单: {len(excl)} 条", flush=True)

    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys=ON")
    load_dicts(con)

    # 已有记录（判重）
    #   三档键：① 标题归一化  ② 文号  ③ source_id（来源唯一 ID）
    #   ⚠️ source_id 这一档是必需的：像「中国人民银行 公告」「××公告（2013年第21号）」
    #      这类标题归一化后为空/过短的条目，标题与文号都拦不住，只能靠 source_id；
    #      否则每次重跑都会把它们重复灌一遍，破坏幂等。
    existing = {}
    for rid, ti, dn, sid in con.execute(
            "SELECT id, title, COALESCE(doc_number,''), COALESCE(source_id,'') FROM regulation"):
        existing[norm_title(ti)] = rid
        if dn:
            existing["#NO#" + dn] = rid
        if sid:
            existing["#SID#" + sid] = rid
    print(f"        库内现有: {len(existing)} 键", flush=True)

    today = date.today().isoformat()
    stat = {"total": 0, "skip_excl": 0, "skip_dupe": 0, "ins": 0, "issuer_link": 0, "cat_link": 0}
    hier_cnt, issuer_cnt, nohier = {}, {}, 0

    for org_dir in sorted(tu.iterdir()):
        if not org_dir.is_dir():
            continue
        org = org_dir.name
        idxf = org_dir / "_index.csv"
        if not idxf.exists():
            step_skip(f"主入库·{org}", "无 _index.csv")
            continue
        idx = list(csv.DictReader(open(idxf, encoding="utf-8-sig")))
        print(f"        === {org}  {len(idx)} 条 ===", flush=True)
        for r in idx:
            stat["total"] += 1
            sid = r["source_id"]
            if (org, sid) in excl:
                stat["skip_excl"] += 1
                continue
            # 唯一 source_id：iweicha 的 file_id 跨机构会撞号，加机构后缀区分
            org_short = "pbc" if org == "人民银行" else "nfra"
            uniq_sid = sid.replace("iweicha:", f"iweicha-{org_short}:", 1) \
                if sid.startswith("iweicha:") else sid
            d = org_dir / r["dir_name"]
            ct = d / "content.txt"
            content = ct.read_text(encoding="utf-8", errors="replace") if ct.exists() else ""
            meta = {}
            mf = d / "_meta.json"
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text(encoding="utf-8"))
                except Exception:
                    pass

            title = clean_title(r["title"])
            doc_no = meta.get("doc_number") or extract_doc_no(title)
            nt = norm_title(title)
            # 标题归一化后过短（如「令」「公告〔2026〕第4号」）无法互相区分，
            # 这类只靠文号判重，否则会把一批不同文件误判成重复
            title_usable = len(nt) >= 6

            # 三档判重：标题 / 文号 / source_id
            dup_why = ""
            if title_usable and nt in existing:
                dup_why = "标题"
            elif doc_no and "#NO#" + doc_no in existing:
                dup_why = f"文号[{doc_no}]"
            elif "#SID#" + uniq_sid in existing:
                dup_why = f"来源[{uniq_sid}]"
            if dup_why:
                stat["skip_dupe"] += 1
                if verbose and stat["skip_dupe"] <= 30:
                    print(f"          跳过({dup_why}) [{org}] {title[:50]}", flush=True)
                continue

            hid, hname = judge_hierarchy(title, r.get("source_sys", ""), meta)
            if not hid:
                nohier += 1
            hier_cnt[hname] = hier_cnt.get(hname, 0) + 1
            issuers = detect_issuers(title, org, None if dry else con)
            for iid, _ in issuers:
                issuer_cnt[iid] = issuer_cnt.get(iid, 0) + 1
                break

            cid, cids = classify(title, content)
            h = hashlib.md5((content or "").encode("utf-8")).hexdigest()

            if dry:
                if stat["total"] <= 6:
                    names = [k for k, v in I_ID.items() if v in [i for i, _ in issuers]]
                    print(f"          [{hname}] {title[:44]}", flush=True)
                    print(f"                文号={doc_no or '-'} 机构={names} 分类={cid}", flush=True)
                stat["ins"] += 1
                if title_usable:
                    existing[nt] = -1
                if doc_no:
                    existing["#NO#" + doc_no] = -1
                existing["#SID#" + uniq_sid] = -1
                continue

            cur = con.execute(
                """INSERT INTO regulation
                   (title, doc_number, issuer, hierarchy, scope, status,
                    pub_date, effective_date, content_text, content_path, source_url,
                    crawl_date, remark, source_id, content_hash, hierarchy_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (title, doc_no,
                 "、".join([k for k, v in I_ID.items() if v in [i for i, _ in issuers]][:1]) or
                 ("中国人民银行" if org == "人民银行" else "国家金融监督管理总局"),
                 hname, "全国性", meta.get("status", ""),
                 meta.get("pub_date", ""), "", content,
                 str(d), r.get("original_url", "") or r.get("detail_url", ""),
                 today, "", uniq_sid, h, hid))
            rid = cur.lastrowid
            for iid, prim in issuers:
                try:
                    con.execute("INSERT OR IGNORE INTO regulation_issuer (reg_id, issuer_id, is_primary) VALUES (?,?,?)",
                                (rid, iid, prim))
                    stat["issuer_link"] += 1
                except Exception:
                    pass
            if cid:
                con.execute("INSERT OR IGNORE INTO reg_category (reg_id, cat_id) VALUES (?,?)", (rid, cid))
                stat["cat_link"] += 1
            for c in cids:
                con.execute("INSERT OR IGNORE INTO reg_category (reg_id, cat_id) VALUES (?,?)", (rid, c))
                stat["cat_link"] += 1
            if title_usable:
                existing[nt] = rid
            if doc_no:
                existing["#NO#" + doc_no] = rid
            existing["#SID#" + uniq_sid] = rid
            stat["ins"] += 1

    if not dry:
        print("        重建全文索引…", flush=True)
        try:
            con.execute("INSERT INTO reg_fts(reg_fts) VALUES('rebuild')")
        except Exception as e:
            print("          rebuild 失败:", e, flush=True)
        con.commit()

    print(f"        总数 {stat['total']} | 排除跳过 {stat['skip_excl']} | "
          f"判重跳过 {stat['skip_dupe']} | 新入库 {stat['ins']}", flush=True)
    print(f"        机构关联 {stat['issuer_link']} | 分类关联 {stat['cat_link']} | 无层级 {nohier}", flush=True)
    print(f"        层级分布 : {hier_cnt}", flush=True)
    con.close()
    return {"step": "regs", **stat, "nohier": nohier}


# ============================================================================
# 步骤 4：补早期18条（source_id / scope / content_hash）
# ============================================================================

DOCID = re.compile(r"[?&]docId=(\d+)")


def run_early18(dry: bool = False) -> dict:
    print("[4/10] 补早期条目缺失字段（source_id/scope/content_hash）…", flush=True)
    con = sqlite3.connect(DB)
    cur = con.cursor()

    rows = cur.execute(
        """SELECT id, title, hierarchy, source_url, COALESCE(source_id,''), COALESCE(content_text,'')
           FROM regulation WHERE COALESCE(source_id,'')='' ORDER BY id"""
    ).fetchall()
    print(f"        待补条目：{len(rows)} 条", flush=True)

    # 先查重：要写入的 source_id 是否已存在（避免撞号）
    existing = {r[0] for r in cur.execute(
        "SELECT source_id FROM regulation WHERE COALESCE(source_id,'')!=''")}

    updates, skipped = [], []
    for rid, title, hier, url, _sid, ct in rows:
        m = DOCID.search(url or "")
        if not m:
            skipped.append((rid, title, "source_url 里没有 docId"))
            continue
        sid = f"nfra:{m.group(1)}"
        if sid in existing:
            skipped.append((rid, title, f"{sid} 与库内已有条目撞号"))
            continue
        existing.add(sid)

        scope = "全国性"
        chash = hashlib.md5((ct or "").encode("utf-8")).hexdigest()
        updates.append((sid, scope, chash, rid, title, hier))

    for sid, scope, chash, rid, title, _hier in updates:
        print(f"          id={rid:<4} {title[:44]}", flush=True)
        print(f"                source_id={sid}  scope={scope}  hash={chash[:16]}…", flush=True)
    for rid, title, why in skipped:
        print(f"          [跳过] id={rid:<4} {title[:40]} —— {why}", flush=True)

    if dry:
        print(f"        [dry-run] 未写库（将补 {len(updates)} 条）", flush=True)
        con.close()
        return {"step": "early18", "updates": len(updates), "skipped": len(skipped), "dry": True}

    cur.executemany(
        "UPDATE regulation SET source_id=?, scope=?, content_hash=? WHERE id=?",
        [(u[0], u[1], u[2], u[3]) for u in updates])
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM regulation WHERE COALESCE(source_id,'')=''").fetchone()[0]
    print(f"        已补写 {len(updates)} 条；复核 source_id 仍空 {n} 条", flush=True)
    con.close()
    return {"step": "early18", "updates": len(updates), "skipped": len(skipped)}


# ============================================================================
# 步骤 5：补文号（从标题抽 doc_number）
# ============================================================================

# 文号核心（在规范化后的标题上匹配）：
#   ① 标准形态 〔YYYY〕第?N号 / 字第N号（年份 2 位或 4 位）
#   ② 机关名紧跟括号后：〔YYYY〕银发字第N号（年份前置倒装，如 [84]银发字第1号）
_NUM = r"[0-9零一二三四五六七八九十百]+"
CORE = (r"[〔\[]\s*(?:[0-9]{2}|[0-9]{4})\s*[〕\]]\s*(?:第?\s*" + _NUM + r"\s*号"
        r"|[A-Za-z\u4e00-\u9fa5]{0,10}字\s*第?\s*" + _NUM + r"\s*号)")

# 已知文号前缀词表（取自库内已有 doc_number 的实际分布 + 常见机关简称）
# 说明：紧贴型标题（「……的通知金规〔2025〕25号」）无法靠"向左取字符"区分前缀与标题正文，
#       必须用白名单匹配，避免把「的通知」这类标题文字吃进文号。
PREFIX_WORDS = [
    # 银保监 / 银监 / 人行（含「字」「便函」「通」「复」等变体）
    "银保监办发", "银保监发", "银保监规", "银监办发", "银监发", "银监规",
    "银办发", "银发", "银监通", "银保监通", "银监办通", "银监复", "银监批",
    "银监办便函", "银保监办便函", "银办便函", "银监便函",
    "银发字", "银监发字", "银办字",
    "银支付", "银货政二", "银货政", "银办函", "银监函", "银保监办函", "银保监函",
    "银管", "银市", "银调", "银科",
    # 金监总局（金规/金办发/金发/金办便函）
    "金规", "金办发", "金办函", "金发", "金监发", "金监办发", "金监函",
    "金办便函", "金监办便函", "金便函", "金监规",
    # 财政部 / 发改（含「财办会」等办公厅文号）
    "财会", "财库", "财金", "财预", "财办会", "财办库", "财办金", "财办", "财税",
    "发改财金", "发改办财金", "发改",
    # 其他部委（含国办函/司发/法发等跨部门文号）
    "国信办通字", "国办函", "国办发", "司发通", "司发", "法发", "司法通",
    "建房规", "建房", "建办", "保监发", "保监办发", "证监发", "证监办发",
    "国税发", "海关总署", "汇发", "外汇局发", "知识产", "国知办发",
    # 通用
    "公告", "令", "办发", "发",
]
# 按长度降序，保证优先匹配最长的前缀（如「银保监办发」优先于「银保监」）
PREFIX_WORDS.sort(key=len, reverse=True)


def _normalize_docno(title: str) -> str:
    """标题规范化：统一括号、修括号错位，便于后续抽取"""
    t = re.sub(r"\s+", "", title or "")
    # 混用括号统一：〔2017］/ [2017〕 → 〔2017〕
    t = t.replace("[", "〔").replace("]", "〕").replace("［", "〔").replace("］", "〕")
    # 括号错位：「（金办发〔2026〕67）号」→「（金办发〔2026〕67号）」
    t = re.sub(r"([（(][^（()）]*?)[）)](\s*号)", r"\1\2", t)
    return t


def extract_doc_number(title: str):
    """从标题抽取文号；抽不到返回 None

    策略：以最靠右的文号核心为锚点，然后只在其左侧紧邻处匹配白名单前缀词。
          匹配不到就不加前缀（宁可只存核心，也不塞标题文字）。
    """
    t = _normalize_docno(title)
    matches = list(re.finditer(CORE, t))
    if not matches:
        return None
    m = matches[-1]                      # 取最靠右的一个（标题尾部通常是真文号）
    core = m.group(0)
    left = t[: m.start()]
    # 去掉左侧可能的分隔符/括号，便于紧邻匹配前缀
    left = re.sub(r"[（(【\[]+$", "", left)
    # 白名单前缀匹配（词表已按长度降序，最长优先）
    for w in PREFIX_WORDS:
        if left.endswith(w):
            return w + core
    return core


def run_docnumber(dry: bool = False) -> dict:
    print("[5/10] 补文号（从标题抽 doc_number）…", flush=True)
    con = sqlite3.connect(DB)
    cur = con.cursor()

    rows = cur.execute(
        """SELECT id, title, doc_number FROM regulation
            WHERE (doc_number IS NULL OR TRIM(doc_number)='')
              AND (title LIKE '%〔%号%' OR title LIKE '%[%]%号%')"""
    ).fetchall()

    todo = []
    for r in rows:
        dn = extract_doc_number(r[1])
        if dn:
            todo.append((dn, r[0], r[1]))

    print(f"        字段为空的法规：{len(rows)} 条；可抽取到文号：{len(todo)} 条", flush=True)
    for dn, rid, title in todo[:20]:
        print(f"          [{rid}] {dn:22s} ← {title[:56]}", flush=True)
    if len(todo) > 20:
        print(f"          …… 另有 {len(todo) - 20} 条", flush=True)

    if dry:
        print("        [dry-run] 未写库", flush=True)
        con.close()
        return {"step": "docnumber", "rows": len(rows), "updates": len(todo), "dry": True}

    for dn, rid, _ in todo:
        cur.execute("UPDATE regulation SET doc_number = ? WHERE id = ?", (dn, rid))
    con.commit()
    after = cur.execute(
        "SELECT COUNT(*) FROM regulation WHERE doc_number IS NOT NULL AND TRIM(doc_number)<>''"
    ).fetchone()[0]
    tot = cur.execute("SELECT COUNT(*) FROM regulation").fetchone()[0]
    print(f"        已回填 {len(todo)} 条；doc_number 覆盖 {after}/{tot} = {after * 100 // max(tot,1)}%", flush=True)
    con.close()
    return {"step": "docnumber", "rows": len(rows), "updates": len(todo)}


# ============================================================================
# 步骤 6：补发布日期（iweicha detail.html / 正文落款）
# ============================================================================

DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# 正文末尾的落款日期（中文写法）：二○○七年十二月五日 / 二00六年十一月三日 / 2007年12月5日
CN_DIGIT = {"○": "0", "〇": "0", "０": "0", "零": "0",
            "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
            "六": "6", "七": "7", "八": "8", "九": "9"}
CN_DATE_RE = re.compile(
    r"([○〇０零一二三四五六七八九0-9]{2,4})\s*年\s*"
    r"([○〇０零一二三四五六七八九十0-9]{1,3})\s*月\s*"
    r"([○〇０零一二三四五六七八九十0-9]{1,3})\s*日")


def cn_to_int(s: str):
    s = (s or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if "十" in s:
        a, _, b = s.partition("十")
        t = 1 if a == "" else CN_DIGIT.get(a)
        o = 0 if b == "" else CN_DIGIT.get(b)
        if t is None or o is None:
            return None
        return int(t) * 10 + int(o)
    n = 0
    for ch in s:
        d = CN_DIGIT.get(ch)
        if d is None:
            return None
        n = n * 10 + int(d)
    return n


def date_from_content(text: str) -> str:
    """从正文**最末尾**的落款里解析中文日期

    ⚠️ 只取末尾 600 字：正文中间常引用别的文件的日期，
       全文倒序找会误取（如「银监发〔2015〕53号」被填成正文引用的 2022 年）
    """
    if not text:
        return ""
    tail = text[-600:]
    for m in reversed(list(CN_DATE_RE.finditer(tail))):
        y = cn_to_int(m.group(1))
        mo = cn_to_int(m.group(2))
        da = cn_to_int(m.group(3))
        if y and mo and da and 1990 <= y <= 2035 and 1 <= mo <= 12 and 1 <= da <= 31:
            return f"{y:04d}-{mo:02d}-{da:02d}"
    return ""


def doc_year(doc_no: str):
    """从文号里取年份，如 银监发〔2015〕53号 → 2015"""
    if not doc_no:
        return None
    m = re.search(r"[〔\[(（]\s*(\d{4})\s*[〕\])）]", doc_no)
    return int(m.group(1)) if m else None


def build_iw_index():
    """(机构, file_id) -> 条目目录"""
    mp = {}
    for org in ("金监局", "人行"):
        base = IW / org
        if not base.is_dir():
            continue
        for yd in base.iterdir():
            if not yd.is_dir():
                continue
            for d in yd.iterdir():
                if d.is_dir() and "_" in d.name:
                    mp[(org, d.name.split("_")[0])] = d
    return mp


def parse_pub_date(d: Path, doc_no: str = "") -> str:
    """优先 detail.html 的 ISO 日期；老模板页面没有日期行，回退到正文末尾的中文落款。
    最后用文号年份做合理性校验（发布年份应在文号年份 -1 ~ +2 之间）。"""
    dt = ""
    dh = d / "detail.html"
    if dh.exists():
        try:
            html = dh.read_text(encoding="utf-8", errors="replace")
        except Exception:
            html = ""
        for m in DATE_RE.finditer(html):
            y, mo, da = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1990 <= y <= 2035 and 1 <= mo <= 12 and 1 <= da <= 31:
                dt = f"{y:04d}-{mo:02d}-{da:02d}"
                break
    if not dt:
        ct = d / "content.txt"
        if ct.exists():
            try:
                dt = date_from_content(ct.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                dt = ""
    if dt and doc_no:
        yd = doc_year(doc_no)
        if yd:
            ydt = int(dt[:4])
            if not (yd - 1 <= ydt <= yd + 2):
                return ""
    return dt


def run_pubdate(dry: bool = False) -> dict:
    print("[6/10] 补发布日期（iweicha detail.html / 正文落款）…", flush=True)
    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute("""SELECT id, source_id, title, COALESCE(doc_number,'')
                          FROM regulation
                          WHERE (pub_date IS NULL OR pub_date='')
                            AND source_id LIKE 'iweicha%'""").fetchall()
    print(f"        待补 pub_date 的 iweicha 条目: {len(rows)}", flush=True)

    idx = build_iw_index()
    print(f"        iweicha 目录索引: {len(idx)}", flush=True)

    ok = miss_dir = miss_date = rejected = 0
    updates = []
    for rid, sid, _title, dno in rows:
        org = "金监局" if "nfra" in sid else "人行"
        fid = sid.split(":")[-1]
        d = idx.get((org, fid))
        if not d:
            miss_dir += 1
            continue
        dt = parse_pub_date(d, dno)
        if not dt:
            # 区分「页面/正文确实没日期」与「解析出来但被文号年份校验否掉」
            if parse_pub_date(d, ""):
                rejected += 1
            else:
                miss_date += 1
            continue
        updates.append((dt, rid))
        ok += 1

    print(f"        解析成功 {ok} | 找不到目录 {miss_dir} | 确实无日期 {miss_date} | "
          f"与文号年份冲突被否 {rejected}", flush=True)
    ys = Counter(u[0][:4] for u in updates)
    print("        补出日期的年份分布（Top 12）: " +
          ", ".join(f"{y}:{c}" for y, c in ys.most_common(12)), flush=True)

    if dry:
        print("        [dry-run] 未写库", flush=True)
        con.close()
        return {"step": "pubdate", "ok": ok, "dry": True}

    cur.executemany("UPDATE regulation SET pub_date=? WHERE id=?", updates)
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM regulation WHERE pub_date IS NULL OR pub_date=''").fetchone()[0]
    print(f"        已更新 {len(updates)} 条；库内 pub_date 仍为空 {n} 条", flush=True)
    con.close()
    return {"step": "pubdate", "ok": ok, "still_empty": n}


# ============================================================================
# 步骤 7：校正发布日期
# ============================================================================

CN_DIGIT2 = {"○": "0", "〇": "0", "０": "0", "零": "0",
             "O": "0", "o": "0", "0": "0",
             "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
             "六": "6", "七": "7", "八": "8", "九": "9"}
_NUM2 = r"[○〇０零Oo一二三四五六七八九0-9]"
_NUM10 = r"[○〇０零Oo一二三四五六七八九十0-9]"
SIGN_DATE_RE = re.compile(rf"({_NUM2}{{2,4}})\s*年\s*({_NUM10}{{1,3}})\s*月\s*({_NUM10}{{1,3}})\s*日")


def cn_to_int2(s):
    s = (s or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if "十" in s:
        a, _, b = s.partition("十")
        t = 1 if a == "" else CN_DIGIT2.get(a)
        o = 0 if b == "" else CN_DIGIT2.get(b)
        if t is None or o is None:
            return None
        return int(t) * 10 + int(o)
    n = 0
    for ch in s:
        d = CN_DIGIT2.get(ch)
        if d is None:
            return None
        n = n * 10 + int(d)
    return n


def sign_date(text, span=600):
    """正文末尾的落款日期。

    只排除**生效日语境**：「自…起施行/执行」这类。
    ⚠️ 不能把「转发至」「截至目前」的「至」也算进去（会误杀真实落款）。
    """
    if not text:
        return ""
    tail = text[-span:]
    hits = []
    for m in SIGN_DATE_RE.finditer(tail):
        before = tail[max(0, m.start() - 8): m.start()]
        after = tail[m.end(): m.end() + 3]
        if "自" in before or "起" in after:
            continue  # 生效日
        y, mo, da = cn_to_int2(m.group(1)), cn_to_int2(m.group(2)), cn_to_int2(m.group(3))
        if y and mo and da and 1990 <= y <= 2035 and 1 <= mo <= 12 and 1 <= da <= 31:
            hits.append(f"{y:04d}-{mo:02d}-{da:02d}")
    return hits[-1] if hits else ""


# ---- 联网核实过的权威日期（正文落款缺失或与文号年冲突时人工查证）----
# 来源：中国政府网公文 / 北大法宝 / 银保监官网原文
VERIFIED = {
    498:  ("2019-10-16", "中国人民银行令〔2019〕第3号 行长签发日（中国政府网）"),
    617:  ("2019-12-28", "人行公告〔2019〕第30号 发布日（中国政府网 2019-12-28）"),
    842:  ("2023-12-07", "正文落款＝国家金融监督管理总局 2023-12-07（标题沿用了旧文号）"),
    881:  ("2019-11-25", "银保监发〔2019〕43号 成文日期（中国政府网公文）"),
    927:  ("2015-12-29", "银监发〔2015〕53号 通知落款（原文：2015年12月29日）"),
    1225: ("2011-03-22", "银监发〔2011〕31号 成文日期（北大法宝 2011-03-22）"),
    1921: ("2011-08-01", "银监发〔2011〕85号 落款（中国政府网：二○一一年八月一日）"),
}

# ---- 源数据错配，不改，仅提示 ----
DUBIOUS_NOTE = {
    1034: "正文实为《银保监发〔2020〕17号》通知（含废止〔2019〕7号的条款），"
          "标题文号却是〔2019〕7号——属源数据错配，待人工核验",
}
SKIP_IDS = set(DUBIOUS_NOTE)


def run_fix_pubdate(dry: bool = False) -> dict:
    print("[7/10] 校正发布日期（修 pub_date 与文号年份冲突）…", flush=True)
    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute(
        """SELECT id, title, doc_number, pub_date, source_id, content_text
           FROM regulation WHERE pub_date != '' AND doc_number != ''"""
    ).fetchall()

    changes, dubious, kept, skipped = [], [], 0, 0
    for rid, title, dn, pd, _sid, ct in rows:
        yd = doc_year(dn)
        if not yd:
            continue
        py = int(pd[:4])
        if yd == py:
            continue

        # 0) 联网核实的权威日期优先
        if rid in SKIP_IDS:
            skipped += 1
            continue
        if rid in VERIFIED:
            new, why = VERIFIED[rid]
            if new != pd:
                changes.append((rid, pd, new, "联网核实", dn, title, why))
            else:
                kept += 1
            continue

        sign = sign_date(ct or "")
        sy = int(sign[:4]) if sign else None

        if sign and abs(sy - yd) <= 1:
            if sign != pd:
                changes.append((rid, pd, sign, "正文落款", dn, title, ""))
            else:
                kept += 1
        elif not sign and abs(py - yd) >= 2:
            new = f"{yd}-12-31"
            if new != pd:
                changes.append((rid, pd, new, "年份推定", dn, title, ""))
            else:
                kept += 1
        elif sign:
            dubious.append((rid, pd, sign, dn, title))
        else:
            kept += 1

    print(f"        文号年份≠发布日期 的条目: {len(changes) + len(dubious) + kept + skipped}", flush=True)
    print(f"        将修正 {len(changes)} 条 | 存疑不动 {len(dubious)} 条 | 保持不动 {kept} 条", flush=True)
    for rid, old, new, why, dn, title, _note in changes[:20]:
        tag = "" if old == new else "  ← 有变化"
        print(f"          id={rid:<5} {old} → {new} [{why}]{tag}  {dn}", flush=True)
    if len(changes) > 20:
        print(f"          …… 另有 {len(changes) - 20} 条", flush=True)

    if dry:
        print("        [dry-run] 未写库", flush=True)
        con.close()
        return {"step": "fix_pubdate", "changes": len(changes), "dubious": len(dubious), "dry": True}

    cur.executemany("UPDATE regulation SET pub_date=? WHERE id=?",
                    [(new, rid) for rid, _old, new, _w, _d, _t, _n in changes])
    con.commit()
    print(f"        已修正 {len(changes)} 条", flush=True)
    con.close()
    return {"step": "fix_pubdate", "changes": len(changes), "dubious": len(dubious)}


# ============================================================================
# 步骤 8：补生效日期
# ============================================================================

TAIL = 2000

CN_DIGIT3 = {"○": "0", "〇": "0", "０": "0", "零": "0", "O": "0", "o": "0",
             "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
             "六": "6", "七": "7", "八": "8", "九": "9"}
_CN3 = r"[○〇０零Oo一二三四五六七八九0-9]"
_N10_3 = r"[○〇０零Oo一二三四五六七八九十0-9]"

# ① 具体日期 + 施行
RE_SELF = re.compile(
    rf"自\s*({_CN3}{{2,4}})\s*年\s*({_N10_3}{{1,3}})\s*月\s*({_N10_3}{{1,3}})\s*日\s*起\s*"
    rf"(?:施行|执行|实施|生效|发生效力)")
# ② 相对日期
RE_REL = re.compile(r"自\s*(?:发布|印发|公布|签发|下发|修订发布)\s*之日\s*起\s*"
                    r"(?:施行|执行|实施|生效)")


def _shift(d: str, days: int) -> str:
    """日期字符串平移若干天，返回 YYYY-MM-DD"""
    try:
        dt = date(*map(int, d.split("-"))) + timedelta(days=days)
        return dt.isoformat()
    except Exception:
        return d


def cn2int(s):
    s = (s or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if "十" in s:
        a, _, b = s.partition("十")
        t = 1 if a == "" else CN_DIGIT3.get(a)
        o = 0 if b == "" else CN_DIGIT3.get(b)
        if t is None or o is None:
            return None
        return int(t) * 10 + int(o)
    n = 0
    for ch in s:
        d = CN_DIGIT3.get(ch)
        if d is None:
            return None
        n = n * 10 + int(d)
    return n


def parse_effective(content: str, pub_date: str, is_law: bool = False):
    """返回 (日期, 依据)。依据 ∈ {具体日期, 发布之日起}

    is_law=True 表示层级为「法律 / 行政法规」——这类有**修正版**，
    正文里常残留**旧版**的施行条款（如保险法 2015 修正版正文仍写「自2009年10月1日起施行」），
    出现「生效日早于发布日」时按错误处理、丢弃。
    而部门规章/规范性文件的「发布日」多来自 iweicha 平台录入日（常偏晚），
    此时**生效日才是对的**，应保留。
    """
    if not content:
        return "", ""
    tail = content[-TAIL:]

    # ① 具体日期：取最后一个（附则在文末）
    hits = []
    for m in RE_SELF.finditer(tail):
        y, mo, da = cn2int(m.group(1)), cn2int(m.group(2)), cn2int(m.group(3))
        if y and mo and da and 1990 <= y <= 2035 and 1 <= mo <= 12 and 1 <= da <= 31:
            hits.append(f"{y:04d}-{mo:02d}-{da:02d}")
    if hits:
        cand = hits[-1]
        if pub_date:
            py = int(pub_date[:4])
            if not (py - 1 <= int(cand[:4]) <= py + 5):
                cand = ""      # 与发布日期差太远，疑似正文引用了别的法规
            elif cand < _shift(pub_date, -30):
                if is_law:
                    cand = ""  # 法律/行政法规修正版残留旧版条款
        if cand:
            return cand, "具体日期"

    # ② 相对日期 → 用发布日期
    if RE_REL.search(tail) and pub_date:
        return pub_date, "发布之日起"

    return "", ""


def run_effective(dry: bool = False) -> dict:
    print("[8/10] 补生效日期（挖正文附则的施行日期）…", flush=True)
    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute(
        """SELECT id, title, COALESCE(content_text,''), COALESCE(pub_date,''), source_id
           FROM regulation WHERE COALESCE(effective_date,'')=''"""
    ).fetchall()
    print(f"        缺 effective_date：{len(rows)} 条", flush=True)

    updates, why = [], Counter()
    for rid, title, content, pd, _sid in rows:
        d, tag = parse_effective(content, pd)
        if d:
            updates.append((d, rid, title, tag))
            why[tag] += 1

    print(f"        能补出：{len(updates)} 条  " +
          " | ".join(f"{k}:{v}" for k, v in why.items()), flush=True)
    step = max(1, len(updates) // 10)
    for d, _rid, title, tag in updates[::step][:10]:
        print(f"          {d}  [{tag}]  {title[:50]}", flush=True)

    if dry:
        print(f"        [dry-run] 未写库（将补 {len(updates)} 条）", flush=True)
        con.close()
        return {"step": "effective", "updates": len(updates), "dry": True}

    cur.executemany("UPDATE regulation SET effective_date=? WHERE id=?",
                    [(d, rid) for d, rid, _t, _tag in updates])
    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM regulation WHERE COALESCE(effective_date,'')=''").fetchone()[0]
    t = cur.execute("SELECT COUNT(*) FROM regulation").fetchone()[0]
    print(f"        已补写 {len(updates)} 条；覆盖率 {(t-n)*100//max(t,1)}%（仍空 {n} 条）", flush=True)
    con.close()
    return {"step": "effective", "updates": len(updates), "still_empty": n}


# ============================================================================
# 步骤 9：重跑分类（用 category_rules.json 重建 reg_category）
# ============================================================================

MAX_AUX = 3

BOOK = re.compile(r"《([^》]{4,60})》")
BRACKET = re.compile(r"[（(]([^）)]{4,60})[）)]")
DOCNO_RE = re.compile(r"[〔\[(（]\s*\d{4}\s*[〕\])）]\s*第?\s*[\d一二三四五六七八九十]+\s*号?")
CLS_ORG = re.compile(r"(中国人民银行|国家金融监督管理总局|金融监管总局|中国银行保险监督管理委员会|"
                     r"中国银行业监督管理委员会|中国保险监督管理委员会|中国证券监督管理委员会|"
                     r"国家外汇管理局|中国银保监会|中国银监会|中国保监会|中国证监会|财政部|"
                     r"国家发展和改革委员会|国家税务总局|国家市场监督管理总局|海关总署|"
                     r"国务院办公厅|国务院|银保监会|银监会|保监会|人民银行)")
PUB = re.compile(r"(?:公布|印发|发布|制定|施行|修订)[^。；\n]{0,15}《([^》]{4,60})》")


def load_category_rules():
    d = json.loads(CATEGORY_RULES.read_text(encoding="utf-8"))
    return d["rules"]


def effective_title(title: str) -> str:
    """原标题 + 书名号/括号里抽出的真名，并**剥离机构名**

    ⚠️ 关键：不剥离机构名会大面积误判——
       「中国人民银行 中国**银行保险**监督管理委员会 关于……的通知」
       文号里的机构名含「保险」二字，会让一批银行文件被错判成保险类。
    """
    t = title or ""
    parts = [t]
    parts += BOOK.findall(t)
    for b in BRACKET.findall(t):
        if not DOCNO_RE.search(b) and not re.match(r"^\s*(19|20)\d{2}", b):
            parts.append(b)
    et = " ".join(parts)
    return CLS_ORG.sub(" ", et)


def is_docno_only(title: str) -> bool:
    """标题去掉机构名/文号后是否已没什么内容"""
    core = re.sub(r"[（(][^）)]*[）)]", " ", title or "")
    core = CLS_ORG.sub(" ", core)
    core = DOCNO_RE.sub(" ", core)
    core = re.sub(r"[\s《》、,，。;；:：\-—_的公告令第号]+", "", core)
    return len(core) < 4


def name_from_content(content: str) -> str:
    """纯文号型标题：从正文开头捞真名。优先「公布/印发《XX》」，再退回最长书名号"""
    head = (content or "")[:600]
    m = PUB.search(head)
    if m and len(m.group(1)) >= 6:
        return m.group(1)
    hits = BOOK.findall(head)
    if hits:
        cand = max(hits, key=len)
        if len(cand) >= 6:
            return cand
    return ""


def pick(text: str, rule: dict):
    """返回该规则在 text 中最早命中的标题词位置；未命中返回 None"""
    if any(x in text for x in rule.get("exclude", [])):
        return None
    best = None
    for kw in rule.get("title", []):
        p = text.find(kw)
        if p >= 0 and (best is None or p < best[0]):
            best = (p, kw)
    return best


def classify_v2(title: str, content: str, org: str, rules):
    """返回 (主分类名, [辅助分类名])"""
    et = effective_title(title)
    # 纯文号型 → 补真名
    if is_docno_only(title):
        nm = name_from_content(content)
        if nm:
            et += " " + nm

    biz = [r for r in rules if r.get("type") != "domain"]
    dom = [r for r in rules if r.get("type") == "domain"]

    scored = []
    for r in biz:
        h = pick(et, r)
        if h:
            scored.append((-r["weight"], h[0], r["name"], True))
    scored.sort()

    main = scored[0][2] if scored else ""
    cats = [s[2] for s in scored[:1 + MAX_AUX]]

    # 正文补给（只作辅助；且**必须已有主分类**，否则会出现"有辅助无主"的怪状态）
    body = (content or "")[:500]
    if body and main:
        for r in biz:
            if r["name"] in cats:
                continue
            if any(x in body for x in r.get("body", [])) and not any(
                    x in body for x in r.get("exclude", [])):
                cats.append(r["name"])
                if len(cats) >= 1 + MAX_AUX:
                    break

    # 兜底域（只认标题）
    if not main:
        for r in dom:
            if pick(et, r):
                main = r["name"]
                cats = [main] + [c for c in cats if c != main]
                break
    # ⚠️ 注意：正文命中的**不得**升格为主分类——否则「能效信贷指引」正文提了一句
    #    「信托公司」就会被归进信托。主分类严格只看标题。
    return main, cats


def run_classify(dry: bool = False) -> dict:
    print("[9/10] 重跑分类（重建 reg_category）…", flush=True)
    if not CATEGORY_RULES.exists():
        step_skip("重跑分类", f"缺少规则文件 {CATEGORY_RULES}")
        return {"step": "classify", "skipped": True}

    rules = load_category_rules()
    con = sqlite3.connect(DB)
    cur = con.cursor()
    CAT = {n: i for i, n in cur.execute("SELECT id, name FROM category")}
    missing = [r["name"] for r in rules if r["name"] not in CAT]
    if missing:
        step_skip("重跑分类", f"规则里有、分类表里没有的：{missing}")
        con.close()
        return {"step": "classify", "skipped": True}

    rows = cur.execute("""SELECT id, title, COALESCE(content_text,''), source_id
                          FROM regulation""").fetchall()
    print(f"        待分类法规：{len(rows)} 条", flush=True)

    out, main_cnt, aux_cnt, nocnt = [], Counter(), Counter(), 0
    for rid, title, content, sid in rows:
        org = "人行" if "pbc" in (sid or "") else "金监局"
        m, cats = classify_v2(title, content, org, rules)
        if not m and not cats:
            nocnt += 1
        if m:
            main_cnt[m] += 1
            out.append((rid, CAT[m], 1))
        for c in cats:
            if c == m:
                continue
            aux_cnt[c] += 1
            out.append((rid, CAT[c], 0))

    print(f"        主分类覆盖：{len(rows) - nocnt} / {len(rows)}（无任何分类 {nocnt} 条）", flush=True)
    print(f"        关联总数：{len(out)}  平均 {len(out)/max(len(rows),1):.2f} 个/条", flush=True)
    print("        主分类 Top 12: " +
          ", ".join(f"{n}:{c}" for n, c in main_cnt.most_common(12)), flush=True)

    if dry:
        print("        [dry-run] 未写库", flush=True)
        con.close()
        return {"step": "classify", "links": len(out), "dry": True}

    cur.execute("DELETE FROM reg_category")
    cur.executemany("INSERT INTO reg_category (reg_id, cat_id, is_primary) VALUES (?,?,?)", out)
    con.commit()
    n_main = sum(1 for r in out if r[2] == 1)
    print(f"        已重建 reg_category，共 {len(out)} 条（主 {n_main} / 辅 {len(out)-n_main}）", flush=True)
    con.close()
    return {"step": "classify", "links": len(out), "main": n_main}


# ============================================================================
# 步骤 10：重建索引（从 _meta.json 重建 待入库 目录的 _index.csv）
# ============================================================================

IDX_FIELDS = ["source_id", "org", "title", "doc_number", "pub_date", "status",
              "content_len", "content_from", "source_sys", "detail_url",
              "original_url", "dir_name"]


def rebuild_index_for(org: str, tu: Path) -> int:
    d = tu / org
    if not d.is_dir():
        step_skip(f"重建索引·{org}", f"目录不存在 {d}")
        return 0
    rows = []
    for mp in sorted(d.glob("*/_meta.json")):
        try:
            m = json.loads(mp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"          [警告] 跳过损坏的 {mp.name}: {e}", flush=True)
            continue
        rows.append({
            "source_id": m.get("source_id", ""),
            "org": m.get("org", org),
            "title": m.get("title", ""),
            "doc_number": m.get("doc_number", ""),
            "pub_date": m.get("pub_date", ""),
            "status": m.get("status", ""),
            "content_len": m.get("content_len", 0),
            "content_from": m.get("content_from", ""),
            "source_sys": m.get("source_sys", ""),
            "detail_url": m.get("detail_url", ""),
            "original_url": m.get("original_url", ""),
            "dir_name": mp.parent.name,
        })

    rows.sort(key=lambda x: (x["pub_date"] or "", x["source_id"]), reverse=True)
    out = d / "_index.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=IDX_FIELDS)
        w.writeheader()
        w.writerows(rows)

    print(f"        {org}: 重建 {len(rows)} 行 → {out.name}", flush=True)
    print("          按 source_sys（数据来自哪）: " +
          ", ".join(f"{k}:{v}" for k, v in Counter(r["source_sys"] for r in rows).most_common()), flush=True)
    print("          按 content_from（正文取自哪）: " +
          ", ".join(f"{k}:{v}" for k, v in Counter(r["content_from"] for r in rows).most_common()), flush=True)
    return len(rows)


def run_reindex(tu: Path, dry: bool = False, only_orgs=None) -> dict:
    print("[10/10] 重建索引（从 _meta.json 重建 _index.csv）…", flush=True)
    if not tu.is_dir():
        step_skip("重建索引", f"目录不存在 {tu}")
        return {"step": "reindex", "skipped": True}
    if dry:
        print("        [dry-run] 不写 _index.csv", flush=True)
        return {"step": "reindex", "dry": True}
    targets = only_orgs or [d.name for d in sorted(tu.iterdir()) if d.is_dir()]
    total = 0
    for org in targets:
        total += rebuild_index_for(org, tu)
    return {"step": "reindex", "rows": total}


# ============================================================================
# 主流程
# ============================================================================
STEPS = ["schema", "category", "regs", "early18", "docnumber",
         "pubdate", "fix_pubdate", "effective", "classify", "reindex"]


def main():
    ap = argparse.ArgumentParser(description="阶段三：灌库与回填")
    ap.add_argument("--only", default=None, help=f"只跑某几步（逗号分隔）：{','.join(STEPS)}")
    ap.add_argument("--dry-run", action="store_true", help="只分析，不写库")
    ap.add_argument("--tu", default="待入库_v2", help="待入库目录（默认 待入库_v2）")
    ap.add_argument("--verbose", action="store_true", help="打印判重跳过明细")
    args = ap.parse_args()

    tu = ROOT / args.tu
    dry = args.dry_run
    only = {s.strip() for s in args.only.split(",")} if args.only else None

    def want(s):
        return only is None or s in only

    print("=" * 72)
    print("合规知识库 · 阶段三 入库")
    print(f"项目根：{ROOT}")
    print(f"数据库：{DB}")
    print(f"待入库：{tu}")
    print(f"{'[dry-run] ' if dry else ''}步骤：{sorted(only) if only else STEPS}")
    print("=" * 72)

    if not DB.exists():
        print(f"[!] 未找到 {DB}，无法入库。")
        sys.exit(1)

    results = []
    # 每步独立 try/except：单步失败不中断整个流程
    def run(tag, fn, *a, **kw):
        try:
            return fn(*a, **kw)
        except Exception as e:
            print(f"      [失败] {tag} —— {type(e).__name__}: {e}", flush=True)
            return {"step": tag, "error": str(e)}

    if want("schema"):
        results.append(run("schema", run_schema, dry))
    if want("category"):
        results.append(run("category", run_category_migration, dry))
    if want("regs"):
        results.append(run("regs", run_regs, tu, dry, args.verbose))
    # ⚠️ 以下回填步骤都依赖主入库后的数据；dry 模式下只分析
    if want("early18"):
        results.append(run("early18", run_early18, dry))
    if want("docnumber"):
        results.append(run("docnumber", run_docnumber, dry))
    if want("pubdate"):
        results.append(run("pubdate", run_pubdate, dry))
    if want("fix_pubdate"):
        results.append(run("fix_pubdate", run_fix_pubdate, dry))
    if want("effective"):
        results.append(run("effective", run_effective, dry))
    if want("classify"):
        results.append(run("classify", run_classify, dry))
    if want("reindex"):
        results.append(run("reindex", run_reindex, tu, dry))

    print("=" * 72)
    print(f"{'[dry-run] ' if dry else ''}阶段三完成")
    for r in results:
        print(f"  {r}")
    print("=" * 72)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(130)
