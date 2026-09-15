# -*- coding: utf-8 -*-
"""
检索服务.py —— 法规库 Web 检索服务（方案B：SQL 结构化检索，无 AI）
====================================================================
用法（基础环境 D:/Python/Python312 已有 fastapi/uvicorn）：

  python backend/检索服务.py               # 默认 http://127.0.0.1:8765
  python backend/检索服务.py --port 9000

接口一览：
  GET  /                      Web 检索页（fronted/index.html）
  POST /api/search            结构化检索：关键词 + 多维筛选 + facets 动态计数 + 分页
  GET  /api/regulation/{id}   单条详情：元数据 + 全文（按需加载）
  GET  /api/filters           筛选树：层级/机构/分类/时效/年份 + 全库计数
  GET  /api/stats             库概况
  POST /api/semantic          【预留】向量语义检索（S2 实施前返回 501）
  POST /retrieval             【预留】Dify 外部知识库规范接口（S4 实施前返回 501）

智能路由（/api/search 内部）：
  关键词含文号特征（〔〕[]/「号」结尾） → 文号/标题 LIKE 精确优先
  长度 >= 3                            → FTS5 trigram 子串检索，BM25 相关度排序
  长度 < 3                             → LIKE 兜底（trigram 覆盖不到短词）
  空                                   → 全库浏览，按发布日期倒序
"""
import os
import re
import sqlite3
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel


# ---------------------------------------------------------------- 项目根定位
def _project_root():
    p = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.exists(os.path.join(p, "法规库.db")) or os.path.exists(os.path.join(p, ".workbuddy")):
            return p
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return p


ROOT = _project_root()
DB = os.path.join(ROOT, "法规库.db")
WEB_DIR = os.path.join(ROOT, "fronted")

DOC_NO_RE = re.compile(r"[〔\[]\s*\d{4}\s*[〕\]]|[〔\[]\s*\d{4}\s*[〕\]]\s*第?\s*\d+\s*号|\d+\s*号\s*$")
STATUS_EMPTY = "未标注"


def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con


app = FastAPI(title="法规库检索服务", version="0.1.0")


# ---------------------------------------------------------------- 请求模型
class SearchReq(BaseModel):
    q: str = ""                          # 旧版整框关键词（保留兼容；新表单用下面字段）
    title_q: Optional[str] = None        # 标题搜索（支持 +且 / 或 组合式）
    title_mode: str = "exact"            # exact 精确(整组连续短语) | fuzzy 模糊(拆词同现)
    content_q: Optional[str] = None      # 正文搜索（同上组合式，且要求同单元命中）
    content_scope: Optional[str] = None  # tiao 同条 | duan 同段 | ju 同句 | None 不限位置（仅精确模式生效）
    doc_number: Optional[str] = None     # 法规文号（文号或标题包含即命中）
    issuer: Optional[str] = None         # 颁布单位（模糊包含，任一关联机构命中即算）
    industries: Optional[List[str]] = None   # 银行业/证券业/保险业（按机构映射）
    hierarchy: Optional[str] = None      # 层级名（顶级自动含子级）
    category: Optional[str] = None       # 专题名
    status: Optional[str] = None         # 有效/已废止/已修订/失效/""(未标注)
    year: Optional[str] = None           # 发布年份
    date_from: Optional[str] = None      # YYYY-MM-DD
    date_to: Optional[str] = None
    page: int = 1
    size: int = 20
    sort: str = "relevance"              # relevance | pub_date | effective_date


# ---------------------------------------------------------------- 工具
def _hierarchy_ids(cur, name: str) -> List[int]:
    """层级名 → id 集合；顶级自动扩展子级"""
    row = cur.execute("SELECT id, parent_id FROM hierarchy WHERE name = ?", (name,)).fetchone()
    if not row:
        return []
    ids = [row["id"]]
    if row["parent_id"] is None:
        ids += [r["id"] for r in cur.execute("SELECT id FROM hierarchy WHERE parent_id = ?", (row["id"],))]
    return ids


def _clean(s):
    return (s or "").strip()


def _snippet(text: str, kw: str, width: int = 90) -> str:
    """找关键词位置截取上下文摘要"""
    text = " ".join(str(text or "").split())
    if not text:
        return ""
    i = text.find(kw) if kw else -1
    if i < 0:
        return text[: width * 2] + ("…" if len(text) > width * 2 else "")
    start = max(0, i - width)
    end = min(len(text), i + len(kw) + width)
    return ("…" if start > 0 else "") + text[start:end] + ("…" if end < len(text) else "")


def _fts_query(q: str) -> Optional[str]:
    """FTS5 查询串：整短语加引号（trigram 下等于子串匹配）"""
    q = _clean(q)
    if not q:
        return None
    return '"' + q.replace('"', '""') + '"'


# ---------------- 法询式组合检索 ----------------
# 组合式语法："+"=且，"/"=或。例：房贷+抵押/质押 = 房贷且(抵押或质押)
def _parse_expr(s):
    """组合式（精确模式）→ [(原始组文本, [且词...]), ...]（组间或）；空返回 None"""
    s = _clean(s)
    if not s:
        return None
    out = []
    for g in s.split("/"):
        g = g.strip()
        if not g:
            continue
        terms = [t.strip() for t in g.split("+") if t.strip()]
        if terms:
            out.append((g, terms))
    return out or None


def _fuzzy_terms(s):
    """模糊模式：不支持组合语法，操作符与空格都当分隔符，各词全部命中即可（顺序无关）"""
    s = _clean(s)
    if not s:
        return []
    terms = [t for t in re.split(r"[\s+/]+", s) if t]
    return terms or ([s] if s else [])


# 行业勾选 → 机构映射（银保监/金监总局横跨银行+保险，两边都算）
_INDUSTRY_ISSUERS = {
    "银行业": ["中国人民银行", "国家金融监督管理总局", "中国银行业监督管理委员会",
             "中国银行保险监督管理委员会", "国家外汇管理局"],
    "证券业": ["中国证券监督管理委员会"],
    "保险业": ["中国保险监督管理委员会", "中国银行保险监督管理委员会",
             "国家金融监督管理总局"],
}

_TIAO_SPLIT = re.compile(r"(?=第\s*[一二三四五六七八九十百千零〇0-9]+\s*条)")


def _content_units(text: str, scope: str) -> List[str]:
    """按同条/同段/同句把正文切成匹配单元；scope 为空 = 不限位置（整篇一个单元）"""
    text = text or ""
    if not scope:                      # 不限位置：整篇作为一个单元
        return [text.strip()] if text.strip() else [""]
    if scope == "duan":
        units = [u.strip() for u in re.split(r"[\r\n]+", text) if u.strip()]
    elif scope == "ju":
        units = [u.strip() for u in re.split(r"[。！？；;\n]+", text) if u.strip()]
    else:  # tiao：在第X条边界切，条号保留在单元开头
        units = [u.strip() for u in _TIAO_SPLIT.split(text) if u.strip()]
    if not units and text.strip():     # 切不出单元（无条款结构/无换行）→ 整篇兜底
        units = [text.strip()]
    return units or [""]


def _grp_hit(unit: str, grp) -> bool:
    """组命中：组内所有且词都出现在单元中（各词自身连续、词间顺序无关）"""
    return all(t in unit for t in grp[1])


def _expr_hit(text: str, expr, scope: str):
    """正文是否命中组合式（要求所有且词落在同一单元）；命中返回该单元，未命中 None"""
    for u in _content_units(text, scope):
        if any(_grp_hit(u, g) for g in expr):
            return u
    return None


def _trim_unit(u: str, width: int = 150) -> str:
    u = " ".join(u.split())
    return u[:width] + ("…" if len(u) > width else "")


def _trim_around(u: str, terms, width: int = 150, pad: int = 40) -> str:
    """不限位置命中：截取首个命中词附近的片段，避免只截正文开头丢了上下文"""
    u = " ".join(u.split())
    if len(u) <= width:
        return u
    pos = -1
    for t in (terms or []):
        p = u.find(t)
        if p >= 0 and (pos < 0 or p < pos):
            pos = p
    if pos < 0:
        return u[:width] + "…"
    start = max(0, pos - pad)
    end = min(len(u), start + width)
    seg = u[start:end]
    return ("…" if start > 0 else "") + seg + ("…" if end < len(u) else "")


def _resolve_keyword(cur, q: str):
    """
    智能路由：返回 (mode, ids_or_None)
    mode: 'none'(无关键词) | 'fts' | 'like' | 'docno'
    ids 非 None 时主查询用 id IN 临时表过滤
    """
    q = _clean(q)
    if not q:
        return "none", None

    cur.execute("DROP TABLE IF EXISTS _q_ids")
    if DOC_NO_RE.search(q):
        # 文号模式：文号或标题精确包含
        cur.execute("CREATE TEMP TABLE _q_ids (id INTEGER PRIMARY KEY)")
        cur.execute(
            "INSERT INTO _q_ids SELECT id FROM regulation "
            "WHERE doc_number LIKE ? OR title LIKE ? OR content_text LIKE ?",
            (f"%{q}%",) * 3,
        )
        return "docno", "_q_ids"

    if len(q) >= 3:
        fts_q = _fts_query(q)
        try:
            rows = cur.execute(
                "SELECT rowid FROM reg_fts WHERE reg_fts MATCH ? ORDER BY bm25(reg_fts)",
                (fts_q,),
            ).fetchall()
            cur.execute("CREATE TEMP TABLE _q_ids (id INTEGER PRIMARY KEY, rank INTEGER)")
            for rank, r in enumerate(rows):
                cur.execute("INSERT INTO _q_ids VALUES (?, ?)", (r["rowid"], rank))
            return "fts", "_q_ids"
        except sqlite3.OperationalError:
            pass  # FTS 语法问题则走 LIKE 兜底

    # LIKE 兜底（短词或 FTS 异常）
    cur.execute("CREATE TEMP TABLE _q_ids (id INTEGER PRIMARY KEY)")
    cur.execute(
        "INSERT INTO _q_ids SELECT id FROM regulation WHERE title LIKE ? OR content_text LIKE ?",
        (f"%{q}%",) * 2,
    )
    return "like", "_q_ids"


# ---------------------------------------------------------------- 条件构建
def _build_conditions(cur, req: SearchReq, exclude: Optional[str] = None):
    """返回 (where_sql_list, params_list)；exclude 用于 facets 排除自身维度"""
    conds, params = [], []

    if req.hierarchy and exclude != "hierarchy":
        ids = _hierarchy_ids(cur, req.hierarchy)
        if not ids:
            ids = [-1]
        conds.append("rg.hierarchy_id IN (%s)" % ",".join("?" * len(ids)))
        params += ids

    if req.issuer and exclude != "issuer":
        conds.append(
            "rg.id IN (SELECT ri.reg_id FROM regulation_issuer ri "
            "JOIN issuer i ON i.id = ri.issuer_id WHERE i.name LIKE ?)"
        )
        params.append(f"%{_clean(req.issuer)}%")

    if req.category and exclude != "category":
        conds.append(
            "rg.id IN (SELECT rc.reg_id FROM reg_category rc "
            "JOIN category c ON c.id = rc.cat_id WHERE c.name = ?)"
        )
        params.append(req.category)

    # 标题搜索：精确=组合语法生效（+且/或，各词连续出现）；模糊=拆词全部命中（顺序无关）
    if _clean(req.title_q) and exclude != "title":
        if req.title_mode == "exact":
            expr_t = _parse_expr(req.title_q)
            if expr_t:
                grps = []
                for raw, terms in expr_t:
                    grps.append(" AND ".join("rg.title LIKE ?" for _ in terms))
                    params += [f"%{t}%" for t in terms]
                conds.append("(" + " OR ".join(f"({g})" for g in grps) + ")")
        else:
            terms = _fuzzy_terms(req.title_q)
            if terms:
                conds.append("(" + " AND ".join("rg.title LIKE ?" for _ in terms) + ")")
                params += [f"%{t}%" for t in terms]

    # 法规文号：文号或标题包含（库内文号覆盖约半数，标题兜底）
    if _clean(req.doc_number) and exclude != "docno":
        conds.append("(rg.doc_number LIKE ? OR rg.title LIKE ?)")
        params += [f"%{_clean(req.doc_number)}%"] * 2

    # 行业勾选：按机构映射取并集
    inds = [x for x in (req.industries or []) if x in _INDUSTRY_ISSUERS]
    if inds and exclude != "industry":
        names = sorted(set().union(*(_INDUSTRY_ISSUERS[x] for x in inds)))
        conds.append(
            "rg.id IN (SELECT ri.reg_id FROM regulation_issuer ri "
            "JOIN issuer i ON i.id = ri.issuer_id WHERE i.name IN (%s))"
            % ",".join("?" * len(names))
        )
        params += names

    if req.status is not None and exclude != "status":
        if req.status == "":
            conds.append("(rg.status IS NULL OR TRIM(rg.status) = '')")
        else:
            conds.append("rg.status = ?")
            params.append(req.status)

    if req.year and exclude != "year":
        conds.append("substr(rg.pub_date, 1, 4) = ?")
        params.append(req.year)

    if req.date_from and exclude != "year":
        conds.append("rg.pub_date >= ?")
        params.append(req.date_from)

    if req.date_to and exclude != "year":
        conds.append("rg.pub_date <= ?")
        params.append(req.date_to)

    return conds, params


# ---------------------------------------------------------------- 接口
@app.post("/api/search")
def api_search(req: SearchReq):
    con = get_db()
    cur = con.cursor()
    try:
        page = max(1, req.page)
        size = min(100, max(1, req.size))
        mode, ids_table = _resolve_keyword(cur, req.q)
        # 位置粒度（同条/同段/同句）仅在「精确」模式生效；模糊模式 = 不限位置
        if req.title_mode == "exact":
            scope = req.content_scope if req.content_scope in ("tiao", "duan", "ju") else None
        else:
            scope = None
        # 精确=组合语法生效；模糊=拆词全部命中（正文同样适用，单元粒度由同条/同段/同句决定）
        if req.title_mode == "exact":
            expr_t = _parse_expr(req.title_q)
            expr_c = _parse_expr(req.content_q)
        else:
            tt, ct = _fuzzy_terms(req.title_q), _fuzzy_terms(req.content_q)
            expr_t = [("", tt)] if tt else None
            expr_c = [("", ct)] if ct else None
        if _clean(req.title_q) or _clean(req.content_q) or _clean(req.doc_number) or (req.industries or []):
            mode = "form"

        # 正文预筛临时表：任一关键词出现在正文才进入单元级匹配（大幅缩小扫描面）
        if expr_c:
            terms = sorted({t for g in expr_c for t in g[1]})
            cur.execute("DROP TABLE IF EXISTS _cand")
            cur.execute("CREATE TEMP TABLE _cand (id INTEGER PRIMARY KEY)")
            cur.execute(
                "INSERT INTO _cand SELECT id FROM regulation WHERE "
                + " OR ".join("content_text LIKE ?" for _ in terms),
                [f"%{t}%" for t in terms],
            )

        def final_rows(exclude: Optional[str] = None):
            """SQL 条件（可排除某维度）∩ 正文单元级匹配 → 有序行列表"""
            conds, params = _build_conditions(cur, req, exclude=exclude)
            if ids_table:
                conds.append(f"rg.id IN (SELECT id FROM {ids_table})")
            if expr_c:
                conds.append("rg.id IN (SELECT id FROM _cand)")
            where = (" WHERE " + " AND ".join(conds)) if conds else ""

            if req.sort == "effective_date":
                order = " ORDER BY (rg.effective_date IS NULL), rg.effective_date DESC, rg.id"
            elif ids_table and mode == "fts" and req.sort == "relevance":
                order = (" ORDER BY (SELECT rank FROM _q_ids WHERE _q_ids.id = rg.id),"
                         " (rg.pub_date IS NULL), rg.pub_date DESC")
            else:
                order = " ORDER BY (rg.pub_date IS NULL), rg.pub_date DESC, rg.id DESC"

            cols = "rg.id, rg.title, rg.pub_date" + (", rg.content_text" if expr_c else "")
            rows = cur.execute(
                f"SELECT {cols} FROM regulation rg{where}{order}", params).fetchall()

            if not expr_c:
                return rows
            out = []
            terms_c = sorted({t for g in expr_c for t in g[1]})
            for r in rows:
                u = _expr_hit(r["content_text"] or "", expr_c, scope)
                if u is not None:
                    d = dict(r)
                    # 不限位置时 u 是整篇，摘要取命中词附近片段；有单元粒度时直接截单元
                    d["_snip"] = _trim_unit(u) if scope else _trim_around(u, terms_c)
                    d.pop("content_text", None)
                    out.append(d)
            # 标题命中的排前面（智能排序时）
            if expr_t and req.sort == "relevance":
                def tsc(d):
                    t = d["title"] or ""
                    hit = any(_grp_hit(t, g) for g in expr_t)
                    return 0 if hit else 1
                rows_sorted = sorted(out, key=tsc)
                return rows_sorted
            return out

        rows = final_rows()
        total = len(rows)
        page_rows = rows[(page - 1) * size: page * size]

        # 组装 hits（含摘要、分类、牵头机构）
        hits = []
        for r in page_rows:
            rid = r["id"]
            cats = [x[0] for x in cur.execute(
                "SELECT c.name FROM reg_category rc JOIN category c ON c.id = rc.cat_id "
                "WHERE rc.reg_id = ? ORDER BY rc.is_primary DESC", (rid,)).fetchall()]
            primary = cur.execute(
                "SELECT i.name FROM regulation_issuer ri JOIN issuer i ON i.id = ri.issuer_id "
                "WHERE ri.reg_id = ? AND ri.is_primary = 1", (rid,)).fetchone()
            if "_snip" in r:
                snip = r["_snip"]
            else:
                ct = cur.execute("SELECT content_text FROM regulation WHERE id = ?", (rid,)).fetchone()[0]
                snip = _snippet(ct, req.q if mode not in ("none", "form") else "")
            hits.append({
                "id": rid, "title": r["title"], "doc_number": None, "hierarchy": None,
                "status": None, "pub_date": r["pub_date"], "effective_date": None,
                "issuer": primary["name"] if primary else None,
                "categories": cats, "source_url": None, "snippet": snip,
            })
        # 补齐展示字段
        meta = {}
        if page_rows:
            pids = [r["id"] for r in page_rows]
            q_marks = ",".join("?" * len(pids))
            for x in cur.execute(
                    f"SELECT id, doc_number, hierarchy, status, effective_date, source_url, issuer "
                    f"FROM regulation WHERE id IN ({q_marks})", pids):
                meta[x["id"]] = x
        for h in hits:
            m = meta.get(h["id"])
            if m:
                h.update({"doc_number": m["doc_number"], "hierarchy": m["hierarchy"],
                          "status": m["status"], "effective_date": m["effective_date"],
                          "source_url": m["source_url"]})
                if not h["issuer"]:
                    h["issuer"] = m["issuer"]

        # facets：每个维度在「其余全部条件」下的分布（含正文单元级匹配）
        facets = {}
        for dim in ("hierarchy", "issuer", "category", "status", "year"):
            fids = [r["id"] for r in final_rows(exclude=dim)]
            cur.execute("DROP TABLE IF EXISTS _fx")
            cur.execute("CREATE TEMP TABLE _fx (id INTEGER PRIMARY KEY)")
            cur.executemany("INSERT INTO _fx VALUES (?)", [(i,) for i in fids])
            if dim == "hierarchy":
                agg = {}
                for x in cur.execute(
                        "SELECT h.name AS k, h.parent_id AS pid, COUNT(*) AS n "
                        "FROM regulation rg JOIN hierarchy h ON h.id = rg.hierarchy_id "
                        "WHERE rg.id IN (SELECT id FROM _fx) GROUP BY h.id"):
                    top = x["k"] if x["pid"] is None else cur.execute(
                        "SELECT name FROM hierarchy WHERE id = ?", (x["pid"],)).fetchone()["name"]
                    agg[top] = agg.get(top, 0) + x["n"]
                facets[dim] = [{"key": k, "count": v}
                               for k, v in sorted(agg.items(), key=lambda kv: -kv[1])]
            elif dim == "issuer":
                facets[dim] = [{"key": x["k"], "count": x["n"]} for x in cur.execute(
                    "SELECT i.name AS k, COUNT(DISTINCT rg.id) AS n FROM regulation rg "
                    "JOIN regulation_issuer ri ON ri.reg_id = rg.id "
                    "JOIN issuer i ON i.id = ri.issuer_id "
                    "WHERE rg.id IN (SELECT id FROM _fx) GROUP BY i.id ORDER BY n DESC")]
            elif dim == "category":
                facets[dim] = [{"key": x["k"], "group": x["grp"], "count": x["n"]}
                               for x in cur.execute(
                    "SELECT c.name AS k, p.name AS grp, COUNT(*) AS n FROM regulation rg "
                    "JOIN reg_category rc ON rc.reg_id = rg.id "
                    "JOIN category c ON c.id = rc.cat_id "
                    "LEFT JOIN category p ON p.id = c.parent_id "
                    "WHERE rg.id IN (SELECT id FROM _fx) AND c.parent_id IS NOT NULL "
                    "GROUP BY c.id ORDER BY n DESC")]
            elif dim == "status":
                facets[dim] = [{"key": x["k"], "count": x["n"]} for x in cur.execute(
                    "SELECT CASE WHEN rg.status IS NULL OR TRIM(rg.status)='' THEN '"
                    + STATUS_EMPTY + "' ELSE rg.status END AS k, COUNT(*) AS n "
                    "FROM regulation rg WHERE rg.id IN (SELECT id FROM _fx) GROUP BY k ORDER BY n DESC")]
            else:
                facets[dim] = [{"key": x["k"], "count": x["n"]} for x in cur.execute(
                    "SELECT substr(rg.pub_date,1,4) AS k, COUNT(*) AS n FROM regulation rg "
                    "WHERE rg.id IN (SELECT id FROM _fx) AND rg.pub_date IS NOT NULL "
                    "GROUP BY k ORDER BY k DESC")]

        return {"total": total, "page": page, "size": size, "mode": mode,
                "hits": hits, "facets": facets}
    finally:
        con.close()


@app.get("/api/regulation/{reg_id}")
def api_regulation(reg_id: int):
    con = get_db()
    cur = con.cursor()
    try:
        r = cur.execute("SELECT * FROM regulation WHERE id = ?", (reg_id,)).fetchone()
        if not r:
            raise HTTPException(404, "法规不存在")
        cats = [{"name": x["name"], "group": x["grp"], "is_primary": x["ip"]} for x in cur.execute(
            "SELECT c.name, p.name AS grp, rc.is_primary AS ip FROM reg_category rc "
            "JOIN category c ON c.id = rc.cat_id LEFT JOIN category p ON p.id = c.parent_id "
            "WHERE rc.reg_id = ? ORDER BY rc.is_primary DESC", (reg_id,))]
        orgs = [{"name": x["name"], "is_primary": x["ip"]} for x in cur.execute(
            "SELECT i.name, ri.is_primary AS ip FROM regulation_issuer ri "
            "JOIN issuer i ON i.id = ri.issuer_id WHERE ri.reg_id = ? ORDER BY ri.is_primary DESC",
            (reg_id,))]
        return {"id": r["id"], "title": r["title"], "doc_number": r["doc_number"],
                "issuer": r["issuer"], "hierarchy": r["hierarchy"], "scope": r["scope"],
                "status": r["status"], "pub_date": r["pub_date"],
                "effective_date": r["effective_date"], "content_text": r["content_text"],
                "source_url": r["source_url"], "categories": cats, "issuers": orgs}
    finally:
        con.close()


@app.get("/api/filters")
def api_filters():
    """全库筛选树（打开页面时拉一次）"""
    con = get_db()
    cur = con.cursor()
    try:
        hier = {}
        for r in cur.execute(
            "SELECT h.id, h.name, h.parent_id, COUNT(rg.id) AS n FROM hierarchy h "
            "LEFT JOIN regulation rg ON rg.hierarchy_id = h.id GROUP BY h.id ORDER BY h.id"):
            hier[r["id"]] = {"name": r["name"], "parent_id": r["parent_id"], "count": r["n"]}
        id_by_name = {v["name"]: k for k, v in hier.items()}
        tops = [v for v in hier.values() if v["parent_id"] is None]
        tree = []
        for t in tops:
            kids = [{"name": v["name"], "count": v["count"]}
                    for v in hier.values() if v["parent_id"] == id_by_name[t["name"]]]
            top_count = t["count"] + sum(k["count"] for k in kids)  # 顶级含子级聚合
            tree.append({"name": t["name"], "count": top_count, "children": kids})

        orgs = [{"key": r["name"], "count": r["n"]} for r in cur.execute(
            "SELECT i.name AS name, COUNT(DISTINCT ri.reg_id) AS n FROM issuer i "
            "JOIN regulation_issuer ri ON ri.issuer_id = i.id GROUP BY i.id ORDER BY n DESC")]

        cat_groups = {}
        for r in cur.execute(
            "SELECT c.name, p.name AS grp, COUNT(*) AS n FROM reg_category rc "
            "JOIN category c ON c.id = rc.cat_id LEFT JOIN category p ON p.id = c.parent_id "
            "WHERE c.parent_id IS NOT NULL GROUP BY c.id ORDER BY p.sort_order, c.sort_order"):
            cat_groups.setdefault(r["grp"] or "其他", []).append({"key": r["name"], "count": r["n"]})

        statuses = [{"key": (r[0] or STATUS_EMPTY), "count": r[1]} for r in cur.execute(
            "SELECT status, COUNT(*) FROM regulation GROUP BY status ORDER BY 2 DESC")]

        years = [{"key": r[0], "count": r[1]} for r in cur.execute(
            "SELECT substr(pub_date,1,4) y, COUNT(*) FROM regulation "
            "WHERE pub_date IS NOT NULL GROUP BY y ORDER BY y DESC")]

        return {"hierarchy": tree, "issuer": orgs,
                "category": [{"group": g, "items": items} for g, items in cat_groups.items()],
                "status": statuses, "year": years}
    finally:
        con.close()


@app.get("/api/stats")
def api_stats():
    con = get_db()
    cur = con.cursor()
    try:
        total = cur.execute("SELECT COUNT(*) FROM regulation").fetchone()[0]
        eff = cur.execute("SELECT COUNT(*) FROM regulation WHERE status='有效'").fetchone()[0]
        void = cur.execute("SELECT COUNT(*) FROM regulation WHERE status='已废止'").fetchone()[0]
        cat_n = cur.execute("SELECT COUNT(*) FROM category WHERE parent_id IS NOT NULL").fetchone()[0]
        return {"total": total, "effective": eff, "repealed": void,
                "categories": cat_n, "ai_ready": False}
    finally:
        con.close()


@app.post("/api/semantic")
def api_semantic():
    """预留：向量语义检索。S2（向量化）实施后启用。"""
    return JSONResponse({"detail": "向量检索未启用：待 S2 向量化完成后开放"}, status_code=501)


@app.post("/retrieval")
def api_retrieval():
    """预留：Dify 外部知识库规范接口。S4 实施后启用（届时按 Dify 规范返回 records）。"""
    return JSONResponse({"detail": "Dify 外部知识库接口未启用：待 S4 实施后开放"}, status_code=501)


@app.get("/")
def index():
    return FileResponse(os.path.join(WEB_DIR, "index.html"))


if __name__ == "__main__":
    import argparse
    import sys
    import uvicorn

    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    url = f"http://{args.host}:{args.port}"
    print(f"法规库检索服务启动中 → {url}", flush=True)
    print("（前台服务会一直占用本终端，属正常现象；按 Ctrl+C 停止）", flush=True)
    sys.stdout.flush()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
