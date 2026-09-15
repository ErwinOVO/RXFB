-- ============================================================
-- 银行合规知识库 —— 法规数据库建库脚本 v2（SQLite / 可平移 MySQL·PG）
-- 设计来源：参照法询智库筛选维度（法规层级/效力范围/时效性/法规分类）
-- 原则：每个字段都要服务于未来的筛选、关联或 AI 引用
--
-- v2 变更（2026-09-10）：
--   1. 法规层级 单层 TEXT → 两级树 hierarchy 表（法询全量，含地方分支）
--   2. 颁布机构 单层 TEXT → issuer 机构树 + regulation_issuer 多对多（联合发文）
--   3. 效力范围 加二级 scope_sub（全国性 / 地方性→省市级/县处级）
--   4. 增量维护：regulation 加 source_id(带来源前缀) + content_hash(正文指纹)
--   5. reg_relation 预留条款级关联字段(clause_reg/clause_related) + source/confidence
--   6. category 补"融资担保"(176)（原脚本运行时自动新建，现正式入设计稿）
--
-- ★ 取值标准（与法询智库法规库一致）
--   hierarchy 法规层级(两级)：法律/行政法规/司法解释/部门规章/地方法规/行业规范
--           二级：法律(法律/全国人大文件/重大问题决定/条约批准/法律释义)…
--   scope     效力范围：全国性 / 地方性
--   scope_sub 效力范围二级：省市级 / 县处级（仅 scope=地方性 时填）
--   status    时效性：有效/失效/部分修订/修订失效/未生效/征求意见
--   category  法规分类(业务条线，多对多，见 category 表)
-- ============================================================

PRAGMA encoding = 'UTF-8';

-- ------------------------------------------------------------
-- 1. 业务分类表（法规分类：反洗钱 / 消费者保护 / 外汇跨境…）
--    单独建表：一篇法规可属多个赛道（多对多）
-- ------------------------------------------------------------
CREATE TABLE category (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,      -- 分类名
    parent_id   INTEGER REFERENCES category(id),  -- 支持二级分类
    sort_order  INTEGER DEFAULT 0
);

-- 1.1 分类种子（全量照搬法询智库，2026-09-09 核实）
--     结构：一级=分组，二级=专题；id 段位：分组 1-11，专题 101+
INSERT INTO category (id, name, parent_id, sort_order) VALUES
  (1,  '大资管', NULL, 1), (2, '内控合规', NULL, 2), (3, '金融市场', NULL, 3),
  (4,  '银行业务', NULL, 4), (5, '证券期货', NULL, 5), (6, '国际业务', NULL, 6),
  (7,  '政府投融资', NULL, 7), (8, '财税会计', NULL, 8), (9, '科创产业', NULL, 9),
  (10, '保险', NULL, 10), (11, '其他综合', NULL, 11),
  -- 大资管
  (101,'信托',1,1),(102,'理财',1,2),(103,'保险资管',1,3),(104,'私募基金',1,4),(105,'大资管综合',1,5),
  -- 内控合规
  (106,'反洗钱',2,1),(107,'内审规定',2,2),(108,'消费者保护',2,3),(109,'案件防控',2,4),(110,'数据与个保法',2,5),
  -- 金融市场
  (111,'票据法规',3,1),(112,'资金业务',3,2),(113,'债券市场',3,3),(114,'地方金融',3,4),
  -- 银行业务
  (115,'银行业综合监管',4,0),(116,'财富管理',4,1),(117,'不良资产',4,2),(118,'专精特新',4,3),(119,'房地产',4,4),
  (120,'小微贷款和普惠金融',4,5),(121,'互联网贷款',4,6),
  -- 证券期货
  (122,'全面注册制',5,1),(123,'证券基金监管',5,2),(124,'期货',5,3),(125,'投行业务',5,4),(126,'REITS',5,5),
  -- 国际业务
  (127,'外汇跨境',6,1),(128,'跨境人民币',6,2),(129,'QFLP政策',6,3),(130,'利用外资',6,4),(131,'ODI',6,5),(132,'QDLP',6,6),
  -- 政府投融资
  (139,'土地制度',7,1),(140,'地方债',7,2),(141,'国企合规',7,3),(142,'绿色融资标准',7,4),(143,'政府采购',7,5),(144,'城市更新',7,6),
  -- 财税会计
  (147,'税务合规',8,1),(148,'会计准则',8,2),(149,'外贸外资税收',8,3),
  -- 科创产业
  (151,'人工智能',9,1),(152,'民营小微',9,2),
  -- 保险
  (160,'保险业常用法规',10,1),
  -- 其他综合
  (176,'融资担保',11,0),
  (170,'海南自贸政策',11,1),(171,'民法典',11,2),(172,'金融标准',11,3),(173,'ESG',11,4),
  (174,'劳动人事',11,5),(175,'贸易融资与供应链金融',11,6);

-- ------------------------------------------------------------
-- 2. 法规层级表（两级树，v2 新增）
--    一级：法律/行政法规/司法解释/部门规章/地方法规/行业规范
--    二级：法询智库真实子类（含地方分支，暂不填数据但结构就绪）
-- ------------------------------------------------------------
CREATE TABLE hierarchy (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,      -- 层级名，如"部门规章""规范性文件及通知"
    parent_id   INTEGER REFERENCES hierarchy(id),  -- 一级为空，二级挂一级
    level       INTEGER DEFAULT 1,         -- 1=一级 2=二级
    sort_order  INTEGER DEFAULT 0
);

INSERT INTO hierarchy (id, name, parent_id, level, sort_order) VALUES
  -- 一级
  (1, '法律',     NULL, 1, 1),
  (2, '行政法规', NULL, 1, 2),
  (3, '司法解释', NULL, 1, 3),
  (4, '部门规章', NULL, 1, 4),
  (5, '地方法规', NULL, 1, 5),
  (6, '行业规范', NULL, 1, 6),
  -- 二级：法律
  (11, '法律',           1, 2, 1),
  (12, '全国人大文件',   1, 2, 2),
  (13, '重大问题决定',   1, 2, 3),
  (14, '条约批准',       1, 2, 4),
  (15, '法律释义',       1, 2, 5),
  -- 二级：行政法规
  (21, '行政法规',       2, 2, 1),
  (22, '国务院其他文件', 2, 2, 2),
  -- 二级：司法解释
  (31, '两高司法解释',           3, 2, 1),
  (32, '地方性司法解释',         3, 2, 2),
  (33, '两高其他文件',           3, 2, 3),
  (34, '两高白皮书及案例',       3, 2, 4),
  (35, '地方司法白皮书及案例',   3, 2, 5),
  -- 二级：部门规章
  (41, '规范性文件及通知', 4, 2, 1),
  (42, '部门规章',         4, 2, 2),
  -- 二级：地方法规
  (51, '省级地方法规',     5, 2, 1),
  (52, '省级地方政府规章', 5, 2, 2),
  (53, '其他地方文件',     5, 2, 3),
  -- 二级：行业规范
  (61, '行业规范（法规）', 6, 2, 1);

-- ------------------------------------------------------------
-- 3. 颁布机构表（机构树，v2 新增）
--    支持机构沿革(former_names) + 联合发文(多对多，见 regulation_issuer)
-- ------------------------------------------------------------
CREATE TABLE issuer (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,     -- 标准简称
    full_name    TEXT,                     -- 全称
    org_type     TEXT,                     -- 人大/国务院/部委/两高/地方政府/自律组织
    admin_level  TEXT,                     -- 国家级/部委级/省市级/县处级
    former_names TEXT,                     -- 历史沿革，逗号分隔（银保监会,银监会,保监会）
    status       TEXT DEFAULT '现行',      -- 现行/已撤销
    sort_order   INTEGER DEFAULT 0
);

INSERT INTO issuer (id, name, full_name, org_type, admin_level, former_names, status, sort_order) VALUES
  (1,  '全国人民代表大会常务委员会', '全国人民代表大会常务委员会', '人大',   '国家级', '',                        '现行', 1),
  (2,  '国务院',                   '中华人民共和国国务院',     '国务院', '国家级', '',                        '现行', 2),
  (13, '国务院办公厅',             '国务院办公厅',             '国务院', '国家级', '国办',                    '现行', 3),
  (3,  '国家金融监督管理总局',     '国家金融监督管理总局',     '部委',   '部委级', '中国银保监会,中国银监会,中国保监会', '现行', 10),
  (4,  '中国人民银行',             '中国人民银行',             '部委',   '部委级', '人民银行',                '现行', 11),
  (5,  '中国证券监督管理委员会',   '中国证券监督管理委员会',   '部委',   '部委级', '证监会',                  '现行', 12),
  (6,  '国家外汇管理局',           '国家外汇管理局',           '部委',   '部委级', '外汇局',                  '现行', 13),
  (7,  '财政部',                   '中华人民共和国财政部',     '部委',   '部委级', '',                        '现行', 14),
  (8,  '国家发展和改革委员会',     '国家发展和改革委员会',     '部委',   '部委级', '发改委',                  '现行', 15),
  (9,  '国家税务总局',             '国家税务总局',             '部委',   '部委级', '税务总局',                '现行', 16),
  (10, '司法部',                   '中华人民共和国司法部',     '部委',   '部委级', '',                        '现行', 17),
  (11, '最高人民法院',             '最高人民法院',             '两高',   '国家级', '',                        '现行', 20),
  (12, '最高人民检察院',           '最高人民检察院',           '两高',   '国家级', '',                        '现行', 21),
  -- 历史机构（已撤销，历史文件仍挂这些，不能删）
  (20, '中国银行保险监督管理委员会', '中国银行保险监督管理委员会', '部委', '部委级', '银保监会', '已撤销', 30),
  (21, '中国银行业监督管理委员会',   '中国银行业监督管理委员会',   '部委', '部委级', '银监会',   '已撤销', 31),
  (22, '中国保险监督管理委员会',     '中国保险监督管理委员会',     '部委', '部委级', '保监会',   '已撤销', 32);

-- ------------------------------------------------------------
-- 4. 法规主表
--    hierarchy/scope/issuer 保留 TEXT 冗余（过渡/展示用），
--    结构化信息走 hierarchy_id + scope_sub + regulation_issuer
-- ------------------------------------------------------------
CREATE TABLE regulation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,               -- 标准全称（去书名号）
    doc_number      TEXT,                        -- 发文字号
    issuer          TEXT,                        -- 发布机关文本（冗余，展示/兜底）
    hierarchy       TEXT,                        -- 法规层级文本（冗余：法律/行政法规/…）
    scope           TEXT,                        -- 效力范围：全国性/地方性
    status          TEXT DEFAULT '有效',          -- 时效性：有效/失效/部分修订/修订失效/未生效/征求意见
    pub_date        TEXT,                        -- 发布日期 YYYY-MM-DD
    effective_date  TEXT,                        -- 生效日期 YYYY-MM-DD
    content_text    TEXT,                        -- 正文全文
    content_path    TEXT,                        -- 原文件路径
    source_url      TEXT,                        -- 来源链接
    crawl_date      TEXT,                        -- 入库时间
    check_date      TEXT,                        -- 时效性最近核验日期
    remark          TEXT,
    -- v2 新增
    source_id       TEXT,                        -- 官网唯一ID，带来源前缀 nfra:xxx / pbc:xxx
    content_hash    TEXT,                        -- 正文指纹(MD5)，用于同ID内容变更检测
    scope_sub       TEXT,                        -- 效力范围二级：省市级/县处级
    hierarchy_id    INTEGER REFERENCES hierarchy(id)  -- 层级外键（二级叶子节点）
);

CREATE INDEX idx_reg_status     ON regulation(status);
CREATE INDEX idx_reg_hierarchy  ON regulation(hierarchy);
CREATE INDEX idx_reg_pub_date   ON regulation(pub_date);
CREATE INDEX idx_reg_issuer     ON regulation(issuer);
CREATE INDEX idx_reg_eff_date   ON regulation(effective_date);
CREATE INDEX idx_reg_source_id  ON regulation(source_id);
CREATE INDEX idx_reg_hid        ON regulation(hierarchy_id);

CREATE VIRTUAL TABLE reg_fts USING fts5(           -- 全文检索（SQLite 自带 FTS5）
    title, content_text,
    content='regulation', content_rowid='id',
    tokenize='trigram'          -- trigram：支持中文任意子串匹配（≥3字）
);

-- ------------------------------------------------------------
-- 5. 法规 ↔ 分类（多对多）
-- ------------------------------------------------------------
CREATE TABLE reg_category (
    reg_id  INTEGER NOT NULL REFERENCES regulation(id),
    cat_id  INTEGER NOT NULL REFERENCES category(id),
    PRIMARY KEY (reg_id, cat_id)
);

-- ------------------------------------------------------------
-- 6. 法规 ↔ 颁布机构（多对多，v2 新增，支持联合发文）
-- ------------------------------------------------------------
CREATE TABLE regulation_issuer (
    reg_id      INTEGER NOT NULL REFERENCES regulation(id),
    issuer_id   INTEGER NOT NULL REFERENCES issuer(id),
    is_primary  INTEGER DEFAULT 0,   -- 1=牵头发文机关
    PRIMARY KEY (reg_id, issuer_id)
);
CREATE INDEX idx_reg_issuer ON regulation_issuer(issuer_id);

-- ------------------------------------------------------------
-- 7. 法规关联表（两篇法规之间的关系）
--    ★ 阶段1：留空；阶段2用 LLM 解析 + 人工核验后回填。
--    relation_type 受控枚举：
--      '废止' / '被废止' / '修订替换' / '部分修改' /
--      '制定依据' / '配套实施' / '官方解释' / '相关'
--    ★ 效力类关联(废止/修订替换/部分修改)触发状态变更，必须经人工核验，
--      禁止系统自动改状态。
--    v2：预留条款级字段(clause_reg/clause_related)，数据暂不填，靠 LLM 未来补。
-- ------------------------------------------------------------
CREATE TABLE reg_relation (
    reg_id         INTEGER NOT NULL REFERENCES regulation(id),
    related_id     INTEGER NOT NULL REFERENCES regulation(id),
    relation_type  TEXT NOT NULL,
    note           TEXT,               -- 备注：涉及条款、文号依据等
    -- v2 新增：条款级关联 + 可信度
    clause_reg     TEXT,               -- 本法规条款（如"第8条"）
    clause_related TEXT,               -- 关联法规条款（如"第12条"）
    source         TEXT,               -- auto=自动解析 / manual=人工确认
    confidence     REAL,               -- 置信度 0~1（自动解析用）
    PRIMARY KEY (reg_id, related_id, relation_type)
);
CREATE INDEX idx_relation_type ON reg_relation(relation_type);

-- ------------------------------------------------------------
-- 8. 处罚案例表（★ 二期预留）
-- ------------------------------------------------------------
CREATE TABLE penalty_case (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    penalty_no      TEXT,
    punished_org    TEXT,
    punished_person TEXT,
    issuer          TEXT,
    province        TEXT,
    penalty_date    TEXT,
    amount          REAL,
    violations      TEXT,
    penalty_type    TEXT,
    content_text    TEXT,
    content_path    TEXT,
    source_url      TEXT,
    crawl_date      TEXT
);
CREATE INDEX idx_case_org  ON penalty_case(punished_org);
CREATE INDEX idx_case_date ON penalty_case(penalty_date);
CREATE VIRTUAL TABLE case_fts USING fts5(
    violations, content_text,
    content='penalty_case', content_rowid='id',
    tokenize='trigram'
);

-- ------------------------------------------------------------
-- 9. 法规 ↔ 处罚案例关联（★ 二期预留）
-- ------------------------------------------------------------
CREATE TABLE reg_penalty (
    reg_id  INTEGER NOT NULL REFERENCES regulation(id),
    case_id INTEGER NOT NULL REFERENCES penalty_case(id),
    PRIMARY KEY (reg_id, case_id)
);

-- ------------------------------------------------------------
-- 10. 行内制度表（★ 二期预留：外规内化层）
-- ------------------------------------------------------------
CREATE TABLE internal_policy (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_code     TEXT,
    title         TEXT NOT NULL,
    doc_number    TEXT,
    issuer_dept   TEXT,
    status        TEXT DEFAULT '有效',
    pub_date      TEXT,
    effective_date TEXT,
    content_text  TEXT,
    content_path  TEXT
);

CREATE TABLE policy_reg_map (
    policy_id   INTEGER NOT NULL REFERENCES internal_policy(id),
    reg_id      INTEGER NOT NULL REFERENCES regulation(id),
    map_note    TEXT,
    PRIMARY KEY (policy_id, reg_id)
);
