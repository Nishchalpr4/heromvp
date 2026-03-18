import sqlite3
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import psycopg2 for Postgres support
try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.errors
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "graph.db"):
        self.db_url = os.getenv("DATABASE_URL")
        self.db_path = db_path
        self.is_postgres = bool(self.db_url and self.db_url.startswith("postgres"))
        
        if self.is_postgres and not POSTGRES_AVAILABLE:
            logger.error("DATABASE_URL is set but psycopg2 is not installed!")
            self.is_postgres = False

        self._init_db()

    def _get_connection(self):
        if self.is_postgres:
            conn = psycopg2.connect(self.db_url)
            return conn
        else:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            return conn

    def _get_cursor(self, conn):
        """Helper to get the right cursor factory for Postgres/SQLite."""
        if self.is_postgres:
            return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return conn.cursor()

    def _init_db(self):
        """Initializes the schema for the Investment Intelligence System."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            if not self.is_postgres:
                cursor.execute("PRAGMA journal_mode=WAL;")

            # 1. Entity Master
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entity_master (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    color TEXT,
                    attributes TEXT, -- JSON
                    aliases TEXT,    -- JSON
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 2. Relation Master
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS relation_master (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    relation TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(source_id) REFERENCES entity_master(id),
                    FOREIGN KEY(target_id) REFERENCES entity_master(id)
                )
            """)

            # 3. Assertions
            if self.is_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS assertions (
                        id SERIAL PRIMARY KEY,
                        subject_id TEXT NOT NULL,
                        subject_type TEXT NOT NULL,
                        source_text TEXT,
                        confidence FLOAT,
                        status TEXT DEFAULT 'PENDING',
                        document_name TEXT,
                        section_ref TEXT,
                        source_authority INTEGER DEFAULT 5,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS assertions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        subject_id TEXT NOT NULL,
                        subject_type TEXT NOT NULL,
                        source_text TEXT,
                        confidence FLOAT,
                        status TEXT DEFAULT 'PENDING',
                        document_name TEXT,
                        section_ref TEXT,
                        source_authority INTEGER DEFAULT 5,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            
            # 4. Quant Data
            if self.is_postgres:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS quant_data (
                        id SERIAL PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        metric TEXT NOT NULL,
                        value REAL,
                        unit TEXT,
                        period TEXT,
                        source_assertion_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(entity_id) REFERENCES entity_master(id),
                        FOREIGN KEY(source_assertion_id) REFERENCES assertions(id)
                    )
                """)
            else:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS quant_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        entity_id TEXT NOT NULL,
                        metric TEXT NOT NULL,
                        value REAL,
                        unit TEXT,
                        period TEXT,
                        source_assertion_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(entity_id) REFERENCES entity_master(id),
                        FOREIGN KEY(source_assertion_id) REFERENCES assertions(id)
                    )
                """)

            # 5. Ontology Rules
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ontology_rules (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            logger.info(f"Database initialized. Type: {'Postgres' if self.is_postgres else 'SQLite'}")
        finally:
            conn.close()

    def drop_all_tables(self):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            # PostgreSQL requires CASCADE for relations
            cascade = " CASCADE" if self.is_postgres else ""
            cursor.execute(f"DROP TABLE IF EXISTS entity_master{cascade}")
            cursor.execute(f"DROP TABLE IF EXISTS relation_master{cascade}")
            cursor.execute(f"DROP TABLE IF EXISTS assertions{cascade}")
            cursor.execute(f"DROP TABLE IF EXISTS quant_data{cascade}")
            cursor.execute(f"DROP TABLE IF EXISTS ontology_rules{cascade}")
            conn.commit()
        finally:
            conn.close()

    def get_ontology(self):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            
            cursor.execute("SELECT key, data FROM ontology_rules")
            rows = cursor.fetchall()
            return {row['key']: json.loads(row['data']) for row in rows}
        finally:
            conn.close()

    def update_ontology(self, key: str, data: list | dict):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO ontology_rules (key, data, last_updated)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, last_updated = CURRENT_TIMESTAMP
                """, (key, json.dumps(data)))
            else:
                cursor.execute("""
                    INSERT OR REPLACE INTO ontology_rules (key, data, last_updated)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (key, json.dumps(data)))
            conn.commit()
        finally:
            conn.close()

    def upsert_entity(self, entity_id: str, name: str, entity_type: str, color: str = None, attributes: dict = None, aliases: list = None):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO entity_master (id, name, type, color, attributes, aliases, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        color = COALESCE(EXCLUDED.color, entity_master.color),
                        attributes = EXCLUDED.attributes,
                        aliases = EXCLUDED.aliases,
                        updated_at = CURRENT_TIMESTAMP
                """, (entity_id, name, entity_type, color, json.dumps(attributes or {}), json.dumps(aliases or [])))
            else:
                cursor.execute("""
                    INSERT INTO entity_master (id, name, type, color, attributes, aliases, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        type=excluded.type,
                        color=COALESCE(excluded.color, entity_master.color),
                        attributes=excluded.attributes,
                        aliases=excluded.aliases,
                        updated_at=CURRENT_TIMESTAMP
                """, (entity_id, name, entity_type, color, json.dumps(attributes or {}), json.dumps(aliases or [])))
            conn.commit()
        finally:
            conn.close()

    def add_relation(self, rel_id: str, source_id: str, target_id: str, relation: str):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO relation_master (id, source_id, target_id, relation)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (rel_id, source_id, target_id, relation))
            else:
                cursor.execute("""
                    INSERT OR IGNORE INTO relation_master (id, source_id, target_id, relation)
                    VALUES (?, ?, ?, ?)
                """, (rel_id, source_id, target_id, relation))
            conn.commit()
        finally:
            conn.close()

    def add_assertion(self, subject_id: str, subject_type: str, source_text: str, confidence: float, document_name: str, section_ref: str, status: str = 'PENDING', source_authority: int = 5):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
                
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO assertions (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority))
                # With RealDictCursor, we access by name
                row = cursor.fetchone()
                assertion_id = row['id']
            else:
                cursor.execute("""
                    INSERT INTO assertions (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority))
                assertion_id = cursor.lastrowid
            conn.commit()
            return assertion_id
        finally:
            conn.close()

    def add_quant_metric(self, entity_id: str, metric: str, value: float, unit: str, period: str, assertion_id: int = None):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            if self.is_postgres:
                cursor.execute("""
                    INSERT INTO quant_data (entity_id, metric, value, unit, period, source_assertion_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (entity_id, metric, value, unit, period, assertion_id))
            else:
                cursor.execute("""
                    INSERT INTO quant_data (entity_id, metric, value, unit, period, source_assertion_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (entity_id, metric, value, unit, period, assertion_id))
            conn.commit()
        finally:
            conn.close()

    def get_graph_data(self):
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            
            cursor.execute("SELECT id, name as label, type, color, attributes, aliases FROM entity_master")
            nodes = []
            for row in cursor.fetchall():
                node = dict(row)
                node['attributes'] = json.loads(node['attributes'])
                node['aliases'] = json.loads(node['aliases'])
                
                # Use %s for Postgres, ? for SQLite
                param = "%s" if self.is_postgres else "?"
                cursor.execute(f"""
                    SELECT status, confidence, source_text, document_name, section_ref, source_authority 
                    FROM assertions 
                    WHERE subject_id = {param} AND subject_type = 'ENTITY' 
                    ORDER BY timestamp DESC LIMIT 3
                """, (node['id'],))
                node['evidence'] = [dict(r) for r in cursor.fetchall()]
                
                cursor.execute(f"""
                    SELECT q.metric, q.value, q.unit, q.period, a.source_authority
                    FROM quant_data q
                    JOIN assertions a ON q.source_assertion_id = a.id
                    WHERE q.entity_id = {param}
                    ORDER BY a.source_authority DESC, a.timestamp DESC
                """, (node['id'],))
                
                all_metrics = [dict(r) for r in cursor.fetchall()]
                consensus_metrics = {}
                for m in all_metrics:
                    key = f"{m['metric']}_{m['period']}"
                    if key not in consensus_metrics:
                        consensus_metrics[key] = m
                node['quant_metrics'] = list(consensus_metrics.values())
                nodes.append(node)

            cursor.execute("SELECT id, source_id as source, target_id as target, relation FROM relation_master")
            links = []
            for row in cursor.fetchall():
                link = dict(row)
                param = "%s" if self.is_postgres else "?"
                cursor.execute(f"""
                    SELECT status, confidence, source_text, document_name, section_ref 
                    FROM assertions 
                    WHERE subject_id = {param} AND subject_type = 'RELATION' 
                    ORDER BY timestamp DESC LIMIT 3
                """, (link['id'],))
                link['evidence'] = [dict(r) for r in cursor.fetchall()]
                links.append(link)

            return {"nodes": nodes, "links": links}
        finally:
            conn.close()

    def seed_ontology(self):
        from models import EntityType, RelationType, ALLOWED_RELATION_TRIPLES, ENTITY_TYPE_COLORS
        
        entity_types = [e.value for e in EntityType]
        self.update_ontology("entity_types", entity_types)
        
        relation_types = [r.value for r in RelationType]
        self.update_ontology("relation_types", relation_types)
        
        allowed_triples = [{"source": s.value, "relation": r.value, "target": t.value} for s, r, t in ALLOWED_RELATION_TRIPLES]
        self.update_ontology("allowed_triples", allowed_triples)
        
        self.update_ontology("entity_colors", ENTITY_TYPE_COLORS)
        
        rules = [
            "ROOT ENTITY: identify the primary company as LegalEntity (ROOT).",
            "NO ORPHANS: Every node must connect to ROOT directly or indirectly.",
            "MANAGEMENT CHAIN: LegalEntity -> HAS_MANAGEMENT -> Management -> HAS_ROLE -> Role -> HELD_BY -> Person.",
            "SUCCESSION: If one Person replaces another, use [Person A] -> SUCCEEDS -> [Person B].",
            "GEOGRAPHY: Region -> Country -> Site hierarchy.",
            "QUANT DATA: DO NOT create nodes for Revenue, PAT, Assets, etc. These MUST only be in 'quant_data'.",
            "BUSINESS UNITS: Key divisions (e.g. Wealth Management) are BusinessUnit nodes."
        ]
        self.update_ontology("extraction_rules", rules)
        logger.info("Database ontology seeded successfully.")
