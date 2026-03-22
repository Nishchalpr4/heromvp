import json
import logging
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# We strictly require psycopg2 for Postgres/Neon
import psycopg2
import psycopg2.extras
import psycopg2.errors

# Import RealDictCursor for convenience
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    CENTRAL KNOWLEDGE ENGINE: Manages all interactions with Neon Postgres.
    It handles schema creation, persistent ontology storage, and graph data access.
    """
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        
        if not self.db_url:
            logger.error("DATABASE_URL not found. Database operations will fail.")
            raise ValueError("DATABASE_URL environment variable is required.")
        
        if not self.db_url.startswith("postgres"):
            raise ValueError("DATABASE_URL must be a valid PostgreSQL connection string starting with 'postgres://'.")

        # ── CONNECTION POOLING (Critical for Render/Neon Free Tier) ──
        # ThreadedConnectionPool is safer for FastAPI's concurrency.
        try:
            from psycopg2 import pool
            
            # Add connection timeout to prevent hanging on bad network
            if "?" in self.db_url:
                dsn = f"{self.db_url}&connect_timeout=10"
            else:
                dsn = f"{self.db_url}?connect_timeout=10"
                
            self.pool = pool.ThreadedConnectionPool(
                1, 20, # min, max connections
                dsn=dsn,
                sslmode='require' # Required for Neon
            )
            logger.info("Neon Postgres Threaded Connection Pool initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize Connection Pool: {e}")
            raise

        self._init_db()

    def _get_connection(self):
        """Retrieves a healthy connection from the pool (implements pre-ping)."""
        conn = self.pool.getconn()
        try:
            # Simple health check (pre-ping)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning(f"Stale connection detected, replacing: {e}")
            self.pool.putconn(conn, close=True) # Close the dead one
            return self.pool.getconn() # Get a fresh one

    def _get_cursor(self, conn):
        """Standardizes on DictCursor for robust row access."""
        return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def _release_connection(self, conn):
        """Returns a connection back to the pool."""
        self.pool.putconn(conn)

    def _init_db(self):
        """Initializes the Neon Postgres schema."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # 1. Entity Master
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entity_master (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    color TEXT,
                    attributes TEXT, -- JSON string
                    aliases TEXT,    -- JSON string
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
                    FOREIGN KEY(source_id) REFERENCES entity_master(id) ON DELETE CASCADE,
                    FOREIGN KEY(target_id) REFERENCES entity_master(id) ON DELETE CASCADE
                )
            """)

            # 3. Assertions (Evidence)
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
            
            # 4. Quant Data (Metrics)
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
                    FOREIGN KEY(entity_id) REFERENCES entity_master(id) ON DELETE CASCADE,
                    FOREIGN KEY(source_assertion_id) REFERENCES assertions(id) ON DELETE CASCADE
                )
            """)

            # 5. Ontology Rules (Dynamic Config)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ontology_rules (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 6. Entity Type Discoveries
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS new_entity_types (
                    id SERIAL PRIMARY KEY,
                    suggested_label TEXT NOT NULL UNIQUE,
                    rationale TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 7. Relation Type Discoveries
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS new_relation_types (
                    id SERIAL PRIMARY KEY,
                    suggested_label TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    rationale TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            logger.info("Neon Postgres Database initialized successfully.")

            # Auto-seed if empty
            cursor.execute("SELECT count(*) FROM ontology_rules")
            if cursor.fetchone()[0] == 0:
                logger.info("Ontology is empty. Auto-seeding from base_ontology.json...")
                self.seed_ontology()
        finally:
            self._release_connection(conn)

    def clear_graph_data(self):
        """
        SURGICAL RESET: Wipes the 'drawn' graph (nodes/links) while 
        keeping the AI's 'knowledge' (ontology/discoveries) intact.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS entity_master CASCADE")
            cursor.execute("DROP TABLE IF EXISTS relation_master CASCADE")
            cursor.execute("DROP TABLE IF EXISTS assertions CASCADE")
            cursor.execute("DROP TABLE IF EXISTS quant_data CASCADE")
            conn.commit()
            logger.warning("Graph data tables cleared. (Ontology and Discoveries preserved)")
        finally:
            self._release_connection(conn)

    def danger_full_wipe(self):
        """
        NUCLEAR RESET: Wipes EVERYTHING, including learned types and rules.
        Use only for catastrophic recovery or total project resets.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS entity_master CASCADE")
            cursor.execute("DROP TABLE IF EXISTS relation_master CASCADE")
            cursor.execute("DROP TABLE IF EXISTS assertions CASCADE")
            cursor.execute("DROP TABLE IF EXISTS quant_data CASCADE")
            cursor.execute("DROP TABLE IF EXISTS ontology_rules CASCADE")
            cursor.execute("DROP TABLE IF EXISTS new_entity_types CASCADE")
            cursor.execute("DROP TABLE IF EXISTS new_relation_types CASCADE")
            conn.commit()
            logger.warning("All Neon Postgres tables dropped (FULL WIPE).")
        finally:
            self._release_connection(conn)

    def get_ontology(self):
        """FETCH RULES: Returns the current AI configuration (types/colors/logic)."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            cursor.execute("SELECT key, data FROM ontology_rules")
            rows = cursor.fetchall()
            return {row['key']: json.loads(row['data']) for row in rows}
        finally:
            self._release_connection(conn)

    def update_ontology(self, key: str, data: list | dict, merge: bool = False):
        """
        LEARNING ENGINE: Persists new entity/relation types. 
        If merge=True, it intelligently deduplicates and combines with existing knowledge.
        """
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            
            final_data = data
            if merge:
                cursor.execute("SELECT data FROM ontology_rules WHERE key = %s", (key,))
                row = cursor.fetchone()
                if row:
                    current_data = json.loads(row['data'])
                    if isinstance(current_data, list) and isinstance(data, list):
                        # Merge lists, unique entries only (handle non-hashable dicts)
                        if any(isinstance(x, dict) for x in current_data + data):
                            # Specialized merge for lists of dicts (like allowed_triples)
                            combined = current_data + data
                            seen = set()
                            unique_list = []
                            for item in combined:
                                # Serialize to unique string for hashing
                                s = json.dumps(item, sort_keys=True)
                                if s not in seen:
                                    seen.add(s)
                                    unique_list.append(item)
                            final_data = unique_list
                        else:
                            # Standard set merge for hashable items (strings)
                            final_data = list(set(current_data + data))
                    elif isinstance(current_data, dict) and isinstance(data, dict):
                        # Merge dicts
                        final_data = {**current_data, **data}

            cursor.execute("""
                INSERT INTO ontology_rules (key, data, last_updated)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, last_updated = CURRENT_TIMESTAMP
            """, (key, json.dumps(final_data)))
            conn.commit()
        finally:
            self._release_connection(conn)

    def upsert_entity(self, entity_id: str, name: str, entity_type: str, color: str = None, attributes: dict = None, aliases: list = None):
        """Upserts an entity into the master table."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
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
            conn.commit()
        finally:
            self._release_connection(conn)

    def add_relation(self, rel_id: str, source_id: str, target_id: str, relation: str):
        """Adds a unique relation link to Neon."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            cursor.execute("""
                INSERT INTO relation_master (id, source_id, target_id, relation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (rel_id, source_id, target_id, relation))
            conn.commit()
        finally:
            self._release_connection(conn)

    def add_assertion(self, subject_id: str, subject_type: str, source_text: str, confidence: float, document_name: str, section_ref: str, status: str = 'PENDING', source_authority: int = 5):
        """Adds an evidence assertion and returns the auto-generated SERIAL ID."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            cursor.execute("""
                INSERT INTO assertions (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (subject_id, subject_type, source_text, confidence, status, document_name, section_ref, source_authority))
            row = cursor.fetchone()
            assertion_id = row['id']
            conn.commit()
            return assertion_id
        finally:
            self._release_connection(conn)

    def add_quant_metric(self, entity_id: str, metric: str, value: float, unit: str, period: str, assertion_id: int = None):
        """Adds a quantitative metric row to Neon."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            cursor.execute("""
                INSERT INTO quant_data (entity_id, metric, value, unit, period, source_assertion_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (entity_id, metric, value, unit, period, assertion_id))
            conn.commit()
        finally:
            self._release_connection(conn)

    def add_discovery(self, d):
        """Logs a newly discovered entity or relation type into its respective distinct table."""
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            if d.type == 'ENTITY':
                cursor.execute("""
                    INSERT INTO new_entity_types (suggested_label, rationale)
                    VALUES (%s, %s)
                    ON CONFLICT (suggested_label) DO NOTHING
                """, (d.suggested_label, getattr(d, 'rationale', None)))
            elif d.type == 'RELATION' and getattr(d, 'source_type', None) and getattr(d, 'target_type', None):
                cursor.execute("""
                    INSERT INTO new_relation_types (suggested_label, source_type, target_type, rationale)
                    VALUES (%s, %s, %s, %s)
                """, (d.suggested_label, d.source_type, d.target_type, getattr(d, 'rationale', None)))
            conn.commit()
        finally:
            self._release_connection(conn)

    def get_graph_data(self):
        """
        VIZ BRIDGE: Aggregates master entities, relations, recent evidence, 
        and consensus metrics into a single D3-ready JSON structure.
        """
        conn = self._get_connection()
        try:
            cursor = self._get_cursor(conn)
            
            cursor.execute("SELECT id, name as label, type, color, attributes, aliases FROM entity_master")
            nodes = []
            for row in cursor.fetchall():
                node = dict(row)
                node['attributes'] = json.loads(node['attributes'])
                node['aliases'] = json.loads(node['aliases'])
                
                # Fetch recent evidence
                cursor.execute("""
                    SELECT status, confidence, source_text, document_name, section_ref, source_authority 
                    FROM assertions 
                    WHERE subject_id = %s AND subject_type = 'ENTITY' 
                    ORDER BY timestamp DESC LIMIT 3
                """, (node['id'],))
                node['evidence'] = [dict(r) for r in cursor.fetchall()]
                
                # Fetch metrics
                cursor.execute("""
                    SELECT q.metric, q.value, q.unit, q.period, a.source_authority
                    FROM quant_data q
                    JOIN assertions a ON q.source_assertion_id = a.id
                    WHERE q.entity_id = %s
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
                cursor.execute("""
                    SELECT status, confidence, source_text, document_name, section_ref 
                    FROM assertions 
                    WHERE subject_id = %s AND subject_type = 'RELATION' 
                    ORDER BY timestamp DESC LIMIT 3
                """, (link['id'],))
                link['evidence'] = [dict(r) for r in cursor.fetchall()]
                links.append(link)

            return {"nodes": nodes, "links": links}
        finally:
            self._release_connection(conn)

    def seed_ontology(self, merge_with_existing: bool = True):
        """Centralized seeder: Reads base_ontology.json and writes to Neon.
        By default, it MERGES with existing rules so learned types aren't lost.
        """
        config_path = Path(__file__).parent / "base_ontology.json"
        if not config_path.exists():
            logger.warning("base_ontology.json not found. Skipping initial seed.")
            return

        with open(config_path, "r") as f:
            data = json.load(f)
            
        self.update_ontology("entity_types", data.get("entity_types", []), merge=merge_with_existing)
        self.update_ontology("relation_types", data.get("relation_types", []), merge=merge_with_existing)
        self.update_ontology("allowed_triples", data.get("allowed_triples", []), merge=merge_with_existing)
        self.update_ontology("entity_colors", data.get("entity_colors", {}), merge=merge_with_existing)
        self.update_ontology("extraction_rules", data.get("extraction_rules", []), merge=merge_with_existing)
        
        logger.info(f"Neon Postgres ontology {'merged' if merge_with_existing else 'seeded'} from base_ontology.json.")
