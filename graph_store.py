"""
Zone 1 — In-Memory Graph Store
================================
Manages the canonical graph state: entities, relations, assertions.
Provides resolution (alias-based dedup), triple validation, and diff tracking
so the frontend can animate only what changed after each extraction.
"""

from __future__ import annotations

import logging
import re
import uuid
import json
import difflib
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

from models import (
    EntityType, RelationType, ReviewState,
    EntityMaster, EntityAssertion,
    RelationMaster, RelationAssertion,
    EntityCandidate, RelationCandidate,
    EvidenceRef, ExtractionPayload,
    validate_relation_triple,
    ZONE1_ONTOLOGY_VERSION,
    ENTITY_TYPE_COLORS,
    GoldenChunk,
)


# ────────────────────────────────────────────────────────────────────────
# ID GENERATION — deterministic, human-readable canonical IDs
# ────────────────────────────────────────────────────────────────────────

# Prefix map for entity types
_TYPE_PREFIX: dict[EntityType, str] = {
    EntityType.LEGAL_ENTITY:          "le",
    EntityType.EXTERNAL_ORGANIZATION: "ext",
    EntityType.BUSINESS_UNIT:         "bu",
    EntityType.SECTOR:                "sec",
    EntityType.INDUSTRY:              "ind",
    EntityType.SUB_INDUSTRY:          "subind",
    EntityType.END_MARKET:            "em",
    EntityType.CHANNEL:               "ch",
    EntityType.PRODUCT_DOMAIN:        "pd",
    EntityType.PRODUCT_FAMILY:        "pf",
    EntityType.PRODUCT_LINE:          "pl",
    EntityType.SITE:                  "site",
    EntityType.GEOGRAPHY:             "geo",
    EntityType.PERSON:                "person",
    EntityType.ROLE:                  "role",
    EntityType.TECHNOLOGY:            "tech",
    EntityType.CAPABILITY:            "cap",
    EntityType.PROGRAM:               "prog",
    EntityType.MANAGEMENT:            "mgmt",
    EntityType.COMPETITORS:           "comps",
}


def _slugify(text: str) -> str:
    """Convert text to a lowercase slug: letters, digits, underscores only."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text


def make_entity_id(entity_type: EntityType, canonical_name: str) -> str:
    """Generate a deterministic canonical ID like 'le_uno_minda_limited'."""
    prefix = _TYPE_PREFIX.get(entity_type, "ent")
    slug = _slugify(canonical_name)
    return f"{prefix}_{slug}"


def make_relation_id(source_id: str, relation_type: RelationType, target_id: str) -> str:
    """Generate a deterministic relation ID."""
    return f"rel_{_slugify(source_id)}__{relation_type.value.lower()}__{_slugify(target_id)}"


# ────────────────────────────────────────────────────────────────────────
# GRAPH STORE
# ────────────────────────────────────────────────────────────────────────

from database import DatabaseManager
from validators import LogicGuard
from inference import GraphInference

class GraphStore:
    """
    Persistent graph store for Zone 1 entities and relations using SQLite.
    Integrates LogicGuard for validation and supports Trust/Quant/Discovery layers.
    """

    def __init__(self, db_path: str = "graph.db"):
        self.db = DatabaseManager(db_path)
        self.ontology = self.db.get_ontology()
        self.guard = LogicGuard(self.ontology)
        
        # In-memory indices for speed
        self._alias_index: dict[str, str] = {}
        self._refresh_alias_index()

        # Diff tracking — IDs added in the most recent extraction
        self._last_new_entity_ids: set[str] = set()
        self._last_new_relation_ids: set[str] = set()

    def _refresh_alias_index(self):
        """Builds the alias-to-ID mapping from the database."""
        with self.db._get_connection() as conn:
            if self.db.is_postgres:
                import psycopg2.extras
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                cursor = conn.cursor()
            cursor.execute("SELECT id, name, aliases FROM entity_master")
            for row in cursor.fetchall():
                entity_id = row['id']
                self._alias_index[self._normalize_name(row['name'])] = entity_id
                aliases = json.loads(row['aliases'])
                for alias in aliases:
                    self._alias_index[self._normalize_name(alias)] = entity_id

    def _normalize_name(self, name: str) -> str:
        text = name.lower()
        text = re.sub(r'\b(inc\.|inc|corp\.|corp|llc\.|llc|ag\.|ag|se\.|se|co\.|co|ltd\.|ltd|limited)\b', '', text)
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def resolve_entity(self, canonical_name: str, aliases: list[str]) -> Optional[str]:
        # 1. Exact or Alias Match (Normalized)
        key = self._normalize_name(canonical_name)
        if key in self._alias_index:
            return self._alias_index[key]
        
        for alias in aliases:
            akey = self._normalize_name(alias)
            if akey in self._alias_index:
                return self._alias_index[akey]
        
        # 2. Fuzzy Match (Fallback)
        # We check all existing normalized names in the index
        existing_keys = list(self._alias_index.keys())
        if not existing_keys:
            return None
            
        matches = difflib.get_close_matches(key, existing_keys, n=1, cutoff=0.85)
        if matches:
            matched_key = matches[0]
            logger.info(f"Fuzzy Match Found: '{canonical_name}' resolved to existing entity via '{matched_key}'")
            return self._alias_index[matched_key]
            
        return None

    def ingest_extraction(self, payload: ExtractionPayload, source_authority: int = 5) -> dict[str, Any]:
        """
        Processes the full payload, resolves masters, tracks evidence, 
        and handles automated ontology discovery.
        """
        # 1. Logic Guards
        warnings = self.guard.validate_extraction(payload)
        
        new_entity_ids = set()
        new_relation_ids = set()
        temp_id_to_master = {}

        # 2. Process Entities
        for candidate in payload.entities:
            master_id = self.resolve_entity(candidate.canonical_name, candidate.aliases)
            
            if not master_id:
                master_id = f"ent_{_slugify(candidate.canonical_name)}_{uuid.uuid4().hex[:4]}"
                new_entity_ids.add(master_id)
            
            # Upsert into DB
            self.db.upsert_entity(
                entity_id=master_id,
                name=candidate.canonical_name,
                entity_type=candidate.entity_type,
                aliases=candidate.aliases,
                attributes=candidate.attributes
            )
            temp_id_to_master[candidate.temp_id] = master_id
            
            # Record Assertion (Evidence)
            status = 'FLAGGED' if any(master_id in w for w in warnings) else 'VERIFIED' if candidate.confidence > 0.9 else 'PENDING'
            self.db.add_assertion(
                subject_id=master_id,
                subject_type='ENTITY',
                source_text=candidate.source_text or "",
                confidence=candidate.confidence,
                status=status,
                document_name=payload.source_document_name,
                section_ref="chunk",
                source_authority=source_authority
            )
            
            # Update local index with canonical name AND all aliases
            self._alias_index[self._normalize_name(candidate.canonical_name)] = master_id
            for alias in candidate.aliases:
                self._alias_index[self._normalize_name(alias)] = master_id

        # 3. Process Relations
        for rel in payload.relations:
            src_master = temp_id_to_master.get(rel.source_temp_id)
            tgt_master = temp_id_to_master.get(rel.target_temp_id)
            
            if not src_master or not tgt_master:
                continue
                
            rel_id = make_relation_id(src_master, rel.relation_type, tgt_master)
            self.db.add_relation(rel_id, src_master, tgt_master, rel.relation_type)
            new_relation_ids.add(rel_id)
            
            # Record Assertion
            status = 'VERIFIED' if rel.confidence > 0.9 else 'PENDING'
            self.db.add_assertion(rel_id, 'RELATION', rel.source_text or "", rel.confidence, payload.source_document_name, "chunk", status, source_authority)

        # 4. Process Quant Data
        for q in payload.quant_data:
            master_id = temp_id_to_master.get(q.subject_id)
            if master_id:
                self.db.add_quant_metric(master_id, q.metric, q.value, q.unit, q.period)

        # 5. Automated Discovery Loop
        self._process_discoveries(payload.discoveries)

        self._last_new_entity_ids = new_entity_ids
        self._last_new_relation_ids = new_relation_ids

        return {
            "new_entity_ids": list(new_entity_ids),
            "new_relation_ids": list(new_relation_ids),
            "warnings": warnings,
            "total_entities": len(self._alias_index)
        }

    def _process_discoveries(self, discoveries: list[OntologyDiscovery]):
        """Adds new high-confidence types and triples to the ontology."""
        ontology_updated = False
        current_ontology = self.db.get_ontology()
        
        for d in discoveries:
            if d.type == 'ENTITY':
                if d.suggested_label not in current_ontology.get('entity_types', []):
                    current_ontology['entity_types'].append(d.suggested_label)
                    ontology_updated = True
                    logger.info(f"Auto-Discovery: Added new EntityType '{d.suggested_label}'")
            
            elif d.type == 'RELATION' and d.source_type and d.target_type:
                # 1. Add Relation Type if new
                if d.suggested_label not in current_ontology.get('relation_types', []):
                    current_ontology['relation_types'].append(d.suggested_label)
                    ontology_updated = True
                    logger.info(f"Auto-Discovery: Added new RelationType '{d.suggested_label}'")
                
                # 2. Add specific triple to allowed_triples
                triple = {
                    "source": d.source_type,
                    "relation": d.suggested_label,
                    "target": d.target_type
                }
                if triple not in current_ontology.get('allowed_triples', []):
                    current_ontology['allowed_triples'].append(triple)
                    ontology_updated = True
                    logger.info(f"Auto-Discovery: Registered new Triple {triple['source']} -> {triple['relation']} -> {triple['target']}")

        if ontology_updated:
            self.db.update_ontology('entity_types', current_ontology['entity_types'])
            self.db.update_ontology('relation_types', current_ontology['relation_types'])
            self.db.update_ontology('allowed_triples', current_ontology['allowed_triples'])
            self.ontology = current_ontology # Refresh in-memory rules
            self.guard = LogicGuard(self.ontology) # Refresh guard rules

    def get_graph_data(self) -> dict[str, Any]:
        data = self.db.get_graph_data()
        
        # Add inferred links
        inference_engine = GraphInference(data['nodes'], data['links'])
        inferred_links = inference_engine.infer_all()
        data['links'].extend(inferred_links)
        
        for node in data['nodes']:
            node['is_new'] = node['id'] in self._last_new_entity_ids
            colors = self.ontology.get('entity_colors', {})
            node['color'] = colors.get(node['type'], "#cccccc")
            
        for link in data['links']:
            link['is_new'] = link.get('id') in self._last_new_relation_ids
            
        data['stats'] = {
            "total_entities": len(data['nodes']),
            "total_relations": len(data['links']),
            "inferred_relations": len(inferred_links)
        }
        return data

    def get_extraction_log(self) -> list:
        return []

    def reset(self):
        """Resets the DB and re-seeds it without deleting the file (avoids Windows locking)."""
        self.db.drop_all_tables()
        self.db._init_db()
        from seed_db import seed
        seed()
        self._refresh_alias_index()
        self._last_new_entity_ids.clear()
        self._last_new_relation_ids.clear()
        self.ontology = self.db.get_ontology()
        self.guard = LogicGuard(self.ontology)

class IngestionStore:
    """
    Manages the ingestion of Golden Chunks and document metadata.
    """
    def __init__(self):
        self.documents: dict[str, dict[str, Any]] = {}  # doc_id -> metadata
        self.chunks: dict[str, list[GoldenChunk]] = {} # doc_id -> list of chunks

    def add_document(self, doc_id: str, metadata: dict[str, Any]):
        self.documents[doc_id] = metadata
        self.chunks[doc_id] = []

    def add_chunk(self, doc_id: str, chunk: GoldenChunk):
        if doc_id in self.chunks:
            self.chunks[doc_id].append(chunk)

    def get_document_chunks(self, doc_id: str) -> list[GoldenChunk]:
        return self.chunks.get(doc_id, [])

    def reset(self):
        self.documents.clear()
        self.chunks.clear()
