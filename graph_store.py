import re
import json
import logging
from typing import List, Dict, Any, Optional
from models import (
    ExtractionPayload,
    EntityCandidate,
    RelationCandidate,
    QuantMetric,
    EvidenceRef,
)

# ────────────────────────────────────────────────────────────────────────
# ID GENERATION — deterministic, human-readable canonical IDs
# ────────────────────────────────────────────────────────────────────────

_TYPE_PREFIX: dict[str, str] = {
    "LegalEntity":          "le",
    "ExternalOrganization": "ext",
    "BusinessUnit":         "bu",
    "Sector":               "sec",
    "Industry":             "ind",
    "SubIndustry":          "subind",
    "EndMarket":            "em",
    "Channel":              "ch",
    "ProductDomain":        "pd",
    "ProductFamily":        "pf",
    "ProductLine":          "pl",
    "Site":                 "site",
    "Geography":            "geo",
    "Person":               "person",
    "Role":                 "role",
    "Technology":           "tech",
    "Capability":           "cap",
    "Program":              "prog",
    "Management":           "mgmt",
    "Competitors":          "comps",
}


def _slugify(text: str) -> str:
    """Convert text to a lowercase slug: letters, digits, underscores only."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text


def make_entity_id(entity_type: str, canonical_name: str) -> str:
    """Generate a deterministic canonical ID like 'le_uno_minda_limited'."""
    prefix = _TYPE_PREFIX.get(entity_type, "ent")
    slug = _slugify(canonical_name)
    return f"{prefix}_{slug}"


def make_relation_id(source_id: str, relation_type: str, target_id: str) -> str:
    """Generate a deterministic relation ID."""
    return f"rel_{_slugify(source_id)}__{_slugify(relation_type)}__{_slugify(target_id)}"


# ────────────────────────────────────────────────────────────────────────
# GRAPH STORE
# ────────────────────────────────────────────────────────────────────────

from database import DatabaseManager
from validators import LogicGuard
from inference import GraphInference

class GraphStore:
    """
    Persistent graph store for Zone 1 entities and relations using Neon Postgres.
    Integrates LogicGuard for validation and supports Trust/Quant/Discovery layers.
    """

    def __init__(self):
        self.db = DatabaseManager()
        self.ontology = self.db.get_ontology()
        self.guard = LogicGuard(self.ontology)
        self._alias_index = {} # name_slug -> entity_id
        self._refresh_alias_index()

    def _refresh_alias_index(self):
        """Builds the alias-to-ID mapping from the database."""
        conn = self.db._get_connection()
        try:
            cursor = self.db._get_cursor(conn)
            cursor.execute("SELECT id, name, aliases FROM entity_master")
            for row in cursor.fetchall():
                entity_id = row['id']
                self._alias_index[self._normalize_name(row['name'])] = entity_id
                aliases = json.loads(row['aliases'])
                for alias in aliases:
                    self._alias_index[self._normalize_name(alias)] = entity_id
        finally:
            conn.close()

    def _normalize_name(self, name: str) -> str:
        text = name.lower()
        text = re.sub(r'\b(inc\.|inc|corp\.|corp|llc\.|llc|ag\.|ag|se\.|se|co\.|co|ltd\.|ltd|limited)\b', '', text)
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def ingest_extraction(self, payload: ExtractionPayload, source_authority: int = 5):
        """Main entry point for processing LLM extraction results."""
        id_map = {} # temp_id -> canonical_id
        
        for entity in payload.entities:
            can_id = self.resolve_entity(entity)
            id_map[entity.temp_id] = can_id
            
            # Fetch color from ontology
            ont_colors = self.ontology.get('entity_colors', {})
            ent_color = ont_colors.get(entity.entity_type, "#3b82f6") # Default fallback blue

            self.db.upsert_entity(
                entity_id=can_id,
                name=entity.canonical_name,
                entity_type=entity.entity_type,
                color=ent_color,
                attributes=entity.attributes,
                aliases=entity.aliases
            )
            
            self.db.add_assertion(
                subject_id=can_id,
                subject_type='ENTITY',
                source_text=entity.source_text or "",
                confidence=entity.confidence,
                document_name=payload.source_document_name,
                section_ref=entity.evidence[0].section_ref if entity.evidence else "extract",
                source_authority=source_authority
            )

        for rel in payload.relations:
            src_id = id_map.get(rel.source_temp_id)
            tgt_id = id_map.get(rel.target_temp_id)
            
            if src_id and tgt_id:
                rel_id = make_relation_id(src_id, rel.relation_type, tgt_id)
                self.db.add_relation(rel_id, src_id, tgt_id, rel.relation_type)
                
                self.db.add_assertion(
                    subject_id=rel_id,
                    subject_type='RELATION',
                    source_text=rel.source_text or "",
                    confidence=rel.confidence,
                    document_name=payload.source_document_name,
                    section_ref=rel.evidence[0].section_ref if rel.evidence else "extract",
                    source_authority=source_authority
                )

        for q in payload.quant_data:
            subj_id = id_map.get(q.subject_id)
            if subj_id:
                assertion_id = self.db.add_assertion(
                    subject_id=subj_id,
                    subject_type='QUANT',
                    source_text=f"Extracted {q.metric}: {q.value} {q.unit or ''}",
                    confidence=0.9,
                    document_name=payload.source_document_name,
                    section_ref="quant_extract",
                    source_authority=source_authority
                )
                
                self.db.add_quant_metric(
                    entity_id=subj_id,
                    metric=q.metric,
                    value=q.value,
                    unit=q.unit,
                    period=q.period,
                    assertion_id=assertion_id
                )

        self._process_discoveries(payload.discoveries)
        self._refresh_alias_index()
        return {"entities_processed": len(payload.entities), "relations_processed": len(payload.relations)}

    def resolve_entity(self, entity: EntityCandidate) -> str:
        name_slug = self._normalize_name(entity.canonical_name)
        if name_slug in self._alias_index:
            return self._alias_index[name_slug]
            
        for alias in entity.aliases:
            alias_slug = self._normalize_name(alias)
            if alias_slug in self._alias_index:
                return self._alias_index[alias_slug]
                
        return make_entity_id(entity.entity_type, entity.canonical_name)

    def _process_discoveries(self, discoveries):
        cur_ont = self.db.get_ontology()
        entities = set(cur_ont.get('entity_types', []))
        relations = set(cur_ont.get('relation_types', []))
        triples = cur_ont.get('allowed_triples', [])
        
        updated = False
        for d in discoveries:
            is_new = False
            if d.type == 'ENTITY' and d.suggested_label not in entities:
                entities.add(d.suggested_label)
                is_new = True
            elif d.type == 'RELATION' and d.suggested_label not in relations:
                relations.add(d.suggested_label)
                is_new = True
                if d.source_type and d.target_type:
                    triple = {"source": d.source_type, "relation": d.suggested_label, "target": d.target_type}
                    if triple not in triples:
                        triples.append(triple)
            
            if is_new:
                updated = True
                self.db.add_discovery(d)
        
        if updated:
            self.db.update_ontology('entity_types', list(entities))
            self.db.update_ontology('relation_types', list(relations))
            self.db.update_ontology('allowed_triples', triples)
            self.ontology = self.db.get_ontology()
            self.guard = LogicGuard(self.ontology)

    def get_full_graph(self, filter_status: str = 'ACCEPTED'):
        data = self.db.get_graph_data()
        engine = GraphInference(data['nodes'], data['links'])
        inferred_links = engine.infer_all()
        data['links'].extend(inferred_links)
        
        # Add stats for the frontend counters
        data['stats'] = {
            "total_entities": len(data['nodes']),
            "total_relations": len(data['links'])
        }
        return data

    def reset(self):
        """Wipes graph data while PRESERVING ontology and learned discoveries."""
        self.db.clear_graph_data()
        self.db._init_db()  # Re-create the dropped graph tables
        self.db.seed_ontology(merge_with_existing=True) 
        self.ontology = self.db.get_ontology()
        self.guard = LogicGuard(self.ontology)
        self._alias_index = {}
        self._refresh_alias_index()

    def get_extraction_log(self):
        """Fetches the history of assertions for the UI log."""
        conn = self.db._get_connection()
        try:
            cursor = self.db._get_cursor(conn)
            cursor.execute("""
                SELECT id, subject_id, subject_type, source_text, confidence, document_name, timestamp 
                FROM assertions 
                ORDER BY timestamp DESC LIMIT 50
            """)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
