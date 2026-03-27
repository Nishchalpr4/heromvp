import re
import json
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)
from models import (
    ExtractionPayload,
    EntityCandidate,
    RelationCandidate,
    QuantMetric,
    EvidenceRef,
)
from validators import safe_json_loads

# ────────────────────────────────────────────────────────────────────────
# ID GENERATION — deterministic, human-readable canonical IDs
# ────────────────────────────────────────────────────────────────────────

_TYPE_PREFIX: dict[str, str] = {
    "LegalEntity":          "le",
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
    "Strategy":             "strat",
    "SupplyChain":          "sc",
    "ManufacturingNetwork": "mn",
    "FinancialReport":      "fin",
    "MarketForecast":       "mf",
    "ProductionInsight":    "pi"
}

# Mapping of common LLM-generated type variations to canonical ontology types
_TYPE_NORMALIZATION: dict[str, str] = {
    "company": "LegalEntity",
    "corporation": "LegalEntity",
    "main company": "LegalEntity",
    "parent company": "LegalEntity",
    "product": "ProductLine",
    "service": "ProductLine", # Services are often modeled as ProductLine in this schema
    "offering": "ProductLine",
    "product domain": "ProductDomain",
    "business unit": "BusinessUnit",
    "segment": "BusinessUnit",
    "portfolio": "ProductPortfolio",
    "group": "ProductPortfolio",
    "person": "Person",
    "executive": "Person",
    "geography": "Geography",
    "location": "Geography",
    "country": "Geography"
}

def normalize_entity_type(etype: str) -> str:
    """Normalizes informal type names to canonical ontology keys."""
    raw = etype.strip().lower()
    return _TYPE_NORMALIZATION.get(raw, etype) # Return original if no mapping found


_DEDUPE_MAP: dict[str, str] = {
    "america": "united states",
    "american": "united states",
    "u s": "united states",
    "us": "united states",
    "nvidia": "nvidia corporation",
    "graphics and compute": "graphics and compute processors",
    "digital services": "services",
    "consumer electronic": "consumer electronics",
}

def normalize_name(name: str) -> str:
    """Canonical normalization for both ID generation and resolution."""
    text = name.lower()
    # Remove standard corporate suffixes
    text = re.sub(r'\b(inc\.|inc|corp\.|corp|llc\.|llc|ag\.|ag|se\.|se|co\.|co|ltd\.|ltd|limited)\b', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Apply deduplication map
    return _DEDUPE_MAP.get(text, text)

def _slugify(text: str) -> str:
    """Convert text to a lowercase slug: letters, digits, underscores only."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text


def make_entity_id(entity_type: str, canonical_name: str) -> str:
    """Generate a deterministic canonical ID using the normalized name."""
    prefix = _TYPE_PREFIX.get(entity_type, "ent")
    # USE THE FULL NORMALIZATION including dedupe map
    norm_name = normalize_name(canonical_name)
    slug = _slugify(norm_name)
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
    KNOWLEDGE MANAGER: The main orchestrator of the system.
    It takes raw LLM payloads, validates them against the ontology (LogicGuard),
    handles entity resolution (deduplication), and persists everything to Neon.
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
                self._alias_index[normalize_name(row['name'])] = entity_id
                aliases = safe_json_loads(row['aliases'], default=[])
                for alias in aliases:
                    self._alias_index[normalize_name(alias)] = entity_id
        finally:
            self.db._release_connection(conn)

    def ingest_extraction(self, payload: ExtractionPayload, source_authority: int = 5, metadata: dict = {}):
        """Main entry point for processing LLM extraction results."""
        # --- RELIABILITY: Self-healing pass ---
        payload = self.guard.refine_payload(payload)
        
        id_map = {} # temp_id -> canonical_id
        
        # CATEGORY UNIFICATION: Local alias index to handle de-duplication within the same payload
        # This prevents "Digital Services" and "Services" from being separate IDs if they appear in one batch.
        local_alias_index = self._alias_index.copy()

        # Identify the root entity if possible from metadata
        subject_name = (metadata.get("company_name") or "").strip()
        subject_id = None
        if subject_name:
            subject_id = make_entity_id("LegalEntity", subject_name)

        for entity in payload.entities:
            # Normalize type before resolution and ingestion
            entity.entity_type = normalize_entity_type(entity.entity_type)
            
            can_id = self.resolve_entity(entity, local_alias_index)
            id_map[entity.temp_id] = can_id

            # Update local index for subsequent entities in this same payload
            name_slug = normalize_name(entity.canonical_name)
            local_alias_index[name_slug] = can_id
            for alias in entity.aliases:
                local_alias_index[normalize_name(alias)] = can_id

            # Identify if this node represents the primary subject for anchoring
            is_subject = (can_id == subject_id)

            # Fetch color from ontology
            ont_colors = self.ontology.get('entity_colors', {})
            ent_color = ont_colors.get(entity.entity_type, "#3b82f6")

            # --- Evidence/source text fix ---
            # If entity.source_text is missing, use first evidence's evidence_quote if available
            source_text = (entity.source_text or "").strip()
            evidence_snippet = None
            if not source_text and entity.evidence and len(entity.evidence) > 0:
                first_ev = entity.evidence[0]
                # Try both 'evidence_quote' and 'source_text' for compatibility
                source_text = getattr(first_ev, 'evidence_quote', None) or getattr(first_ev, 'source_text', None) or ""
                evidence_snippet = source_text
            elif entity.evidence and len(entity.evidence) > 0:
                first_ev = entity.evidence[0]
                evidence_snippet = getattr(first_ev, 'evidence_quote', None) or getattr(first_ev, 'source_text', None) or ""
            # Add snippet to attributes for frontend display
            attributes = {**entity.attributes, "is_root": is_subject}
            if evidence_snippet:
                attributes["evidence_snippet"] = evidence_snippet

            self.db.upsert_entity(
                entity_id=can_id,
                name=entity.canonical_name,
                entity_type=entity.entity_type,
                color=ent_color,
                description=entity.description,
                short_info=entity.short_info,
                attributes=attributes,
                aliases=entity.aliases
            )

            self.db.add_assertion(
                subject_id=can_id,
                subject_type='ENTITY',
                source_text=source_text,
                confidence=entity.confidence,
                document_name=payload.source_document_name,
                section_ref=entity.evidence[0].section_ref if entity.evidence else "extract",
                source_authority=source_authority
            )

        # --- STRICT SINGLE-PARENT TREE ENFORCEMENT ---
        # 1. First Pass: Identify the primary taxonomic parent for each child node
        # These relations define the core tree hierarchy where each node should have only ONE parent.
        taxonomic_rels = [
            "HAS_MANAGEMENT", "HAS_STRATEGY", "HAS_SUPPLY_CHAIN", "HAS_NETWORK", 
            "HAS_PRODUCTS", "HAS_PRODUCT", "HAS_FINANCIALS", "HAS_MARKET_INSIGHT", "INCLUDES",
            "HAS_PRODUCT_PORTFOLIO", "HAS_SERVICE_PORTFOLIO", "HAS_PRODUCT_DOMAIN", "HAS_PRODUCT_FAMILY", "HAS_PRODUCT_LINE", 
            "HAS_BUSINESS_UNIT", "OFFERS",
            "HAS_ROLE", "HELD_BY", "COMPETES_WITH", "HAS_INITIATIVE", "LEADS", "REPORTED_BY"
        ]
        
        node_has_parent = set() # target_ids that already have an incoming parent link
        filtered_relations = []
        
        # Priority 1: Taxonomic Structure (Force a Tree)
        for rel in payload.relations:
            rel_type_norm = rel.relation_type.upper().replace(" ", "_")
            if rel_type_norm in taxonomic_rels:
                src_id = id_map.get(rel.source_temp_id)
                tgt_id = id_map.get(rel.target_temp_id)
                if not src_id or not tgt_id: continue
                
                # REJECT SELF-LOOPS
                if src_id == tgt_id:
                    print(f"[HIERARCHY] Rejecting self-loop for {src_id}")
                    continue
                
                # REJECT REDUNDANT PARENT
                existing_parent = self.db.get_node_parent(tgt_id, taxonomic_rels)
                if (tgt_id in node_has_parent or existing_parent) and (existing_parent != src_id):
                    print(f"[HIERARCHY] Rejecting DIFFERENT taxonomic link to {tgt_id} (Existing parent: {existing_parent}, New: {src_id})")
                    continue
                
                node_has_parent.add(tgt_id)
                filtered_relations.append(rel)

        # Priority 2: Associative Structure (Market links, Geography, etc.)
        for rel in payload.relations:
            rel_type_norm = rel.relation_type.upper().replace(" ", "_")
            if rel_type_norm not in taxonomic_rels:
                src_id = id_map.get(rel.source_temp_id)
                tgt_id = id_map.get(rel.target_temp_id)
                if not src_id or not tgt_id: continue

                # User Rule: "sub node only connect to main node above it ntg else"
                if rel_type_norm == "APPLIES_TO_END_MARKET" and (src_id in node_has_parent or self.db.node_has_parent(src_id, taxonomic_rels)):
                    print(f"[HIERARCHY] Rejecting redundant Market link from {src_id} -> {tgt_id}")
                    continue
                
                # General Tree Safety: Only allow one parent for ANY node in this minimalist view
                # General Tree Safety: Only allow one parent for ANY node in this minimalist view
                existing_parent = self.db.get_node_parent(tgt_id, taxonomic_rels)
                if (tgt_id in node_has_parent or existing_parent) and (existing_parent != src_id):
                    # We allow multiple outgoing links for some things (like Company -> Capabilities), 
                    # but the user said "sub node only connect to main node ABOVE it".
                    # Let's be aggressive for now.
                    if rel_type_norm in ["HAS_CAPABILITY", "OPERATES_IN", "COMPETES_WITH"]:
                        filtered_relations.append(rel) # Allow multiple capabilities/geos
                    else:
                        print(f"[HIERARCHY] Rejecting second DIFFERENT incoming link to {tgt_id} ({rel_type_norm})")
                        continue
                else:
                    node_has_parent.add(tgt_id)
                    filtered_relations.append(rel)

        # 2. Add filtered relations and assertions
        for rel in filtered_relations:
            src_id = id_map.get(rel.source_temp_id)
            tgt_id = id_map.get(rel.target_temp_id)
            if not src_id or not tgt_id: continue
            rel_id = make_relation_id(src_id, rel.relation_type, tgt_id)
            try:
                self.db.add_relation(rel_id, src_id, tgt_id, rel.relation_type)
            except Exception as e:
                print(f"Error adding relation {rel_id}: {e}")
                
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

        # Get taxonomic rules from ontology to avoid hardcoding
        struct_meta = self.ontology.get("structural_metadata", {})
        taxonomic_rels = struct_meta.get("taxonomic_rels", taxonomic_rels)

        self._enforce_structural_hierarchy(payload, id_map, taxonomic_rels, subject_id, source_authority, metadata)
        self._global_reanchor(taxonomic_rels, subject_id)
        self._process_discoveries(payload.discoveries)
        self._check_and_fix_roots()
        self._refresh_alias_index()
        return {"entities_processed": len(payload.entities), "relations_processed": len(payload.relations)}

    def _enforce_structural_hierarchy(self, payload: ExtractionPayload, id_map: dict, taxonomic_rels: List[str], subject_id: str, source_authority: int, metadata: dict = {}):
        """
        DYNAMIC HIERARCHY GUARD: 
        1. Anchors orphans to the root using bridge rules.
        2. Active Decluttering: Re-routes direct root-to-node links through bridges.
        3. Regional Nesting: Prefers Region -> Country links over Root -> Country.
        """
        if not subject_id:
            for ent in payload.entities:
                if ent.entity_type == "LegalEntity":
                    subject_id = id_map.get(ent.temp_id)
                    break
        
        if not subject_id: return 

        struct_meta = self.ontology.get("structural_metadata", {})
        bridge_rules = struct_meta.get("bridge_rules", {})
        comp_name = metadata.get("company_name", "Corporate").strip()

        # Step 1: Detect all current parent-child mappings in this payload
        parent_map = {} # target -> source
        for rel in payload.relations:
            src = id_map.get(rel.source_temp_id) or rel.source_id
            tgt = id_map.get(rel.target_temp_id) or rel.target_id
            if src and tgt and rel.relation_type.upper() in [r.upper() for r in taxonomic_rels]:
                parent_map[tgt] = src

        # Step 2: Enforce Rules
        for entity in payload.entities:
            can_id = id_map.get(entity.temp_id)
            if not can_id or can_id == subject_id: continue
            
            etype = entity.entity_type
            has_parent = self.db.node_has_parent(can_id, taxonomic_rels)
            current_parent = parent_map.get(can_id)

            # ORPHAN HEALING
            if not has_parent and not current_parent:
                self._apply_bridge_rule(can_id, etype, subject_id, bridge_rules, comp_name)
            
            # ACTIVE DECLUTTERING: If linked directly to root but a bridge rule exists, move it.
            elif current_parent == subject_id and etype in bridge_rules:
                # Exception: Geography already has a parent region in this payload? 
                # (e.g., LLM linked Nike -> Vietnam AND SE Asia -> Vietnam)
                # We skip re-bridging Geography if it already has a non-root taxonomic parent.
                is_nested = any(r.target_id == can_id and r.source_id != subject_id and r.relation_type.upper() in [tr.upper() for tr in taxonomic_rels] for r in payload.relations)
                
                if not is_nested:
                    print(f"[DECLUTTER] Re-routing root link {subject_id} -> {can_id} through {etype} bridge.")
                    self._apply_bridge_rule(can_id, etype, subject_id, bridge_rules, comp_name)

    def _apply_bridge_rule(self, can_id: str, etype: str, subject_id: str, bridge_rules: dict, comp_name: str):
        """Applies a bridge rule to a specific node, anchoring it correctly."""
        rule = bridge_rules.get(etype)
        if rule:
            # DYNAMIC BRIDGE CREATION
            bridge_type = rule["type"]
            bridge_name = f"{comp_name} {rule['suffix']}"
            bridge_id = make_entity_id(bridge_type, bridge_name)
            
            self.db.upsert_entity(
                entity_id=bridge_id,
                name=bridge_name,
                entity_type=bridge_type,
                color=self.ontology.get("entity_colors", {}).get(bridge_type, "#94a3b8"),
                description=f"Automated group for {comp_name}",
                attributes={"is_bridge": True}
            )
            
            # Subject -> Bridge
            b_rel_type = rule["bridge_rel"]
            b_rel_id = make_relation_id(subject_id, b_rel_type, bridge_id)
            self.db.add_relation(b_rel_id, subject_id, bridge_id, b_rel_type)
            
            # Bridge -> Target
            parent_id = bridge_id
            rel_type = rule["rel"]
            
            # Extra Safety: Geography -> LOCATED_IN -> Manufacturing Hubs? No, Bridge -> LOCATED_IN -> Geography.
            # The rel in rule is "from bridge to target".
        else:
            # Fallback context-free link
            parent_id = subject_id
            rel_type = "INCLUDES"
        
        # Apply relation
        rel_id = make_relation_id(parent_id, rel_type, can_id)
        self.db.add_relation(rel_id, parent_id, can_id, rel_type)
        return parent_id, rel_type

    def _global_reanchor(self, taxonomic_rels: List[str], subject_id: str):
        """
        CRITICAL REPAIR: Scan the entire DB for nodes that have ZERO incoming taxonomic links and anchor them 
        to the root. This handles historical orphans.
        """
        if not subject_id: return
        
        conn = self.db._get_connection()
        try:
            cursor = self.db._get_cursor(conn)
            # Find all entity IDs that have NO incoming taxonomic relations
            cursor.execute(f"""
                SELECT id, name, type 
                FROM entity_master 
                WHERE id != '{subject_id}'
                AND id NOT IN (
                    SELECT target_id FROM relation_master 
                    WHERE UPPER(relation) = ANY(%s)
                )
            """, ([r.upper() for r in taxonomic_rels],))
            
            orphans = cursor.fetchall()
            # Load bridge rules from ontology for dynamic anchoring
            struct_meta = self.ontology.get("structural_metadata", {})
            bridge_rules = struct_meta.get("bridge_rules", {})
            
            for row in orphans:
                oid = row['id']
                oname = row['name']
                otype = row['type']
                
                # Dynamic anchor via bridge rules
                rule = bridge_rules.get(otype)
                if rule:
                    parent_id, rel_type = self._apply_bridge_rule(oid, otype, subject_id, bridge_rules, "Corporate")
                else:
                    rel_type = "INCLUDES"
                    parent_id = subject_id
                    rid = make_relation_id(parent_id, rel_type, oid)
                    self.db.add_relation(rid, parent_id, oid, rel_type)
                
                print(f"[RE-ANCHOR] Fixed historical orphan node: {oname} ({oid}) via {rel_type}")
                
        finally:
            self.db._release_connection(conn)

    def _check_and_fix_roots(self):
        """
        ROOT RECONCILIATION: Ensures at least one LegalEntity is marked as 'is_root' 
        if no metadata was provided. This drives the D3 tree visualization.
        """
        conn = self.db._get_connection()
        try:
            cursor = self.db._get_cursor(conn)
            cursor.execute("SELECT id, name, type, attributes FROM entity_master")
            nodes = cursor.fetchall()
            
            root_exists = False
            legal_entities = []
            
            for node in nodes:
                attrs = safe_json_loads(node['attributes'], default={})
                if attrs.get('is_root'):
                    root_exists = True
                    break
                if node['type'] == 'LegalEntity':
                    legal_entities.append(node)
                    
            if not root_exists and legal_entities:
                # Heuristic: Pick the LegalEntity that has 'inc', 'corp', or 'root' in name
                target_root = legal_entities[0]
                for le in legal_entities:
                    name_lower = le['name'].lower()
                    if any(term in name_lower for term in ['inc', 'corp', 'limited']):
                        target_root = le
                        break
                
                attrs = safe_json_loads(target_root['attributes'], default={})
                attrs['is_root'] = True
                cursor.execute("UPDATE entity_master SET attributes = %s WHERE id = %s", 
                               (json.dumps(attrs), target_root['id']))
                conn.commit()
                logger.info(f"[ROOT REPAIR] Tagged {target_root['name']} as root node.")
        finally:
            self.db._release_connection(conn)

    def resolve_entity(self, entity: EntityCandidate, custom_index: dict = None) -> str:
        name_slug = normalize_name(entity.canonical_name)
        index = custom_index if custom_index is not None else self._alias_index

        # 1. Direct match in alias index (handles cross-type resolution since slugs are shared)
        if name_slug in index:
            return index[name_slug]
            
        # 2. Alias match
        for alias in entity.aliases:
            alias_slug = normalize_name(alias)
            if alias_slug in index:
                return index[alias_slug]
                
        # 3. CATEGORY UNIFICATION: Cross-type resolution fallback (handled by index lookup if slug matches)
        return make_entity_id(entity.entity_type, entity.canonical_name)

    def _process_discoveries(self, discoveries):
        """
        LEARNING PROCESSOR: Integrates new types/relations found by the LLM
        into the persistent ontology rules.
        """
        cur_ont = self.db.get_ontology()
        entities = set(cur_ont.get('entity_types', []))
        relations = set(cur_ont.get('relation_types', []))
        colors = cur_ont.get('entity_colors', {})
        triples = cur_ont.get('allowed_triples', [])
        
        updated = False
        for d in discoveries:
            is_new = False
            if d.type == 'ENTITY' and d.suggested_label not in entities:
                entities.add(d.suggested_label)
                # Assign default color if missing
                if d.suggested_label not in colors:
                    colors[d.suggested_label] = "#3b82f6" 
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
            self.db.update_ontology('entity_colors', colors)
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
        try:
            self.db.clear_graph_data()
            # No need for _init_db() if clear_graph_data uses TRUNCATE
            self.db.seed_ontology(merge_with_existing=True) 
            self.ontology = self.db.get_ontology()
            self.guard = LogicGuard(self.ontology)
            self._alias_index = {}
            self._refresh_alias_index()
            logger.info("GraphStore reset successful.")
        except Exception as e:
            logger.error(f"GraphStore reset failed: {e}")
            raise

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
            self.db._release_connection(conn)
