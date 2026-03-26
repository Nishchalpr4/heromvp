import json
import logging
from typing import List, Dict, Any, Optional
from models import ExtractionPayload, EntityCandidate, RelationCandidate

logger = logging.getLogger(__name__)

def safe_json_loads(data: Any, default: Any = None) -> Any:
    """
    Safely load a JSON string. 
    If data is already a dict/list, return it.
    If data is an empty string or None, return the default.
    If parsing fails, log the error and return the default.
    """
    if data is None:
        return default
    if isinstance(data, (dict, list)):
        return data
    if not isinstance(data, str) or not data.strip():
        return default
        
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse JSON: {e}. Data starts with: {str(data)[:100]}")
        return default

class LogicGuard:
    def __init__(self, ontology: Dict[str, Any]):
        self.ontology = ontology
        self.allowed_triples = set()
        for t in ontology.get('allowed_triples', []):
            self.allowed_triples.add((t['source'], t['relation'], t['target']))

    def refine_payload(self, payload: ExtractionPayload) -> ExtractionPayload:
        """
        SELF-HEALING: Actively corrects the payload structure based on rules.
        """
        entity_map = {e.temp_id: e for e in payload.entities}
        
        # 1. Type Correction based on common relations
        for rel in payload.relations:
            src = entity_map.get(rel.source_temp_id)
            tgt = entity_map.get(rel.target_temp_id)
            if not src or not tgt: continue
            
            # If target is linked via HAS_MANAGEMENT/HELD_BY, it MUST be Person/Management
            if rel.relation_type in ["HAS_MANAGEMENT", "HELD_BY"] and tgt.entity_type not in ["Management", "Person"]:
                logger.info(f"[HEAL] Fixing {tgt.canonical_name} type: {tgt.entity_type} -> Person/Management")
                # Heuristic: If it has (CEO) or similar in short_info, it's a Person
                if tgt.short_info and any(x in tgt.short_info.upper() for x in ["CEO", "COO", "CTO", "CHAIR"]):
                    tgt.entity_type = "Person"
                else:
                    tgt.entity_type = "Management"
                    
            # If target is linked via HAS_PRODUCTS, it's likely a ProductLine or Portfolio
            if rel.relation_type == "HAS_PRODUCTS" and tgt.entity_type not in ["ProductLine", "ProductFamily", "Brand"]:
                logger.info(f"[HEAL] Fixing {tgt.canonical_name} type: {tgt.entity_type} -> ProductLine")
                tgt.entity_type = "ProductLine"

        # 2. Re-parenting for Cardinal Rule violations
        # Rule: Person/Management cannot own a LegalEntity (usually extraction flip)
        new_relations = []
        for rel in payload.relations:
            src = entity_map.get(rel.source_temp_id)
            tgt = entity_map.get(rel.target_temp_id)
            if not src or not tgt:
                new_relations.append(rel)
                continue
                
            if src.entity_type in ["Person", "Management"] and tgt.entity_type == "LegalEntity":
                logger.info(f"[HEAL] Flipping relation between {src.canonical_name} and {tgt.canonical_name}")
                rel.source_temp_id, rel.target_temp_id = rel.target_temp_id, rel.source_temp_id
                # Logic says LegalEntity -> HAS_MANAGEMENT -> Person
                rel.relation_type = "HAS_MANAGEMENT"
            
            new_relations.append(rel)
            
        payload.relations = new_relations
        return payload

    def validate_extraction(self, payload: ExtractionPayload) -> List[str]:
        """Runs all logic guards and returns a list of warning/error messages."""
        flags = []
        flags.extend(self._check_types(payload.entities, payload.relations))
        flags.extend(self._check_cycles(payload.relations))
        flags.extend(self._check_quant(payload.quant_data))
        return flags

    def _check_types(self, entities: List[EntityCandidate], relations: List[RelationCandidate]) -> List[str]:
        flags = []
        entity_map = {e.temp_id: e.entity_type for e in entities}
        
        for rel in relations:
            src_type = entity_map.get(rel.source_temp_id)
            tgt_type = entity_map.get(rel.target_temp_id)
            
            if not src_type or not tgt_type:
                continue
                
            # Allow COMPETES_WITH between LegalEntity and Competitors (common discovery case)
            if rel.relation_type == "COMPETES_WITH":
                if src_type in ["LegalEntity", "Competitors"] and tgt_type in ["LegalEntity", "Competitors"]:
                    continue

            triple = (src_type, rel.relation_type, tgt_type)
            if triple not in self.allowed_triples:
                flags.append(f"Type Guard: Relation {rel.relation_type} not allowed between {src_type} and {tgt_type}")
        
        return flags

    def _check_cycles(self, relations: List[RelationCandidate]) -> List[str]:
        flags = []
        # Simple immediate cycle check: A -> B and B -> A
        links = {} # (source, target) -> relation_type
        for rel in relations:
            links[(rel.source_temp_id, rel.target_temp_id)] = rel.relation_type
            
            if (rel.target_temp_id, rel.source_temp_id) in links:
                flags.append(f"Cycle Guard: Potential circular relationship between {rel.source_temp_id} and {rel.target_temp_id}")
                
        # Self-loop check
        for rel in relations:
            if rel.source_temp_id == rel.target_temp_id:
                flags.append(f"Cycle Guard: Self-loop detected for {rel.source_temp_id}")
                
        return flags

    def _check_quant(self, quant_data) -> List[str]:
        flags = []
        for q in quant_data:
            if q.metric.lower() in ['revenue', 'price', 'pat', 'ebitda'] and q.value < 0:
                flags.append(f"Quant Guard: Invalid negative value for {q.metric}: {q.value}")
        return flags
