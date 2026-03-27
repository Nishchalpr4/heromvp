import os
import json
import logging
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from models import ExtractionPayload, EntityCandidate, RelationCandidate
from database import DatabaseManager
from validators import safe_json_loads

logger = logging.getLogger(__name__)

def log_stage_debug(stage: str, prompt: str, response: str):
    """
    Disabled debug logger to avoid creating local files as per user request.
    """
    pass

import time
import random

def call_llm(prompt: str, model: str = None, timeout: int = 90) -> str:
    """
    Calls the LLM via OpenRouter using direct requests for absolute timeout control.
    """
    import requests
    import os
    import time
    import random
    from dotenv import load_dotenv
    load_dotenv(override=True)
    
    if not model:
        model = os.getenv("LLM_MODEL", "arcee-ai/trinity-mini:free")
        
    api_key = os.getenv("LLM_API_KEY")
    base_url = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:8000",
        "X-Title": "Investment Intelligence System"
    }
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"[LLM] Dispatching to {model} (Attempt {attempt+1}/{max_retries})")
            response = requests.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            result = response.json()
            return result['choices'][0]['message']['content']
        except Exception as e:
            err_msg = str(e).lower()
            if any(x in err_msg for x in ["429", "rate limit", "timeout", "connection"]) and attempt < max_retries - 1:
                delay = 2 * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"[LLM] Retryable error: {e}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                logger.error(f"[LLM] Final failure after {attempt+1} attempts: {e}")
                return ""
    return ""

def get_dynamic_prompt(text: str = "{text}") -> str:
    """
    Generates a prompt enriched with the current ontology and strict hierarchical rules.
    """
    db = DatabaseManager()
    ontology = db.get_ontology()
    
    entity_types = ", ".join(ontology.get('entity_types', []))
    
    relations_list = []
    for triple in ontology.get('allowed_triples', []):
        relations_list.append(f"{triple['relation']}: {triple['source']} -> {triple['target']}")
    relations_str = "\n".join(relations_list)
    
    rules_list = ontology.get('extraction_rules', [])
    # Clean rules if they have numbers to avoid double numbering in the prompt
    cleaned_rules = [re.sub(r'^\d+\.\s*', '', r) for r in rules_list]
    rules_str = "\n".join([f"{i+1}. {rule}" for i, rule in enumerate(cleaned_rules)])
    
    # FETCH EXAMPLES FROM DATABASE (Reduces hardcoding)
    examples_list = ontology.get('extraction_examples', [])
    examples_str = ""
    for ex in examples_list:
        examples_str += "### EXAMPLE\n"
        examples_str += f"Input: \"{ex.get('input', 'N/A')}\"\n"
        examples_str += f"Thought: {ex.get('thought_process', 'N/A')}\n"
        
        # Include JSON structure if available (Better for schema adherence)
        if 'output_json' in ex:
            examples_str += f"Output JSON:\n{json.dumps(ex['output_json'], indent=2)}\n"
        
        # Always include ASCII tree if available (Better for hierarchical clarity)
        if 'ascii_tree' in ex:
            examples_str += f"Expected Hierarchy:\n{ex['ascii_tree']}\n"
        examples_str += "\n"

    return f"""### ROLE
You are an Advanced Investment Analyst AI. Your task is to transform unstructured corporate text into a STICKER HIERARCHICAL KNOWLEDGE GRAPH. 

### 1. ONTOLOGY (LABELS ONLY)
- ENTITY TYPES: {entity_types}
- ALLOWED TRIPLES:
{relations_str}

### 2. STRUCTURAL MANDATES (EXECUTION STANDARDS)
{rules_str}

### 3. PERFECT EXAMPLES
{examples_str}

### 4. FINAL INSTRUCTION
Process the text below with PLATINUM PRECISION.
OUTPUT MUST BE A SINGLE JSON OBJECT with EXACTLY these keys:
- "thought_process": Reasoning for the hierarchy.
- "entities": List of extracted components. Each entity MUST have:
    - [temp_id, canonical_name, entity_type]
    - "short_info": A 1-sentence descriptor of the entity (NEVER use "N/A", synthesize if needed).
- "relations": List of connections. Each relation MUST have:
    - [source_temp_id, relation_type, target_temp_id]
    - "source_text": The EXACT VERBATIM SENTENCE from the text that proves this relation. (CRITICAL for evidence).
    - "confidence": 0-1 score.

MANDATORY: NO ORPHANS. Every node MUST lead back to the ROOT.

TEXT:
{text}
"""

def extract_knowledge(text: str, document_id: str = "doc_test", document_name: str = "Unspecified Source") -> ExtractionPayload:
    """
    Full pipeline: text -> dynamic prompt -> LLM -> Validation.
    """
    prompt = get_dynamic_prompt(text)
    raw_json = call_llm(prompt)
    with open("raw_response.json", "w") as f:
        f.write(raw_json)
    
    try:
        # CLEAN: Strip Markdown code blocks if present
        clean_json = re.sub(r'^```(?:json)?\s*', '', raw_json.strip())
        clean_json = re.sub(r'\s*```$', '', clean_json)
        
        data = json.loads(clean_json)
        
        # DEFENSIVE: If LLM returned a list, wrap it (though role says return object)
        if isinstance(data, list):
            logger.warning("[PARSE] LLM returned a list instead of an object. Attempting wrap.")
            data = {"entities": data, "relations": [], "thought_process": "Wrapped list into object."}

        # Add basic validation for required fields
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict from JSON, got {type(data)}")

        if "entities" not in data: data["entities"] = []
        if "relations" not in data: data["relations"] = []
        if "source_document_id" not in data: data["source_document_id"] = document_id
        if "source_document_name" not in data: data["source_document_name"] = document_name
        if "thought_process" not in data: data["thought_process"] = "No thought process provided by LLM."
        
        # Post-process for consistency
        for ent in data.get("entities", []):
            if "short_info" not in ent or ent["short_info"] == "N/A":
                ent["short_info"] = ent.get("description", ent.get("logic", "Resolved Entity"))
        
        for rel in data.get("relations", []):
            if "source_text" not in rel or not rel["source_text"]:
                rel["source_text"] = rel.get("evidence", rel.get("justification", ""))

        # Clean data for Pydantic (remove unexpected keys if any)
        valid_keys = {"entities", "relations", "thought_process", "source_document_id", "source_document_name", "quant_data", "unstructured_analysis"}
        pydantic_data = {k: v for k, v in data.items() if k in valid_keys}

        return ExtractionPayload(**pydantic_data)
    except Exception as e:
        logger.error(f"Failed to parse LLM output: {e}\nRaw output: {raw_json}")
        # Return a valid empty payload instead of crashing
        return ExtractionPayload(
            thought_process=f"Error parsing LLM output: {str(e)}",
            source_document_id=document_id,
            source_document_name=document_name,
            entities=[],
            relations=[]
        )

class MultiStageExtractor:
    """
    4-STAGE PRECISION ENGINE:
    Implements the "Funnel" architecture for high-fidelity extraction.
    Stage 1: Entity Discovery (Recall)
    Stage 2: Entity Resolution (Deduplication)
    Stage 3: Relation Mapping (Strict Grounding)
    Stage 4: Fact/Attribute Enrichment (Precision)
    """
    def __init__(self, text: str, document_id: str, document_name: str):
        self.text = text
        self.doc_id = document_id
        self.doc_name = document_name
        self.db = DatabaseManager()
        self.ontology = self.db.get_ontology()
        self.prompts = self.ontology.get("multi_stage_prompts", {})
        
        self.raw_entities = []   # Raw extracted mentions
        self.resolved_map = {}   # ID -> Canonical Name / Data
        self.relations = []      # Final relations
        self.quant_data = []     # Final quant data
        self.thought_process = [] # Log of steps

    def _log(self, message: str):
        logger.info(f"[MULTI-STAGE] {message}")
        self.thought_process.append(message)

    def run_stage_1_entities(self) -> List[Dict]:
        """STAGE 1: Extract all entity variations with evidence."""
        self._log("Starting Stage 1: Entity Discovery...")
        prompt_tpl = self.prompts.get("stage_1_entities")
        if not prompt_tpl:
            self._log("ERROR: Stage 1 prompt not found in ontology.")
            return []
            
        prompt = f"{prompt_tpl}\n\nTEXT:\n{self.text}"
        raw_json = call_llm(prompt, timeout=90) # Added timeout
        log_stage_debug("STAGE 1 (Discovery)", prompt, raw_json)
        from validators import safe_json_loads, find_list_data
        data = safe_json_loads(raw_json, default=[])
        data = find_list_data(data)
        
        self.raw_entities = data
        self._log(f"Stage 1 Complete: Found {len(self.raw_entities)} raw mentions.")
        return self.raw_entities

    def run_stage_2_resolution(self) -> Dict:
        """STAGE 2: Deduplicate and resolve mentions into EIDs."""
        self._log("Starting Stage 2: Entity Resolution...")
        prompt_tpl = self.prompts.get("stage_2_resolution")
        if not prompt_tpl:
            self._log("ERROR: Stage 2 prompt not found.")
            return {}

        entities_str = json.dumps(self.raw_entities, indent=2)
        prompt = f"{prompt_tpl}\n\nCANDIDATE ENTITIES:\n{entities_str}"
        raw_json = call_llm(prompt, timeout=90)
        log_stage_debug("STAGE 2 (Resolution)", prompt, raw_json)
        
        from validators import safe_json_loads, find_list_data
        data = safe_json_loads(raw_json, default=[])
        data = find_list_data(data)
        
        for item in data:
            if not isinstance(item, dict): continue
            eid = item.get('entity_id')
            if eid:
                self.resolved_map[eid] = item
            else:
                logger.warning(f"[MULTI-STAGE] Stage 2 item missing entity_id: {item}")
        
        self._log(f"Stage 2 Complete: Resolved into {len(self.resolved_map)} unique entities.")
        return self.resolved_map

    def run_stage_3_relations(self) -> List[Dict]:
        """STAGE 3: Strict relation mapping using resolved IDs."""
        self._log("Starting Stage 3: Relation Extraction...")
        prompt_tpl = self.prompts.get("stage_3_relations")
        if not prompt_tpl:
            self._log("ERROR: Stage 3 prompt not found.")
            return []

        # Prepare allowed relations for prompt
        allowed_rels = ", ".join(self.ontology.get("relation_types", []))
        
        resolved_list = [{"entity_id": k, "name": v.get('canonical_name', 'Unknown')} for k, v in self.resolved_map.items()]
        entities_context = json.dumps(resolved_list, indent=2)
        
        prompt = f"{prompt_tpl.replace('{relations}', allowed_rels)}\n\nRESOLVED ENTITIES:\n{entities_context}\n\nTEXT:\n{self.text}"
        raw_json = call_llm(prompt, timeout=120)
        log_stage_debug("STAGE 3 (Relations)", prompt, raw_json)
        
        from validators import safe_json_loads, find_list_data
        res = safe_json_loads(raw_json, default=[])
        res = find_list_data(res)
                
        self.relations = res
        print(f"DEBUG: Stage 3 Raw Relations (first 2): {json.dumps(self.relations[:2], indent=2)}")
        self._log(f"Stage 3 Complete: Extracted {len(self.relations)} strict relations.")
        return self.relations

    def run_stage_4_facts(self) -> List[Dict]:
        """STAGE 4: Attribute and Quantitative extraction."""
        self._log("Starting Stage 4: Fact Enrichment...")
        prompt_tpl = self.prompts.get("stage_4_facts")
        if not prompt_tpl:
            self._log("ERROR: Stage 4 prompt not found.")
            return []

        resolved_list = [{"entity_id": k, "name": v.get('canonical_name', 'Unknown')} for k, v in self.resolved_map.items()]
        entities_context = json.dumps(resolved_list, indent=2)
        
        prompt = f"{prompt_tpl}\n\nRESOLVED ENTITIES:\n{entities_context}\n\nTEXT:\n{self.text}"
        raw_json = call_llm(prompt, timeout=90)
        log_stage_debug("STAGE 4 (Facts)", prompt, raw_json)
        
        from validators import safe_json_loads, find_list_data
        res = safe_json_loads(raw_json, default=[])
        res = find_list_data(res)
        
        self.quant_data = res
        self._log(f"Stage 4 Complete: Extracted {len(self.quant_data)} facts.")
        return self.quant_data

    def finalize(self) -> ExtractionPayload:
        """Assembles the stages into a final ExtractionPayload and enforces grounding."""
        from models import EntityCandidate, RelationCandidate, QuantMetric, EvidenceRef
        
        payload_entities = []
        for eid, data in self.resolved_map.items():
            if not isinstance(data, dict): continue
            
            # Match back to Stage 1 to recover evidence
            cname = data.get('canonical_name', data.get('name', 'Unknown'))
            matches = [m for m in self.raw_entities if m.get('name') == cname or m.get('name') in data.get('aliases', [])]
            
            etype = data.get('type')
            if not etype:
                etype = matches[0].get('type', 'LegalEntity') if matches else 'LegalEntity'
            
            # Map evidence from Stage 1
            evidence_list = []
            primary_info = "Resolved Entity"
            for m in matches:
                if m.get('evidence'):
                    primary_info = m['evidence'] # Use the first available Stage 1 evidence as short_info
                    evidence_list.append(EvidenceRef(
                        document_id=self.doc_id,
                        document_name=self.doc_name,
                        evidence_quote=m['evidence']
                    ))
                
            payload_entities.append(EntityCandidate(
                temp_id=eid,
                canonical_name=cname,
                entity_type=etype,
                aliases=data.get('aliases', []),
                short_info=primary_info,
                evidence=evidence_list
            ))

        payload_relations = []
        for rel in (self.relations if isinstance(self.relations, list) else []):
            sid = rel.get('source_id')
            tid = rel.get('target_id')
            rtype = rel.get('relation')
            revidence = rel.get('evidence', rel.get('source_text', ''))
            
            if not all([sid, tid, rtype]):
                continue
                
            # Create proper evidence reference
            evidence_ref = []
            if revidence:
                evidence_ref.append(EvidenceRef(
                    document_id=self.doc_id,
                    document_name=self.doc_name,
                    evidence_quote=revidence
                ))
            
            payload_relations.append(RelationCandidate(
                source_temp_id=sid,
                target_temp_id=tid,
                relation_type=rtype,
                confidence=rel.get('confidence', 1.0),
                source_text=rel.get('evidence', ""),
                evidence=rel_evidence
            ))

        payload_quants = []
        for q in (self.quant_data if isinstance(self.quant_data, list) else []):
            eid = q.get('entity_id')
            attr = q.get('attribute')
            val_raw = q.get('value')
            
            if not all([eid, attr, val_raw is not None]):
                continue
            
            # STRICT GROUNDING: Filter out placeholder '0' or unquantified metrics
            val_str = str(val_raw).replace('$', '').replace(',', '').strip()
            numeric_match = re.search(r'[-+]?\d*\.?\d+', val_str)
            
            if not numeric_match:
                self._log(f"Skipping unquantified metric: {attr} for {eid} (No number found in '{val_raw}')")
                continue
                
            val = float(numeric_match.group())
            
            # REJECT 0.0 HALLUCINATION: If value is 0.0 but '0' isn't in the original value string, it's a placeholder
            if val == 0.0 and '0' not in val_str:
                self._log(f"Rejecting '0' placeholder for {attr}")
                continue
            
            payload_quants.append(QuantMetric(
                subject_id=eid,
                metric=attr,
                value=val,
                unit=q.get('unit'),
                period=q.get('time_context')
            ))

        # ════════════════════════════════════════════════════════════════════════
        # LOGIC GUARD INTEGRATION
        # ════════════════════════════════════════════════════════════════════════
        from validators import LogicGuard
        self._log("Running LogicGuard Self-Healing...")
        
        payload = ExtractionPayload(
            thought_process="\n".join(self.thought_process),
            source_document_id=self.doc_id,
            source_document_name=self.doc_name,
            entities=payload_entities,
            relations=payload_relations,
            quant_data=payload_quants
        )
        
        guard = LogicGuard(self.ontology)
        payload = guard.refine_payload(payload)
        
        return payload

def extract_knowledge_multistage(text: str, document_id: str = "doc_test", document_name: str = "Unspecified Source") -> ExtractionPayload:
    """
    Entry point for the Multi-Stage extraction pipeline.
    SELF-HEALING FALLBACK: Unified Mode for short text or specific models.
    """
    load_dotenv(override=True)
    model = os.getenv("LLM_MODEL", "google/gemini-2.0-flash-001")
    
    # Check if we should use Unified Mode (Short text or specific models)
    if len(text) < 1500 or "free" in model.lower() or "mini" in model.lower() or "flash" in model.lower():
        logger.info(f"[EXTRACTION] Unified Mode active for: {model}")
        payload = extract_knowledge(text, document_id, document_name)
        
        # Still apply LogicGuard for structural integrity
        from validators import LogicGuard
        db = DatabaseManager()
        ontology = db.get_ontology()
        guard = LogicGuard(ontology)
        return guard.refine_payload(payload)

    # Standard Multi-Stage
    extractor = MultiStageExtractor(text, document_id, document_name)
    extractor.run_stage_1_entities()
    extractor.run_stage_2_resolution()
    extractor.run_stage_3_relations()
    return extractor.finalize()
