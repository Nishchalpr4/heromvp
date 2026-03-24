"""
Zone 1 — LLM Extraction Engine (Hierarchical Structural Reasoning)
==================================================================
Converts unstructured text into a hierarchical knowledge graph.
Follows a specific 7-step intent-capture process defined by the user.
"""

from __future__ import annotations
import json
import os
import re
import uuid
import httpx
from typing import Any
from models import (
    ExtractionPayload, EntityCandidate, RelationCandidate,
    EvidenceRef, ZONE1_ONTOLOGY_VERSION,
)
from validators import safe_json_loads

def _get_llm_config():
    """LLM CONFIG: Loads credentials and model settings from .env."""
    from dotenv import load_dotenv
    load_dotenv(override=True)
    return {
        "api_key":  os.getenv("LLM_API_KEY", ""),
        "base_url": os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1"),
        "model":    os.getenv("LLM_MODEL", "llama-3.3-70b-versatile"),
    }


def _repair_truncated_json(raw: str) -> dict:
    """
    Attempt to repair a truncated JSON response from the LLM.
    Common issues: unterminated strings, missing closing brackets/braces.
    """
    text = raw.strip()

    # 1. Close any unterminated string (odd number of unescaped quotes)
    quote_count = 0
    i = 0
    while i < len(text):
        if text[i] == '"' and (i == 0 or text[i-1] != '\\'):
            quote_count += 1
        i += 1
    if quote_count % 2 == 1:
        # Find the last quote and truncate after closing it
        text += '"'

    # 2. Remove any trailing comma before we add closing brackets
    text = re.sub(r',\s*$', '', text)

    # 3. Count open brackets/braces and close them
    open_braces = text.count('{') - text.count('}')
    open_brackets = text.count('[') - text.count(']')

    # Close brackets first (inner arrays), then braces (outer objects)
    text += ']' * max(0, open_brackets)
    text += '}' * max(0, open_braces)

    try:
        return safe_json_loads(text, default={})
    except Exception as e:
        print("\n[ERROR] Failed to parse LLM JSON output:")
        print(text)
        print(f"[ERROR] {e}")
        raise


from database import DatabaseManager

db = DatabaseManager()

def get_dynamic_prompt() -> str:
    """Builds a highly structural system prompt with tier-based hierarchy and few-shot examples."""
    ontology = db.get_ontology()
    entity_types = ", ".join(ontology.get('entity_types', []))
    
    relations_list = []
    for triple in ontology.get('allowed_triples', []):
        relations_list.append(f"{triple['relation']}: {triple['source']} -> {triple['target']}")
    relations_str = "\n".join(relations_list)
    
    rules_list = ontology.get('extraction_rules', [])
    cleaned_rules = [re.sub(r'^\d+\.\s*', '', r) for r in rules_list]
    
    # Tiered Hierarchy Logic (STRICT)
    tier_rules = [
        "TIER 0 (APEX): Primary Company (LegalEntity).",
        "TIER 1 (CONTAINERS): Management, Competitors, ProductPortfolio, BusinessUnit, Manufacturer.",
        "TIER 2 (ROLES/DOMAINS): Role, ProductDomain, Site, Sector.",
        "TIER 3 (PEOPLE/FAMILIES): Person, ProductFamily, Technology, Capability.",
        "TIER 4 (LINES/LEAF): ProductLine, Brand, Program, Initiative."
    ]
    all_rules = cleaned_rules + tier_rules + [
        "REASONING (CRITICAL): You MUST output a 'thought_process' field. Explain how you connected every node to the ROOT. List the 'missing links' you had to infer to prevent islands.",
        "ZERO FLOATING NODES (STRICT): Every entity MUST have an incoming or outgoing relationship that eventually leads to the Tier 0 ROOT. Absolute zero tolerance for floating leaf nodes (e.g. Mac).",
        "UNIFIED HARDWARE GROUPING: When the text mentions diverse hardware (e.g. iPhone, Mac), group them under a single ProductFamily node: 'Hardware Products' (unless specific sub-families like 'Phones' are mentioned).",
        "PRODUCT TAXONOMY (FACTUAL): Consumer Electronics (ProductDomain) -> Hardware Products (ProductFamily) -> iPhone / Mac (ProductLines). This creates a valid 4-hop chain (Company -> Portfolio -> Domain -> Family -> Line).",
        "SUPPLY CHAIN: Nodes like Foxconn should be 'Manufacturer' type and linked via 'MANUFACTURES_FOR' to the LegalEntity.",
        "DEEP GEOGRAPHY: Scrutinize every location. If 'Asia' is mentioned, create a Geography node and link the company via 'OPERATES_IN'.",
        "NARRATIVE ATTRIBUTES: Capture focuses (e.g. 'design focus') as detailed fields in the LegalEntity's 'attributes' object.",
        "STRICT NUMERICS: The 'value' field in 'quant_data' MUST be a JSON number.",
        "MANAGEMENT CHAIN: [Company] -> HAS_MANAGEMENT -> [Management] -> HAS_ROLE -> [Role] -> HELD_BY -> [Person].",
        "GEOSPATIAL HIERARCHY: Region -> Country -> Site. Link Company to the Region.",
        "CATEGORY SYNTHESIS: You MUST synthesize intermediate nodes (Portfolio -> Domain -> Family) even if not explicitly named in the text, to bridge the leaf products back to the ROOT.",
        "EVIDENCE: Every relation MUST have 'source_text' with the EXACT verbatim quote.",
        "ENTITY DESCRIPTION (STRICT): Every entity MUST have a concise 1-sentence 'description' field explaining its identity/role.",
        "RELATION ATTRIBUTES (STRICT): Every relation MUST have an 'attributes' object containing a 'description' (explaining the link) and a 'weight' (float 0.0-1.0 representing strength/importance).",
        "CANONICAL SINGLETONS (DEDUPE): Use formal canonical names. Example: Use 'United States' instead of 'America' or 'American'.",
        "ANTI-GROUPING (SPLIT): NEVER create a single node for a list of entities. Example: Split 'AMD and Intel' into separate Compititor nodes.",
        "MANDATORY ENTITY CHECKLIST: You MUST create separate nodes for: 1. EVERY mentioned country/geography, 2. EVERY mentioned End Market (e.g. 'Gaming', 'Data Centers'), 3. EVERY mentioned technology capability (e.g. 'AI Hardware', 'AI Software').",
        "EXHAUSTIVE RELATIONSHIPS: Every product line MUST link to its taxonomical parent AND its target End Market (e.g. GeForce -> Graphics AND GeForce -> Gaming).",
        "DENSITY OVER SIMPLICITY: I prioritize absolute factual completeness. If it's in the text, it MUST be in the graph as a node or relation. Expected count: 12-18 entities."
    ]
    
    rules_str = "\n".join([f"{i+1}. {rule}" for i, rule in enumerate(all_rules)])
    
    return f"""### ROLE
You are an Advanced Investment Analyst AI. Your task is to transform unstructured corporate text into a STICKER HIERARCHICAL KNOWLEDGE GRAPH. 

### 1. ONTOLOGY (LABELS ONLY)
- ENTITY TYPES: {entity_types}
- ALLOWED TRIPLES:
{relations_str}

### 2. STRUCTURAL MANDATES (EXECUTION STANDARDS)
{rules_str}

### 3. FEW-SHOT EXAMPLE (GOLD STANDARD)
INPUT: "Apple designs and sells consumer electronics like the iPhone and Mac. Manufacturing is handled by Foxconn in Asia. It competes with Samsung and Google."
OUTPUT:
{{
    "thought_process": "1. Apple Inc. is ROOT. 2. Created 'Apple Product Portfolio' as a top-level container. 3. 'Consumer Electronics' is a ProductDomain and an EndMarket. 4. iPhone and Mac share a 'Hardware Products' ProductFamily under the domain. 5. Foxconn is a Manufacturer; linked it to Apple via MANUFACTURES_FOR. 6. Samsung and Google are split into separate Competitor nodes. 7. United States is a Geography for Apple.",
    "entities": [
        {{ 
            "temp_id": "e_root", "entity_type": "LegalEntity", "canonical_name": "Apple Inc.", 
            "description": "A global leader in consumer electronics and software services.",
            "short_info": "Global Tech Giant",
            "aliases": ["Apple"],
            "attributes": {{ "is_root": true }}
        }},
        {{ 
            "temp_id": "e_lp", "entity_type": "ProductLine", "canonical_name": "iPhone", 
            "description": "Apple's flagship line of smartphones.",
            "short_info": "Smartphone Line",
            "attributes": {{ }}
        }},
        {{ 
            "temp_id": "e_geo", "entity_type": "Geography", "canonical_name": "United States", 
            "description": "The primary country of origin for the subject.",
            "short_info": "US Origin",
            "attributes": {{ }}
        }},
        {{ 
            "temp_id": "e_em", "entity_type": "EndMarket", "canonical_name": "Consumer Electronics", 
            "description": "The target market for Apple's hardware products.",
            "short_info": "Hardware Market",
            "attributes": {{ }}
        }},
        {{ 
            "temp_id": "e_comp1", "entity_type": "Competitors", "canonical_name": "Samsung", 
            "description": "A major competitor in the smartphone market.",
            "short_info": "Tech Competitor",
            "attributes": {{ }}
        }}
    ],
    "relations": [
        {{ "source_temp_id": "e_root", "target_temp_id": "e_lp", "relation_type": "HAS_PRODUCT_LINE", "source_text": "Apple designs and sells iPhone", "confidence": 1.0, "weight": 1.0, "attributes": {{ "description": "iPhone is the primary revenue driver." }} }},
        {{ "source_temp_id": "e_root", "target_temp_id": "e_geo", "relation_type": "OPERATES_IN", "source_text": "American company", "confidence": 1.0, "weight": 1.0, "attributes": {{ "description": "Headquartered in US." }} }},
        {{ "source_temp_id": "e_lp", "target_temp_id": "e_em", "relation_type": "SERVES_END_MARKET", "source_text": "iPhone for consumer electronics", "confidence": 1.0, "weight": 1.0, "attributes": {{ "description": "Primary market for iPhone." }} }},
        {{ "source_temp_id": "e_root", "target_temp_id": "e_comp1", "relation_type": "COMPETES_WITH", "source_text": "competes with Samsung", "confidence": 1.0, "weight": 1.0, "attributes": {{ "description": "Major rival." }} }}
    ]
}}

### 4. FINAL INSTRUCTION
Process the text below.
OUTPUT ONLY THE JSON OBJECT. THE OUTPUT MUST START WITH '{{' AND END WITH '}}'.
MANDATORY: Every entity MUST have a non-null 'description' (full sentence) AND a non-null 'short_info' (2-3 words, e.g. 'Tech Supplier').
Ensure zero orphans. Every node MUST trace back to the ROOT.
"""



def _extract_json_block(text: str) -> str:
    """Finds the first '{' and last '}' to isolate a JSON object from surrounding text."""
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            return text[start:end+1]
    except Exception:
        pass
    return text


async def call_llm(text: str, document_name: str = "User Input", section_ref: str = "chunk", metadata: dict = {}, custom_prompt: str = None) -> ExtractionPayload:
    """
    INGESTION CORE: Constructs the intent-capture prompt, calls the LLM,
    repairs the JSON response, and validates it against the ExtractionPayload schema.
    """
    cfg = _get_llm_config()
    document_id = f"doc_{uuid.uuid4().hex[:8]}"
    
    meta_str = json.dumps(metadata) if metadata else "N/A"
    user_prompt = f"METADATA: {meta_str}\n\nExtract the hierarchical knowledge graph and financial analysis from this text:\n\n{text}"

    headers = {
        "Authorization": f"Bearer {cfg['api_key']}",
        "Content-Type": "application/json",
    }

    # If the user provided a custom prompt in the UI, we use it as the system message.
    # Otherwise, we use the generated dynamic prompt based on ontology.
    effective_system_prompt = custom_prompt if custom_prompt else get_dynamic_prompt()

    body = {
        "model": cfg["model"],
        "messages": [
            {"role": "system", "content": effective_system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 8000,
    }

    endpoint = cfg['base_url']
    if not endpoint.endswith("/chat/completions"):
        endpoint = endpoint.rstrip("/") + "/chat/completions"
    
    finish_reason = "mock"
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(endpoint, headers=headers, json=body)
            response.raise_for_status()
            
        try:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            finish_reason = result["choices"][0].get("finish_reason", "stop")
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"[ERROR] Failed to parse LLM response JSON or structure: {e}")
            print(f"[DEBUG] Raw response: {response.text[:500]}...")
            raise Exception(f"Invalid response from LLM API. Details: {e}")
    except Exception as e:
        print(f"[ERROR] LLM API failed: {e}.")
        raise Exception(f"API Rate Limit or Connection Error. Please wait a few minutes and try again. Details: {e}")
    
    # Strip fences or isolate JSON block
    content = _extract_json_block(content).strip()
    
    # Parse JSON - DEFENSIVE
    if not content:
        print(f"[ERROR] LLM returned empty or non-JSON content. Finish reason: {finish_reason}")
        parsed = {"entities": [], "relations": [], "abstentions": ["LLM returned empty or non-JSON response"]}
    else:
        # First attempt: simple parse
        parsed = safe_json_loads(content)
        
        if parsed is None:
            # Second attempt: repair and parse (only if it looks like JSON)
            print(f"[DEBUG] Initial JSON parse failed. Attempting repair. Content starts with: {content[:100]}...")
            parsed = _repair_truncated_json(content)
            
        if not parsed:
            # Final fallback
            print(f"[ERROR] All JSON parsing attempts failed for content: {content[:200]}...")
            parsed = {"entities": [], "relations": [], "abstentions": ["FAILED_TO_PARSE_JSON"]}

    # Load dynamic ontology for validation
    ontology = db.get_ontology()
    valid_entity_types = set(ontology.get("entity_types", []))
    valid_relation_types = set(ontology.get("relation_types", []))

    # Parse entities
    entities = []
    skipped_entities = []
    for e in parsed.get("entities", []):
        try:
            # DEFENSIVE FALLBACK: Ensure mandatory fields are never null
            if not e.get("short_info"):
                e["short_info"] = e.get("canonical_name", "N/A")[:30]
            if not e.get("description"):
                e["description"] = f"A {e.get('entity_type', 'Entity')} related to {e.get('canonical_name', 'topic')}."
                
            print(f"[DEBUG] Processing entity '{e.get('canonical_name')}': short_info='{e.get('short_info')}'")
            
            ent_cand = EntityCandidate(**e)
            if ent_cand.entity_type not in valid_entity_types:
                pass
            entities.append(ent_cand)
        except Exception as exc:
            skipped_entities.append(f"Entity '{e.get('canonical_name', '?')}': {exc}")

    # Parse relations
    relations = []
    skipped_relations = []
    for r in parsed.get("relations", []):
        try:
            rel_cand = RelationCandidate(**r)
            relations.append(rel_cand)
        except Exception as exc:
            skipped_relations.append(f"Relation: {exc}")

    # Collect abstentions
    abstentions = parsed.get("abstentions", [])
    abstentions.extend(skipped_entities)
    abstentions.extend(skipped_relations)

    from models import AnalysisAttributes, QuantMetric, OntologyDiscovery
    analysis_data = parsed.get("analysis_attributes", {})
    analysis_attr = AnalysisAttributes(**analysis_data) if analysis_data else None

    # Parse Quant and Discoveries
    quant_data = [QuantMetric(**q) for q in parsed.get("quant_data", [])]
    discoveries = [OntologyDiscovery(**d) for d in parsed.get("discoveries", [])]

    return ExtractionPayload(
        thought_process=parsed.get("thought_process", ""),
        source_document_id=document_id,
        source_document_name=document_name,
        entities=entities,
        relations=relations,
        quant_data=quant_data,
        discoveries=discoveries,
        abstentions=abstentions,
        analysis_attributes=analysis_attr,
        llm_analysis_summary=parsed.get("llm_analysis_summary")
    )
