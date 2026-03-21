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

    return json.loads(text)


from database import DatabaseManager

db = DatabaseManager()

def get_dynamic_prompt() -> str:
    """Builds a system prompt using rules fetched from the database."""
    ontology = db.get_ontology()
    entity_types = ", ".join(ontology.get('entity_types', []))
    
    relations_list = []
    for triple in ontology.get('allowed_triples', []):
        relations_list.append(f"{triple['relation']}: {triple['source']} -> {triple['target']}")
    relations_str = "\n".join(relations_list)
    
    rules_list = ontology.get('extraction_rules', [])
    # Strip any leading numbers from the ontology rules to prevent "1. 1." errors
    cleaned_rules = [re.sub(r'^\d+\.\s*', '', r) for r in rules_list]
    
    # Combine DB rules with strict system rules
    all_rules = cleaned_rules + [
        "TRUST LAYER: For EVERY entity and relation, you MUST provide 'source_text' (verbatim quote) and 'confidence' (0.0 to 1.0).",
        "QUANT LAYER: Identify numeric financial metrics (Revenue, PAT, Market Size) and return them in the 'quant_data' list.",
        "AUTO-DISCOVERY: If you find an important entity or relation type NOT on the list above, return it in the 'discoveries' list.",
        "TEMPORAL NORMALIZATION: For the 'period' field in quant_data, use standard YYYY-QX or YYYY-MM or YYYY-FY formats.",
        "REFERENTIAL INTEGRITY (FATAL ERROR): Every single 'source_temp_id' and 'target_temp_id' used in relations MUST EXACTLY MATCH a 'temp_id' defined in the 'entities' list. NEVER hallucinate or misspell IDs.",
        "GEOSPATIAL ABSTRACTION: If a text mentions a Region (e.g., Southeast Asia) and its Countries (Vietnam, Cambodia), connect the Company ONLY to the Region. Connect the Countries to the Region using 'PART_OF'. DO NOT connect the Company directly to the Countries.",
        "HYPOTHETICAL NODES (ZERO TOLERANCE FOR ISLANDS): You MUST create intermediate nodes (like 'Management', 'ProductPortfolio', or 'Market') to ensure EVERY node connects to the ROOT. For example, a Market node MUST connect to the Company via 'OPERATES_IN_MARKET'. NEVER leave a node floating.",
        "PRODUCT HIERARCHY (STRICT): [Company] -> HAS_PRODUCT_PORTFOLIO -> [ProductPortfolio node] -> HAS_PRODUCT_DOMAIN/FAMILY/LINE -> [Brand/Product node]. NEVER link products directly to the Company. ALWAYS use the Portfolio hierarchy.",
        "NO BYPASS (CRITICAL): NEVER create a direct relation from a sub-node (e.g., PERSON, BRAND, ROLE) to the ROOT if a hierarchical path exists. Direct shortcuts are FORBIDDEN."
    ]
    
    rules_str = "\n".join([f"{i+1}. {rule}" for i, rule in enumerate(all_rules)])
    
    return f"""You are a professional investment intelligence system. Convert unstructured text into a high-trust knowledge graph.

### 1. ALLOWED ENTITIES (Use EXACTLY these labels)
{entity_types}

### 2. ALLOWED RELATIONS (Strict Mapping Only)
{relations_str}

### 3. EXTRACTION RULES
{rules_str}

### 4. OUTPUT FORMAT (Strict JSON)
{{
    "thought_process": "Analyze the text for hierarchy, trust, and missing types...",
    "entities": [
        {{
            "temp_id": "e_root",
            "entity_type": "LegalEntity",
            "canonical_name": "Official Name",
            "attributes": {{ "context": "Detailed explanation of why this entity matters in this context" }},
            "source_text": "...",
            "confidence": 0.95,
            "evidence": [{{ "evidence_quote": "..." }}]
        }}
    ],
    "relations": [
        {{
            "source_temp_id": "...",
            "target_temp_id": "...",
            "relation_type": "...",
            "source_text": "...",
            "confidence": 0.9,
            "evidence": [{{ "evidence_quote": "..." }}]
        }}
    ],
    "quant_data": [
        {{ "metric": "Revenue", "value": 2500, "unit": "Cr", "period": "2026-Q3", "subject_id": "e_root" }},
        {{ "metric": "PAT", "value": 15.5, "unit": "Billion", "period": "2024-FY", "subject_id": "e_root" }}
    ],
    "discoveries": [
        {{ "type": "ENTITY", "name": "...", "suggested_label": "NewEntityType", "context": "..." }},
        {{ "type": "RELATION", "name": "...", "suggested_label": "NEW_RELATION", "source_type": "TypeA", "target_type": "TypeB", "context": "..." }}
    ],
    "analysis_attributes": {{ ... }},
    "llm_analysis_summary": "..."
}}
"""

def _mock_extraction_response(text: str, document_id: str, document_name: str, section_ref: str) -> str:
    """Generate a mock extraction response for demo/fallback when LLM API fails."""
    # Simple keyword matching to extract entities
    entities = []
    relations = []
    quant_data = [] # New field
    discoveries = [] # New field
    entity_id_map = {}
    next_id = 1
    
    # Extract company names (heuristic: capitalized words before "Corp", "Inc", "Ltd", etc.)
    import re
    company_pattern = r'\b([A-Z][a-z\s]*(?:Corp|Inc|Ltd|LLC|AG|SE|Co))\b'
    companies = re.findall(company_pattern, text)
    
    # Create entity candidates for companies first
    for company in set(companies):
        eid = f"e{next_id}"
        entity_id_map[company] = eid
        entities.append({
            "temp_id": eid,
            "entity_type": "LegalEntity",
            "canonical_name": company,
            "aliases": [],
            "attributes": {},
            "evidence": [{
                "document_id": document_id,
                "document_name": document_name,
                "section_ref": section_ref,
                "evidence_quote": company
            }],
            "confidence": 0.9
        })
        next_id += 1

    # Extract additional company mentions that may not include typical suffixes
    # BUT: Skip "published by <Company>" since those are sources/citations, not entities
    company_cues = []
    # Removed "published by" pattern to avoid extracting sources as entities
    for pat in company_cues:
        for match in re.findall(pat, text):
            if match in entity_id_map:
                continue
            eid = f"e{next_id}"
            entity_id_map[match] = eid
            entities.append({
                "temp_id": eid,
                "entity_type": "LegalEntity",
                "canonical_name": match,
                "aliases": [],
                "attributes": {},
                "evidence": [{
                    "document_id": document_id,
                    "document_name": document_name,
                    "section_ref": section_ref,
                    "evidence_quote": match
                }],
                "confidence": 0.8
            })
            next_id += 1

    # Extract persons primarily by role/leadership cues to avoid catching dates or geographies as people
    stop_first_words = {"in", "on", "at", "the", "a", "an", "according", "per"}
    month_words = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"}

    role_titles = ["CEO", "COO", "CTO", "CFO", "President", "VP", "Vice President", "Manager"]
    # Match roles like "CEO John Donahoe" or "COO Andy Campion".
    # Avoid case-insensitive matching to prevent unintended captures like "and COO".
    role_pattern = re.compile(rf"\b(?:{'|'.join(role_titles)})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)")
    for match in role_pattern.finditer(text):
        person_name = match.group(1).strip()
        first_word = person_name.split()[0].lower()
        if first_word in stop_first_words or first_word in month_words:
            continue
        if person_name not in entity_id_map:
            eid = f"e{next_id}"
            entity_id_map[person_name] = eid
            entities.append({
                "temp_id": eid,
                "entity_type": "Person",
                "canonical_name": person_name,
                "aliases": [],
                "attributes": {},
                "evidence": [{
                    "document_id": document_id,
                    "document_name": document_name,
                    "section_ref": section_ref,
                    "evidence_quote": person_name
                }],
                "confidence": 0.85
            })
            next_id += 1

    # Extract geographies via common regional keywords and explicit place names
    # Geographic hierarchy: Regions -> Countries -> Cities
    geo_hierarchy = {
        "Southeast Asia": ["Vietnam", "Indonesia", "Cambodia", "Thailand", "Philippines", "Singapore", "Malaysia"],
        "Europe": ["Germany", "France", "UK", "Spain", "Italy"],
    }
    
    extracted_geos = {}  # {geo: "region" or "country"}
    country_to_region = {}  # {country: parent_region}
    
    # Scan text for all geographies - prioritize longer/more specific names first
    # to avoid matching "Asia" when "Southeast Asia" is present
    geo_priority = [
        "Southeast Asia", "East Asia", "South Asia", "Central Asia",
        "Europe", "North America", "South America",
        "Vietnam", "Indonesia", "Cambodia", "Thailand", "Philippines", "Singapore", "Malaysia",
        "Germany", "France", "UK", "Spain", "Italy", "India", "China", "Japan", "Korea"
    ]
    
    for geo in geo_priority:
        if re.search(rf"\b{re.escape(geo)}\b", text, re.IGNORECASE):
            # Determine if this is a region or country
            is_region = any(geo in geos for geos in geo_hierarchy.keys())
            extracted_geos[geo] = "region" if is_region else "country"
            
            # Track parent region for countries
            if not is_region:
                for region, countries in geo_hierarchy.items():
                    if geo in countries:
                        country_to_region[geo] = region
                        # Ensure parent region is also extracted
                        if region not in extracted_geos:
                            extracted_geos[region] = "region_inferred"
                        break
    
    # Create Geography entities
    regions_created = set()
    for geo, geo_type in extracted_geos.items():
        if geo not in entity_id_map:
            eid = f"e{next_id}"
            entity_id_map[geo] = eid
            entities.append({
                "temp_id": eid,
                "entity_type": "Geography",
                "canonical_name": geo,
                "aliases": [],
                "attributes": {},
                "evidence": [{
                    "document_id": document_id,
                    "document_name": document_name,
                    "section_ref": section_ref,
                    "evidence_quote": geo
                }],
                "confidence": 0.9 if geo_type in ["region", "region_inferred"] else 0.85
            })
            if geo_type in ["region", "region_inferred"]:
                regions_created.add(geo)
            next_id += 1
    
    # Create parent-child geography relations (PART_OF)
    # Link countries to their parent regions
    for country, parent_region in country_to_region.items():
        if country in entity_id_map and parent_region in entity_id_map:
            relations.append({
                "source_temp_id": entity_id_map[country],
                "target_temp_id": entity_id_map[parent_region],
                "relation_type": "PART_OF",
                "evidence": [{
                    "document_id": document_id,
                    "document_name": document_name,
                    "section_ref": section_ref,
                    "evidence_quote": f"{country} is in {parent_region}"
                }],
                "confidence": 0.9
            })
    
    # Determine the primary company to anchor leadership relations
    company_ids = [entity_id_map[c] for c in companies if c in entity_id_map]
    primary_company_id = company_ids[0] if company_ids else None

    # Extract roles and leadership
    role_titles = ["CEO", "COO", "CTO", "CFO", "President", "VP", "Manager"]
    role_before_pattern = re.compile(rf"\b({'|'.join(role_titles)})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)")
    role_after_pattern1 = re.compile(rf"\b([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:is|serves as)\s+(?:the\s+)?({'|'.join(role_titles)})\b")
    role_after_pattern2 = re.compile(rf"\b([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:is|are|was|were|serves as|served as)\s+(?:the\s+)?(?:current\s+)?({'|'.join(role_titles)})\b")


    # Create Management node for the primary company
    management_id = None
    if primary_company_id and companies:
        company_name = companies[0]
        management_id = f"e{next_id}"
        mgmt_name = f"{company_name} Management"
        entities.append({
            "temp_id": management_id,
            "entity_type": "Management",
            "canonical_name": mgmt_name,
            "aliases": [],
            "attributes": {},
            "evidence": [{
                "document_id": document_id,
                "document_name": document_name,
                "section_ref": section_ref,
                "evidence_quote": f"Management of {company_name}"
            }],
            "confidence": 0.9
        })
        relations.append({
            "source_temp_id": primary_company_id,
            "target_temp_id": management_id,
            "relation_type": "HAS_MANAGEMENT",
            "evidence": [{
                "document_id": document_id,
                "document_name": document_name,
                "section_ref": section_ref,
                "evidence_quote": f"{company_name} management"
            }],
            "confidence": 0.9
        })
        next_id += 1

    # Extract roles and leadership
    seen_roles = set()
    for pattern in (role_before_pattern, role_after_pattern1, role_after_pattern2):
        for match in re.findall(pattern, text):
            if len(match) == 2 and match[0] in role_titles:
                role_title, person_name = match
            else:
                person_name, role_title = match

            person_name = person_name.strip()
            role_title = role_title.strip()
            role_key = (person_name, role_title)
            if role_key in seen_roles:
                continue
            seen_roles.add(role_key)

            # Create Designation entity (role as a position under Management)
            designation_id = f"e{next_id}"
            next_id += 1
            entities.append({
                "temp_id": designation_id,
                "entity_type": "Role",
                "canonical_name": role_title,
                "aliases": [],
                "attributes": {},
                "evidence": [{
                    "document_id": document_id,
                    "document_name": document_name,
                    "section_ref": section_ref,
                    "evidence_quote": f"{person_name} is {role_title}"
                }],
                "confidence": 0.9
            })

            # Management -> Role (HAS_ROLE)
            if management_id:
                relations.append({
                    "source_temp_id": management_id,
                    "target_temp_id": designation_id,
                    "relation_type": "HAS_ROLE",
                    "evidence": [{
                        "document_id": document_id,
                        "document_name": document_name,
                        "section_ref": section_ref,
                        "evidence_quote": f"{mgmt_name} has {role_title}"
                    }],
                    "confidence": 0.9
                })

            # Role -> Person (HELD_BY)
            if person_name in entity_id_map:
                person_id = entity_id_map[person_name]
                relations.append({
                    "source_temp_id": designation_id,
                    "target_temp_id": person_id,
                    "relation_type": "HELD_BY",
                    "evidence": [{
                        "document_id": document_id,
                        "document_name": document_name,
                        "section_ref": section_ref,
                        "evidence_quote": f"{role_title} held by {person_name}"
                    }],
                    "confidence": 0.9
                })
    
    # Extract acquisitions/partnerships with better patterns
    acq_keywords = ["acquired", "acquired by", "acquires", "acquisition of", "took over", "merged with"]
    for keyword in acq_keywords:
        if keyword.lower() in text.lower():
            # Pattern: "X acquired Y" or "X acquired Y Inc"
            acq_pattern = rf'([A-Z][a-z\s]*?(?:Corp|Inc|Ltd|LLC|Co)?(?:\s+Inc)?(?:\s+Ltd)?)\s+(?:{re.escape(keyword)})\s+([A-Z][a-zA-Z\s]*?(?:Inc|Corp|Ltd|LLC|Co)?)'
            acquisitions = re.findall(acq_pattern, text)
            for acquirer, target in acquisitions:
                acquirer = acquirer.strip()
                target = target.strip()
                # Avoid dummy entries
                if len(acquirer) < 3 or len(target) < 3 or acquirer == target:
                    continue
                # Create target entity if not exists
                if target not in entity_id_map:
                    eid = f"e{next_id}"
                    entity_id_map[target] = eid
                    entities.append({
                        "temp_id": eid,
                        "entity_type": "ExternalOrganization",
                        "canonical_name": target,
                        "aliases": [],
                        "attributes": {},
                        "evidence": [{
                            "document_id": document_id,
                            "document_name": document_name,
                            "section_ref": section_ref,
                            "evidence_quote": target
                        }],
                        "confidence": 0.75
                    })
                    next_id += 1
                # Create acquisition relation
                if acquirer in entity_id_map and target in entity_id_map:
                    relations.append({
                        "source_temp_id": entity_id_map[acquirer],
                        "target_temp_id": entity_id_map[target],
                        "relation_type": "ACQUIRED_STAKE_IN",
                        "evidence": [{
                            "document_id": document_id,
                            "document_name": document_name,
                            "section_ref": section_ref,
                            "evidence_quote": f"{acquirer} {keyword} {target}"
                        }],
                        "confidence": 0.85
                    })
            break  # Avoid duplicate processing
    
    # Redundant region relations removed to follow NO REDUNDANCY rule.
    # Geography hierarchy is now handled via Country -> PART_OF -> Region.
    
    # Extract Sectors
    sectors = ["Financial Services", "Technology", "Healthcare", "Consumer Goods", "Energy"]
    for sector in sectors:
        if re.search(rf"\b{re.escape(sector)}\b", text, re.IGNORECASE):
            if sector not in entity_id_map:
                eid = f"e{next_id}"
                entity_id_map[sector] = eid
                entities.append({
                    "temp_id": eid,
                    "entity_type": "Sector",
                    "canonical_name": sector,
                    "evidence": [{"document_id": document_id, "document_name": document_name, "section_ref": section_ref, "evidence_quote": sector}],
                    "confidence": 0.8
                })
                next_id += 1
            if primary_company_id:
                relations.append({
                    "source_temp_id": primary_company_id,
                    "target_temp_id": entity_id_map[sector],
                    "relation_type": "BELONGS_TO_SECTOR",
                    "evidence": [{"document_id": document_id, "document_name": document_name, "section_ref": section_ref, "evidence_quote": f"firm in {sector}"}],
                    "confidence": 0.8
                })

    return json.dumps({
        "thought_process": "Mock extraction using regex patterns (LLM API unavailable)",
        "entities": entities,
        "relations": relations,
        "quant_data": quant_data,
        "discoveries": discoveries,
        "abstentions": [],
        "analysis_attributes": {
            "signal_type": "neutral",
            "sentiment": "neutral",
            "metric_type": ["N/A"]
        },
        "llm_analysis_summary": "Mock analysis generated from regex patterns."
    })


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
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        finish_reason = result["choices"][0].get("finish_reason", "stop")
    except Exception as e:
        print(f"[ERROR] LLM API failed: {e}.")
        raise Exception(f"API Rate Limit or Connection Error. Please wait a few minutes and try again. Details: {e}")
    
    # Strip fences
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()
    
    # Parse JSON
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = _repair_truncated_json(content)

    # Load dynamic ontology for validation
    ontology = db.get_ontology()
    valid_entity_types = set(ontology.get("entity_types", []))
    valid_relation_types = set(ontology.get("relation_types", []))

    # Parse entities
    entities = []
    skipped_entities = []
    for e in parsed.get("entities", []):
        try:
            ent_cand = EntityCandidate(**e)
            if ent_cand.entity_type not in valid_entity_types:
                # We allow it but mark as discovery if not in valid types
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
