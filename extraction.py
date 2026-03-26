import os
import json
import logging
import re
from typing import List, Dict, Any, Optional
from models import ExtractionPayload, EntityCandidate, RelationCandidate
from database import DatabaseManager

logger = logging.getLogger(__name__)

def call_llm(prompt: str, model: str = None) -> str:
    """
    Calls the LLM via OpenRouter.
    """
    import openai
    api_key = os.getenv("LLM_API_KEY")
    base_url = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
    model = model or os.getenv("LLM_MODEL", "google/gemini-2.0-flash-lite-001")

    client = openai.OpenAI(api_key=api_key, base_url=base_url)
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"LLM Call failed: {e}")
        return "{}"

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
Process the text below. 
OUTPUT MUST BE A SINGLE JSON OBJECT with EXACTLY these keys:
- "thought_process": Reasoning for the hierarchy.
- "entities": List of extracted components with [temp_id, canonical_name, entity_type, short_info].
- "relations": List of connections with [source_temp_id, relation_type, target_temp_id, source_text, confidence].

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
    
    try:
        data = json.loads(raw_json)
        
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
