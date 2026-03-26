from __future__ import annotations
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime

"""
CONTRACT LAYER: Defines the strict Pydantic schemas for LLM extraction and graph storage.
Ensures that every piece of data (Entities, Relations, Discoveries) follows a predictable structure.
"""

# ════════════════════════════════════════════════════════════════════════
# REVIEW STATE
# ════════════════════════════════════════════════════════════════════════

class ReviewState(str, Enum):
    """Assertion review status."""
    AUTO_ACCEPTED  = "auto_accepted"
    HUMAN_ACCEPTED = "human_accepted"
    REJECTED       = "rejected"
    PENDING        = "pending"

# ════════════════════════════════════════════════════════════════════════
# DATA MODELS (Pydantic)
# ════════════════════════════════════════════════════════════════════════

class EvidenceRef(BaseModel):
    document_id: str = "doc_1"
    document_name: str = "Unknown"
    section_ref: str = "chunk"
    evidence_quote: str = "No quote provided."
    as_of_date: Optional[str] = None

class EntityCandidate(BaseModel):
    """Incoming entity candidate from extraction."""
    temp_id: str
    entity_type: str 
    canonical_name: str
    aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    description: str | None = Field(default="No description provided.")
    short_info: str | None = Field(default="N/A")
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: float = 1.0
    source_text: Optional[str] = None
    notes: Optional[str] = None

class RelationCandidate(BaseModel):
    """Incoming relation candidate from extraction."""
    source_temp_id: str
    target_temp_id: str
    relation_type: str
    attributes: dict[str, Any] = Field(default_factory=dict)
    weight: float = 1.0
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: float = 1.0
    source_text: Optional[str] = None
    notes: Optional[str] = None

class DocSpecificAttributes(BaseModel):
    has_tables: bool = False
    has_images: bool = False
    tables_html: list[str] = Field(default_factory=list)
    images_descriptions: list[str] = Field(default_factory=list)

class AnalysisAttributes(BaseModel):
    signal_type: Optional[str] = "neutral"
    time_horizon: Optional[str] = None
    metric_type: list[str] = Field(default_factory=list)
    sentiment: Optional[str] = "neutral"

class GoldenChunk(BaseModel):
    chunk_id: str
    doc_id: str
    company_ticker: str
    company_name: str
    sector: str
    fiscal_year: int
    fiscal_period: str
    date_iso: str
    doc_type: str = "PRESENTATION"
    filename: str
    page_number: int
    content: str
    doc_specific_attributes: DocSpecificAttributes
    analysis_attributes: AnalysisAttributes
    normalized_metrics: dict[str, list[str]] = Field(default_factory=dict)
    llm_analysis_summary: Optional[str] = None
    reasoning_details: Optional[Any] = None

class QuantMetric(BaseModel):
    metric: str
    value: float
    unit: Optional[str] = None
    period: Optional[str] = None
    subject_id: str  # temp_id of the entity this belongs to

class OntologyDiscovery(BaseModel):
    type: str  # 'ENTITY' or 'RELATION'
    name: str
    suggested_label: str
    context: str
    source_type: Optional[str] = None  # For RELATION
    target_type: Optional[str] = None  # For RELATION

class ExtractionPayload(BaseModel):
    thought_process: str = ""
    ontology_version: str = "v1.0.0"
    source_document_id: str
    source_document_name: str
    entities: list[EntityCandidate]
    relations: list[RelationCandidate]
    quant_data: list[QuantMetric] = Field(default_factory=list)
    discoveries: list[OntologyDiscovery] = Field(default_factory=list)
    abstentions: list[str] = Field(default_factory=list)
    analysis_attributes: Optional[AnalysisAttributes] = None
    llm_analysis_summary: Optional[str] = None
    reasoning_details: Optional[Any] = None

class EntityMaster(BaseModel):
    entity_id: str
    entity_type: str
    canonical_name: str
    aliases: list[str] = Field(default_factory=list)
    description: Optional[str] = None
    short_info: Optional[str] = "N/A"
    attributes: dict[str, Any] = Field(default_factory=dict)

class RelationMaster(BaseModel):
    relation_id: str
    relation_type: str
    source_entity_id: str
    target_entity_id: str
    weight: float = 1.0
    attributes: dict[str, Any] = Field(default_factory=dict)

class EntityAssertion(BaseModel):
    assertion_id: str
    entity_id: str
    asserted_fields: dict[str, Any] = Field(default_factory=dict)
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: float = 1.0
    review_state: ReviewState = ReviewState.PENDING

class RelationAssertion(BaseModel):
    assertion_id: str
    relation_id: str
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: float = 1.0
    review_state: ReviewState = ReviewState.PENDING
    asserted_attributes: dict[str, Any] = Field(default_factory=dict)

ZONE1_ONTOLOGY_VERSION = "v1.0.0"
