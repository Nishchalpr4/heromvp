"""
Zone 1 — Entity Zone: Pydantic Models & Ontology
=================================================
Defines the canonical entity types, relation types, allowed relation triples,
and all data models for the Entity Zone of the investment-analysis knowledge graph.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime


# ════════════════════════════════════════════════════════════════════════
# CORE ENUMS
# ════════════════════════════════════════════════════════════════════════

class EntityType(str, Enum):
    LEGAL_ENTITY         = "LegalEntity"
    EXTERNAL_ORGANIZATION = "ExternalOrganization"
    BUSINESS_UNIT        = "BusinessUnit"
    SECTOR               = "Sector"
    INDUSTRY             = "Industry"
    SUB_INDUSTRY         = "SubIndustry"
    END_MARKET           = "EndMarket"
    CHANNEL              = "Channel"
    PRODUCT_DOMAIN       = "ProductDomain"
    PRODUCT_FAMILY       = "ProductFamily"
    PRODUCT_LINE         = "ProductLine"
    SITE                 = "Site"
    GEOGRAPHY            = "Geography"
    PERSON               = "Person"
    ROLE                 = "Role"
    TECHNOLOGY           = "Technology"
    CAPABILITY           = "Capability"
    BRAND                = "Brand"
    INITIATIVE           = "Initiative"
    FINANCIAL            = "Financial"
    PROGRAM              = "Program"
    MANAGEMENT           = "Management"
    COMPETITORS          = "Competitors"
    PRODUCT_PORTFOLIO     = "ProductPortfolio"


class RelationType(str, Enum):
    HAS_MANAGEMENT      = "HAS_MANAGEMENT"
    HAS_COMPETITORS     = "HAS_COMPETITORS"
    HAS_COMPETITOR      = "HAS_COMPETITOR"
    PARENT_OF           = "PARENT_OF"
    SUBSIDIARY_OF       = "SUBSIDIARY_OF"
    JV_WITH             = "JV_WITH"
    ASSOCIATE_OF        = "ASSOCIATE_OF"
    ACQUIRED_ENTITY     = "ACQUIRED_ENTITY"
    ACQUIRED_STAKE_IN   = "ACQUIRED_STAKE_IN"
    LICENSES_TECH_FROM  = "LICENSES_TECH_FROM"
    PARTNERS_WITH       = "PARTNERS_WITH"
    COMPETES_WITH       = "COMPETES_WITH"
    HAS_BUSINESS_UNIT       = "HAS_BUSINESS_UNIT"
    PART_OF_BUSINESS_UNIT   = "PART_OF_BUSINESS_UNIT"
    PART_OF                 = "PART_OF"
    HAS_PRODUCT_DOMAIN      = "HAS_PRODUCT_DOMAIN"
    HAS_PRODUCT_FAMILY      = "HAS_PRODUCT_FAMILY"
    HAS_PRODUCT_LINE        = "HAS_PRODUCT_LINE"
    BELONGS_TO_DOMAIN       = "BELONGS_TO_DOMAIN"
    BELONGS_TO_FAMILY       = "BELONGS_TO_FAMILY"
    APPLIES_TO_END_MARKET   = "APPLIES_TO_END_MARKET"
    SERVES_CHANNEL          = "SERVES_CHANNEL"
    LOCATED_IN          = "LOCATED_IN"
    OPERATES_IN         = "OPERATES_IN"
    OWNS_SITE           = "OWNS_SITE"
    OPERATES_SITE       = "OPERATES_SITE"
    HOLDS_ROLE          = "HOLDS_ROLE"
    HAS_ROLE            = "HAS_ROLE"
    HELD_BY             = "HELD_BY"
    MANAGEMENT_OF       = "MANAGEMENT_OF"
    COMPETITORS_OF      = "COMPETITORS_OF"
    LEADS               = "LEADS"
    ROLE_WITHIN         = "ROLE_WITHIN"
    HAS_CAPABILITY          = "HAS_CAPABILITY"
    DEVELOPS_TECHNOLOGY     = "DEVELOPS_TECHNOLOGY"
    USES_TECHNOLOGY         = "USES_TECHNOLOGY"
    BELONGS_TO_INDUSTRY     = "BELONGS_TO_INDUSTRY"
    BELONGS_TO_SECTOR       = "BELONGS_TO_SECTOR"
    RUNS_PROGRAM                = "RUNS_PROGRAM"
    PROGRAM_LOCATED_IN          = "PROGRAM_LOCATED_IN"
    PROGRAM_FOR_PRODUCT_LINE    = "PROGRAM_FOR_PRODUCT_LINE"
    MAPPED_TO                   = "MAPPED_TO"
    REPORTED_BY                 = "REPORTED_BY"
    HAS_PRODUCT_PORTFOLIO       = "HAS_PRODUCT_PORTFOLIO"
    HAS_INITIATIVE              = "HAS_INITIATIVE"
    WORKS_WITH                  = "WORKS_WITH"
    SUCCEEDS                    = "SUCCEEDS"


ALLOWED_RELATION_TRIPLES: set[tuple[EntityType, RelationType, EntityType]] = {
    (EntityType.LEGAL_ENTITY, RelationType.PARENT_OF, EntityType.LEGAL_ENTITY),
    (EntityType.LEGAL_ENTITY, RelationType.SUBSIDIARY_OF, EntityType.LEGAL_ENTITY),
    (EntityType.LEGAL_ENTITY, RelationType.JV_WITH, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.LEGAL_ENTITY, RelationType.JV_WITH, EntityType.LEGAL_ENTITY),
    (EntityType.LEGAL_ENTITY, RelationType.PARTNERS_WITH, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.LEGAL_ENTITY, RelationType.PARTNERS_WITH, EntityType.LEGAL_ENTITY),
    (EntityType.LEGAL_ENTITY, RelationType.COMPETES_WITH, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.LEGAL_ENTITY, RelationType.COMPETES_WITH, EntityType.LEGAL_ENTITY),
    (EntityType.LEGAL_ENTITY, RelationType.HAS_BUSINESS_UNIT, EntityType.BUSINESS_UNIT),
    (EntityType.BUSINESS_UNIT, RelationType.HAS_BUSINESS_UNIT, EntityType.BUSINESS_UNIT),
    (EntityType.LEGAL_ENTITY, RelationType.HAS_PRODUCT_PORTFOLIO, EntityType.PRODUCT_PORTFOLIO),
    (EntityType.PRODUCT_PORTFOLIO, RelationType.HAS_PRODUCT_DOMAIN, EntityType.PRODUCT_DOMAIN),
    (EntityType.PRODUCT_DOMAIN, RelationType.HAS_PRODUCT_FAMILY, EntityType.PRODUCT_FAMILY),
    (EntityType.PRODUCT_FAMILY, RelationType.HAS_PRODUCT_LINE, EntityType.PRODUCT_LINE),
    (EntityType.LEGAL_ENTITY, RelationType.OPERATES_IN, EntityType.GEOGRAPHY),
    (EntityType.GEOGRAPHY, RelationType.PART_OF, EntityType.GEOGRAPHY),
    (EntityType.PERSON, RelationType.HOLDS_ROLE, EntityType.ROLE),
    (EntityType.ROLE, RelationType.ROLE_WITHIN, EntityType.LEGAL_ENTITY),
    (EntityType.ROLE, RelationType.ROLE_WITHIN, EntityType.BUSINESS_UNIT),
    (EntityType.PERSON, RelationType.LEADS, EntityType.LEGAL_ENTITY),
    (EntityType.PERSON, RelationType.PART_OF, EntityType.BUSINESS_UNIT),
    (EntityType.SITE, RelationType.LOCATED_IN, EntityType.GEOGRAPHY),
    (EntityType.LEGAL_ENTITY, RelationType.OWNS_SITE, EntityType.SITE),
    (EntityType.BRAND, RelationType.PART_OF, EntityType.LEGAL_ENTITY),
    (EntityType.INITIATIVE, RelationType.PART_OF, EntityType.LEGAL_ENTITY),
    (EntityType.FINANCIAL, RelationType.REPORTED_BY, EntityType.LEGAL_ENTITY),
    (EntityType.FINANCIAL, RelationType.PART_OF, EntityType.INITIATIVE),
    (EntityType.BUSINESS_UNIT, RelationType.MAPPED_TO, EntityType.LEGAL_ENTITY),
    (EntityType.PRODUCT_LINE, RelationType.APPLIES_TO_END_MARKET, EntityType.END_MARKET),

    # Sector / Industry / Market
    (EntityType.LEGAL_ENTITY, RelationType.BELONGS_TO_SECTOR, EntityType.SECTOR),
    (EntityType.LEGAL_ENTITY, RelationType.BELONGS_TO_INDUSTRY, EntityType.INDUSTRY),
    (EntityType.BUSINESS_UNIT, RelationType.BELONGS_TO_SECTOR, EntityType.SECTOR),
    (EntityType.BUSINESS_UNIT, RelationType.BELONGS_TO_INDUSTRY, EntityType.INDUSTRY),
    (EntityType.LEGAL_ENTITY, RelationType.OPERATES_IN, EntityType.END_MARKET),
    (EntityType.BUSINESS_UNIT, RelationType.OPERATES_IN, EntityType.END_MARKET),

    # Geography / Site / Infrastructure
    (EntityType.LEGAL_ENTITY, RelationType.OPERATES_IN, EntityType.GEOGRAPHY),
    (EntityType.BUSINESS_UNIT, RelationType.OPERATES_IN, EntityType.GEOGRAPHY),
    (EntityType.GEOGRAPHY, RelationType.PART_OF, EntityType.GEOGRAPHY),
    (EntityType.SITE, RelationType.LOCATED_IN, EntityType.GEOGRAPHY),
    (EntityType.LEGAL_ENTITY, RelationType.OWNS_SITE, EntityType.SITE),
    (EntityType.LEGAL_ENTITY, RelationType.LOCATED_IN, EntityType.GEOGRAPHY),

    # Management and Competitors
    (EntityType.LEGAL_ENTITY, RelationType.HAS_MANAGEMENT, EntityType.MANAGEMENT),
    (EntityType.LEGAL_ENTITY, RelationType.HAS_COMPETITORS, EntityType.COMPETITORS),
    (EntityType.COMPETITORS, RelationType.HAS_COMPETITOR, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.COMPETITORS, RelationType.HAS_COMPETITOR, EntityType.LEGAL_ENTITY),
    (EntityType.MANAGEMENT, RelationType.HAS_ROLE, EntityType.ROLE),
    (EntityType.ROLE, RelationType.HELD_BY, EntityType.PERSON),
    (EntityType.ROLE, RelationType.ROLE_WITHIN, EntityType.LEGAL_ENTITY),
    (EntityType.ROLE, RelationType.ROLE_WITHIN, EntityType.BUSINESS_UNIT),

    # Product and Brand hierarchy
    (EntityType.LEGAL_ENTITY, RelationType.HAS_PRODUCT_PORTFOLIO, EntityType.PRODUCT_PORTFOLIO),
    (EntityType.LEGAL_ENTITY, RelationType.HAS_BUSINESS_UNIT, EntityType.BUSINESS_UNIT),
    (EntityType.PRODUCT_PORTFOLIO, RelationType.HAS_PRODUCT_DOMAIN, EntityType.PRODUCT_DOMAIN),
    (EntityType.PRODUCT_DOMAIN, RelationType.HAS_PRODUCT_FAMILY, EntityType.PRODUCT_FAMILY),
    (EntityType.PRODUCT_FAMILY, RelationType.HAS_PRODUCT_LINE, EntityType.PRODUCT_LINE),
    (EntityType.BRAND, RelationType.PART_OF, EntityType.LEGAL_ENTITY),
    (EntityType.BRAND, RelationType.PART_OF, EntityType.PRODUCT_LINE),

    # Tech and Capabilities (Meaningful structural links)
    (EntityType.LEGAL_ENTITY, RelationType.HAS_CAPABILITY, EntityType.CAPABILITY),
    (EntityType.BUSINESS_UNIT, RelationType.HAS_CAPABILITY, EntityType.CAPABILITY),
    (EntityType.LEGAL_ENTITY, RelationType.USES_TECHNOLOGY, EntityType.TECHNOLOGY),
    (EntityType.LEGAL_ENTITY, RelationType.DEVELOPS_TECHNOLOGY, EntityType.TECHNOLOGY),
    (EntityType.PRODUCT_LINE, RelationType.USES_TECHNOLOGY, EntityType.TECHNOLOGY),
    
    # Initiatives and Programs
    (EntityType.LEGAL_ENTITY, RelationType.HAS_INITIATIVE, EntityType.INITIATIVE),
    (EntityType.INITIATIVE, RelationType.PART_OF, EntityType.LEGAL_ENTITY),
    (EntityType.INITIATIVE, RelationType.LEADS, EntityType.PERSON),
    (EntityType.PERSON, RelationType.LEADS, EntityType.INITIATIVE),
    (EntityType.ROLE, RelationType.LEADS, EntityType.INITIATIVE),
    (EntityType.INITIATIVE, RelationType.OPERATES_IN, EntityType.GEOGRAPHY),
    (EntityType.PROGRAM, RelationType.PART_OF, EntityType.INITIATIVE),

    # External Organizations and Partnerships
    (EntityType.LEGAL_ENTITY, RelationType.WORKS_WITH, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.LEGAL_ENTITY, RelationType.PARTNERS_WITH, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.EXTERNAL_ORGANIZATION, RelationType.REPORTED_BY, EntityType.EXTERNAL_ORGANIZATION),
    (EntityType.LEGAL_ENTITY, RelationType.REPORTED_BY, EntityType.EXTERNAL_ORGANIZATION),
    
    # Other
    (EntityType.INITIATIVE, RelationType.PART_OF, EntityType.LEGAL_ENTITY),
    (EntityType.FINANCIAL, RelationType.REPORTED_BY, EntityType.LEGAL_ENTITY),
    (EntityType.PERSON, RelationType.PART_OF, EntityType.BUSINESS_UNIT),
    (EntityType.PERSON, RelationType.SUCCEEDS, EntityType.PERSON),
}

def validate_relation_triple(st: EntityType, rt: RelationType, tt: EntityType) -> bool:
    return (st, rt, tt) in ALLOWED_RELATION_TRIPLES

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
# DATA MODELS
# ════════════════════════════════════════════════════════════════════════

class EvidenceRef(BaseModel):
    document_id: str = "doc_1"
    document_name: str = "Unknown"
    section_ref: str = "chunk"
    evidence_quote: str
    as_of_date: Optional[str] = None

class EntityCandidate(BaseModel):
    temp_id: str
    entity_type: EntityType 
    canonical_name: str
    aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: float = 1.0
    source_text: Optional[str] = None
    notes: Optional[str] = None

class RelationCandidate(BaseModel):
    source_temp_id: str
    target_temp_id: str
    relation_type: RelationType
    attributes: dict[str, Any] = Field(default_factory=dict)
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

class EntityMaster(BaseModel):
    entity_id: str
    entity_type: EntityType
    canonical_name: str
    aliases: list[str] = Field(default_factory=list)
    description: Optional[str] = None
    attributes: dict[str, Any] = Field(default_factory=dict)

class RelationMaster(BaseModel):
    relation_id: str
    relation_type: RelationType
    source_entity_id: str
    target_entity_id: str
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

ENTITY_TYPE_COLORS: dict[str, str] = {
    "LegalEntity":          "#4A90D9",
    "ExternalOrganization": "#E67E22",
    "BusinessUnit":         "#27AE60",
    "Sector":               "#8E44AD",
    "Industry":             "#2C3E50",
    "SubIndustry":          "#16A085",
    "EndMarket":            "#D35400",
    "Channel":              "#C0392B",
    "ProductDomain":        "#2980B9",
    "ProductFamily":        "#3498DB",
    "ProductLine":          "#1ABC9C",
    "Site":                 "#E74C3C",
    "Geography":            "#F39C12",
    "Person":               "#9B59B6",
    "Role":                 "#7F8C8D",
    "Technology":           "#00BCD4",
    "Capability":           "#FF5722",
    "Brand":                "#FF9800",
    "Initiative":           "#795548",
    "Financial":            "#4CAF50",
    "Program":              "#607D8B",
    "Management":           "#FFD700",
    "Competitors":          "#C0392B",
    "ProductPortfolio":     "#3b82f6",
}
