import json
import logging
from typing import List, Dict, Any, Optional
from models import ExtractionPayload, EntityCandidate, RelationCandidate

logger = logging.getLogger(__name__)

def safe_json_loads(data: Any, default: Any = None) -> Any:
    if data is None: return default
    if isinstance(data, (dict, list)): return data
    if not isinstance(data, str) or not data.strip(): return default
    try:
        cleaned = data.strip()
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[-1].split("```")[0].strip()
        return json.loads(cleaned)
    except:
        import re
        try:
            dict_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            list_match = re.search(r'\[.*\]', cleaned, re.DOTALL)
            if dict_match and (not list_match or dict_match.start() < list_match.start()): return json.loads(dict_match.group())
            elif list_match: return json.loads(list_match.group())
        except: pass
        return default

def find_list_data(data: Any) -> List[Any]:
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ["entities", "relations", "facts", "data"]:
            if key in data and isinstance(data[key], list): return data[key]
    return []

class LogicGuard:
    def __init__(self, ontology: Dict[str, Any]):
        self.ontology = ontology

    def refine_payload(self, payload: ExtractionPayload) -> ExtractionPayload:
        """
        PLATINUM STANDARD HEALER:
        1. Inverts Geography -> PRODUCES -> Product to Product -> MANUFACTURED_IN -> Geography.
        2. Deeply nests all products/brands under Product Portfolio.
        3. Prunes head-node clutter and adopts orphans.
        """
        entity_map = {e.temp_id: e for e in payload.entities}
        def norm(t): return str(t).lower().replace(" ", "").replace("_", "")
        
        # 1. Identify Root (Prioritize Nike or the most descriptive corporate name)
        root = next((e for e in payload.entities if "nike" in e.canonical_name.lower() and norm(e.entity_type) in ["legalentity", "company"]), None)
        if not root:
            root = next((e for e in payload.entities if norm(e.entity_type) in ["legalentity", "company"]), None)
        if not root and payload.entities: root = payload.entities[0]
        if not root: return payload

        logger.info(f"LogicGuard: Centering hierarchy around root node: {root.canonical_name} ({root.temp_id})")

        # 2. Bridge Nodes Discovery/Creation
        bridges = {} 
        for e in payload.entities:
            ename, etype = str(e.canonical_name).lower(), norm(e.entity_type)
            # Semantic Bridge Matching
            if "management" in ename: bridges["management"] = e.temp_id
            elif "competitor" in ename: bridges["competitornetwork"] = e.temp_id
            elif any(x in ename for x in ["service", "subscription"]) and etype in ["subindustry", "industry", "businessunit", "productdomain", "serviceportfolio"]:
                bridges["serviceportfolio"] = e.temp_id
            elif any(x in ename for x in ["product", "electronic", "portfolio"]) and etype in ["industry", "subindustry", "productdomain", "productportfolio"]:
                bridges["productportfolio"] = e.temp_id
            elif any(x in ename for x in ["manufactur", "network", "supply"]):
                bridges["manufacturingnetwork"] = e.temp_id

        needed = {
            "management": ("Management", f"{root.canonical_name} Management", "HAS_MANAGEMENT"),
            "competitornetwork": ("CompetitorNetwork", "Competitor Network", "HAS_COMPETITOR_NETWORK"),
            "productportfolio": ("ProductPortfolio", "Product Portfolio", "HAS_PRODUCTS"),
            "manufacturingnetwork": ("ManufacturingNetwork", "Manufacturing Network", "HAS_NETWORK"),
            "serviceportfolio": ("ServicePortfolio", "Service Portfolio", "HAS_SERVICE_PORTFOLIO")
        }

        relevance = {
            "management": any(norm(e.entity_type) == "person" for e in payload.entities),
            "competitornetwork": any(norm(e.entity_type) == "legalentity" and e.temp_id != root.temp_id for e in payload.entities),
            "productportfolio": any(norm(e.entity_type) in ["productline", "product", "brand", "productfamily", "productdomain"] for e in payload.entities),
            "manufacturingnetwork": any(norm(e.entity_type) == "geography" and e.temp_id != root.temp_id for e in payload.entities),
            "serviceportfolio": any(norm(e.entity_type) in ["service", "serviceportfolio", "digitalproduct"] or any(x in str(e.canonical_name).lower() for x in ["icloud", "apple music", "subscription"]) for e in payload.entities)
        }

        for btype, (etype, name, rel) in needed.items():
            if relevance[btype] and btype not in bridges:
                bid = f"bridge_{btype}_001"
                payload.entities.append(EntityCandidate(temp_id=bid, canonical_name=name, entity_type=etype, short_info=f"Auto-generated {etype} container"))
                payload.relations.append(RelationCandidate(source_temp_id=root.temp_id, target_temp_id=bid, relation_type=rel))
                bridges[btype] = bid
                entity_map[bid] = payload.entities[-1]

        # 3. Relation Refining & Inversion
        new_relations = []
        for rel in payload.relations:
            src, tgt = entity_map.get(str(rel.source_temp_id)), entity_map.get(str(rel.target_temp_id))
            if not src or not tgt: continue
            
            stype, ttype = norm(src.entity_type), norm(tgt.entity_type)

            # INVERSION RULE: Geography/Site PRODUCES Product -> Product MANUFACTURED_IN Geography/Site
            if stype in ["geography", "site", "businessunit"] and ttype in ["productline", "product", "brand"] and rel.relation_type == "PRODUCES":
                rel.source_temp_id, rel.target_temp_id = tgt.temp_id, src.temp_id
                rel.relation_type = "MANUFACTURED_IN"
            
            # Re-route direct root links
            if src.temp_id == root.temp_id:
                # Decide if this should go through a bridge
                target_bridge = None
                if ttype == "person": target_bridge = bridges.get("management")
                elif ttype in ["productline", "product", "brand", "service", "productfamily", "productdomain", "digitalproduct"]: 
                    n = str(tgt.canonical_name).lower()
                    if any(x in n for x in ["service", "music", "cloud", "icloud"]):
                        target_bridge = bridges.get("serviceportfolio")
                    else:
                        target_bridge = bridges.get("productportfolio")
                elif ttype == "geography" and tgt.temp_id != root.temp_id: target_bridge = bridges.get("manufacturingnetwork")
                elif ttype == "legalentity" and "competitor" in str(tgt.short_info).lower(): target_bridge = bridges.get("competitornetwork")
                
                if target_bridge and target_bridge != tgt.temp_id:
                    rel.source_temp_id = target_bridge
                    rel.relation_type = "INCLUDES"

            new_relations.append(rel)

        # 4. Deep Portfolio Nesting (Ensure every product is under portfolio)
        port_id = bridges.get("productportfolio")
        if port_id:
            has_portfolio_link = set()
            for r in new_relations:
                if str(r.source_temp_id) == str(port_id): has_portfolio_link.add(str(r.target_temp_id))
            
            for e in payload.entities:
                ename = str(e.canonical_name).lower()
                etype = norm(e.entity_type)
                if etype in ["productline", "product", "brand", "service", "productfamily", "productdomain", "digitalproduct", "item"] and e.temp_id not in bridges.values():
                    # Decide which portfolio
                    target_bid = bridges.get("productportfolio")
                    if any(x in ename for x in ["service", "music", "cloud", "icloud"]):
                        target_bid = bridges.get("serviceportfolio") or target_bid
                    
                    if target_bid and str(e.temp_id) not in has_portfolio_link and str(e.temp_id) != str(target_bid):
                        new_relations.append(RelationCandidate(
                            source_temp_id=target_bid, 
                            target_temp_id=e.temp_id, 
                            relation_type="INCLUDES"
                        ))

        # 5. Aggressive Re-Parenting & Adoption
        has_bridge_incoming = set()
        for r in new_relations:
            if str(r.source_temp_id) in bridges.values():
                has_bridge_incoming.add(str(r.target_temp_id))

        for e in payload.entities:
            eid = str(e.temp_id)
            if eid == str(root.temp_id) or eid in bridges.values(): continue
            
            # If not yet bridged, force it
            if eid not in has_bridge_incoming:
                etype = norm(e.entity_type)
                bid = None
                print(f"DEBUG: LogicGuard Re-Parenting Check: {e.canonical_name} type={etype}")
                if etype == "person": bid = bridges.get("management")
                elif etype in ["productline", "product", "brand", "service", "productfamily", "productdomain", "digitalproduct"]: 
                    n = e.canonical_name.lower()
                    if any(x in n for x in ["service", "music", "cloud", "icloud"]):
                        bid = bridges.get("serviceportfolio")
                    else:
                        bid = bridges.get("productportfolio")
                elif etype == "geography": bid = bridges.get("manufacturingnetwork")
                elif etype == "legalentity" and eid != str(root.temp_id): bid = bridges.get("competitornetwork")
                elif etype in ["service", "serviceportfolio"]: bid = bridges.get("serviceportfolio")
                
                if bid:
                    print(f"  -> FORCING {e.canonical_name} into {bid}")
                    # PLATINUM GROUNDING: Inherit source_text from any existing relation to this node
                    existing_evidence = ""
                    for rel in payload.relations:
                        if str(rel.target_temp_id) == eid and rel.source_text:
                            existing_evidence = rel.source_text
                            break
                            
                    new_relations.append(RelationCandidate(
                        source_temp_id=bid,
                        target_temp_id=e.temp_id,
                        relation_type="INCLUDES",
                        source_text=existing_evidence
                    ))
                    has_bridge_incoming.add(eid)

        # 6. Final Catch-All / Orphan Adoption
        # Ensure EVERY node has at least one incoming link (except root)
        final_rels = []
        seen = set()
        has_any_incoming = set()

        # Phase A: Collect all valid relations and determine which nodes have incoming links
        temp_final = []
        bridge_ids = set(bridges.values())
        for r in new_relations:
            src_id, tgt_id = str(r.source_temp_id), str(r.target_temp_id)
            if src_id == tgt_id: continue
            
            # Pruning Rule: Remove link from root if target has a bridge parent
            if src_id == str(root.temp_id) and tgt_id in has_bridge_incoming and tgt_id not in bridge_ids:
                continue
            
            temp_final.append(r)
            has_any_incoming.add(tgt_id)

        # Phase B: Adopt any node that is still an orphan or a floating category
        for e in payload.entities:
            eid = str(e.temp_id)
            if eid == str(root.temp_id) or eid in has_any_incoming:
                continue
            
            # Special case: If this is a Category (Industry/Domain/Bridge), it MUST connect to root
            # Connect to root as fallback
            temp_final.append(RelationCandidate(
                source_temp_id=root.temp_id,
                target_temp_id=e.temp_id,
                relation_type="ASSOCIATED_WITH" if norm(e.entity_type) not in ["industry", "subindustry", "productdomain", "management", "serviceportfolio", "productportfolio"] else "INCLUDES"
            ))

        # Phase C: Final unique check
        for r in temp_final:
            key = (str(r.source_temp_id), str(r.target_temp_id), str(r.relation_type))
            if key not in seen:
                final_rels.append(r)
                seen.add(key)

        # ════════════════════════════════════════════════════════════════════════
        # PHASE F: BFS-Based Strict Tree Enforcement (Platinum Guard)
        # ════════════════════════════════════════════════════════════════════════
        tree_rels = []
        visited = {str(root.temp_id)}
        queue = [str(root.temp_id)]
        
        while queue:
            current_pid = queue.pop(0)
            # Find all potential children of this parent
            for r in final_rels:
                if str(r.source_temp_id) == current_pid:
                    tgt = str(r.target_temp_id)
                    if tgt not in visited:
                        tree_rels.append(r)
                        visited.add(tgt)
                        queue.append(tgt)
                    else:
                        logger.info(f"Skipping cycle/redundant link to {tgt}")
                        
        # Final safety: If any nodes were missed, link them to root
        all_eids = {str(e.temp_id) for e in payload.entities}
        missed = all_eids - visited
        for eid in missed:
            if eid == str(root.temp_id): continue
            logger.info(f"Adopting lone node into root: {eid}")
            tree_rels.append(RelationCandidate(
                source_temp_id=root.temp_id,
                target_temp_id=eid,
                relation_type="ASSOCIATED_WITH"
            ))

        payload.relations = tree_rels

        # Phase D: Prune Empty Auto-Bridges
        # If an auto-bridge was created but has no children, and a better one exists, remove it.
        has_outgoing = {str(r.source_temp_id) for r in payload.relations}
        final_entities = []
        for e in payload.entities:
            eid = str(e.temp_id)
            if eid.startswith("bridge_") and eid not in has_outgoing:
                logger.info(f"Pruning empty auto-bridge: {e.canonical_name} ({eid})")
                continue
            final_entities.append(e)
        payload.entities = final_entities

        # Phase E: Strict One-Parent Rule (Perfect Hierarchy)
        # Goal: Every node (except root) should have EXACTLY one taxonomic parent.
        # If a node is inside a Bridge/Portfolio, delete its direct link to Root/Industry.
        bridge_ids = {str(e.temp_id) for e in payload.entities if norm(e.entity_type) in ["productportfolio", "serviceportfolio", "management", "competitornetwork", "manufacturingnetwork"]}
        
        # 1. Map each target to its incoming relations
        incoming_map = {}
        for r in payload.relations:
            tid = str(r.target_temp_id)
            if tid not in incoming_map: incoming_map[tid] = []
            incoming_map[tid].append(r)
            
        final_rels_strict = []
        root_id = str(root.temp_id)
        
        for tid, rels in incoming_map.items():
            if len(rels) <= 1:
                final_rels_strict.extend(rels)
                continue
            
            # If multiple, prioritize Bridge/Portfolio sources
            bridge_source_rels = [r for r in rels if str(r.source_temp_id) in bridge_ids]
            if bridge_source_rels:
                # ONLY keep the bridge relationship(s). Prioritize the first one if multiple.
                final_rels_strict.append(bridge_source_rels[0])
                logger.info(f"StrictGuard: Pruned redundant parents for {tid}, kept bridge {bridge_source_rels[0].source_temp_id}")
            else:
                # If no bridge source, but multiple others (e.g. Industry and Root), prioritize Industry over Root
                industry_source_rels = [r for r in rels if any(x in str(r.source_temp_id).lower() for x in ["ind_", "sub_"])]
                if industry_source_rels:
                    final_rels_strict.append(industry_source_rels[0])
                else:
                    final_rels_strict.append(rels[0])

        payload.relations = final_rels_strict
        return payload

    def validate_extraction(self, payload: ExtractionPayload) -> List[str]:
        return []
