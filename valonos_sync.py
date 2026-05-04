"""
Sync ValonOS servicer_law_firms entities from BigQuery.

Pulls the prod data, stores it locally, and auto-matches entities
to indexed firms using the same fuzzy matching logic as the rest of the app.
"""
import os
import json
import urllib.request
import urllib.error
from datetime import datetime
from models import get_session, IndexedFirm, Moniker, ValonosEntity
from matching import normalize_name, calculate_match_score

GESTALT_BASE = "https://api.gestalt.peachstreet.dev/api/v1"

# Tenant creation_order_id -> friendly name
TENANT_NAMES = {
    1: "Valon Production",
    2: "VT Ocean UAT",
    3: "ServiceMac Trial",
    4: "VT Ocean SDT Iteration",
    5: "Ocean Sandbox 3",
    7: "ServiceMac Training",
    9: "ServiceMac Production",
    10: "Ocean Sandbox 4",
}


def _bq_query(query, max_results=500):
    """Run a BigQuery query via Gestalt and return rows."""
    api_key = os.environ.get("GESTALT_API_KEY", "")
    if not api_key:
        raise RuntimeError("GESTALT_API_KEY is not set.")

    url = f"{GESTALT_BASE}/bigquery/query"
    payload = json.dumps({
        "project_id": "prod-peach-street",
        "query": query,
        "max_results": max_results,
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"BigQuery API error: {e.code} {e.reason}")

    return body.get("data", {}).get("rows", [])


def fetch_valonos_entities():
    """Fetch all servicer_law_firms from ValonOS prod."""
    rows = _bq_query(
        "SELECT name, is_active, tenant_key, sid "
        "FROM `datastream_cellar.servicer_law_firms` "
        "WHERE datetime_deleted IS NULL "
        "ORDER BY tenant_key, name"
    )

    entities = []
    for r in rows:
        entities.append({
            "sid": r["sid"],
            "name": r["name"],
            "tenant_key": r["tenant_key"],
            "tenant_name": TENANT_NAMES.get(r["tenant_key"], f"Tenant {r['tenant_key']}"),
            "is_active": bool(r["is_active"]),
        })
    return entities


def _match_entity_to_index(entity_name, firms, monikers_by_name):
    """Try to match a ValonOS entity name to an indexed firm.

    Returns (firm_id, score) or (None, 0).
    """
    normalized = normalize_name(entity_name)

    # 1. Exact normalized match on firm name
    for firm in firms:
        if normalize_name(firm.name) == normalized:
            return firm.id, 100

    # 2. Exact normalized match on moniker
    for mon_name, firm_id in monikers_by_name.items():
        if normalize_name(mon_name) == normalized:
            return firm_id, 100

    # 3. Fuzzy match on firm names
    best_id = None
    best_score = 0
    for firm in firms:
        score = calculate_match_score(entity_name, firm.name)
        if score["overall"] > best_score:
            best_score = score["overall"]
            best_id = firm.id

    # 4. Fuzzy match on monikers
    for mon_name, firm_id in monikers_by_name.items():
        score = calculate_match_score(entity_name, mon_name)
        if score["overall"] > best_score:
            best_score = score["overall"]
            best_id = firm_id

    if best_score >= 80:
        return best_id, best_score

    return None, best_score


def sync_valonos_entities(session):
    """Fetch ValonOS entities from BigQuery and upsert locally.

    Auto-matches entities to indexed firms using fuzzy matching.
    Returns a summary dict.
    """
    raw_entities = fetch_valonos_entities()

    # Build lookup structures for matching
    all_firms = session.query(IndexedFirm).all()
    all_monikers = session.query(Moniker).all()
    monikers_by_name = {m.name: m.indexed_firm_id for m in all_monikers}

    created = 0
    updated = 0
    matched = 0
    unmatched = []

    now = datetime.utcnow()

    for ent in raw_entities:
        existing = session.query(ValonosEntity).filter(
            ValonosEntity.sid == ent["sid"]
        ).first()

        if existing:
            existing.name = ent["name"]
            existing.tenant_key = ent["tenant_key"]
            existing.tenant_name = ent["tenant_name"]
            existing.is_active = ent["is_active"]
            existing.synced_at = now
            # Re-match if not already matched
            if not existing.indexed_firm_id:
                firm_id, score = _match_entity_to_index(
                    ent["name"], all_firms, monikers_by_name
                )
                if firm_id:
                    existing.indexed_firm_id = firm_id
                    matched += 1
                else:
                    unmatched.append(ent["name"])
            updated += 1
        else:
            firm_id, score = _match_entity_to_index(
                ent["name"], all_firms, monikers_by_name
            )

            entity = ValonosEntity(
                sid=ent["sid"],
                name=ent["name"],
                tenant_key=ent["tenant_key"],
                tenant_name=ent["tenant_name"],
                is_active=ent["is_active"],
                indexed_firm_id=firm_id,
                synced_at=now,
            )
            session.add(entity)

            if firm_id:
                matched += 1
            else:
                unmatched.append(ent["name"])
            created += 1

    session.commit()

    return {
        "total": len(raw_entities),
        "created": created,
        "updated": updated,
        "matched": matched,
        "unmatched": unmatched,
    }


def refresh_index_from_valonos(session, tenant_key=1):
    """Refresh the canonical IndexedFirm list from ValonOS prod.

    Uses ValonOS servicer_law_firms (filtered by tenant_key) as the source
    of truth. For each active entity:
      - If already linked to an IndexedFirm, update the firm name to match ValonOS
      - If not linked but fuzzy-matches an existing firm, link it and update the name
      - If no match, create a new IndexedFirm from the ValonOS name

    Returns a summary dict.
    """
    # First, make sure ValonosEntity table is up to date
    sync_result = sync_valonos_entities(session)

    # Now work with the local ValonosEntity records for the target tenant
    valonos_ents = session.query(ValonosEntity).filter(
        ValonosEntity.tenant_key == tenant_key,
        ValonosEntity.is_active == True,
    ).all()

    all_firms = session.query(IndexedFirm).all()
    all_monikers = session.query(Moniker).all()
    monikers_by_name = {m.name: m.indexed_firm_id for m in all_monikers}

    added = []
    updated = []
    already_current = 0

    for ent in valonos_ents:
        if ent.indexed_firm_id:
            # Already linked — update the indexed firm name if different
            firm = session.query(IndexedFirm).get(ent.indexed_firm_id)
            if firm:
                if normalize_name(firm.name) != normalize_name(ent.name):
                    old_name = firm.name
                    firm.name = ent.name
                    updated.append({"old": old_name, "new": ent.name})
                else:
                    already_current += 1
        else:
            # Not linked — try to fuzzy match
            firm_id, score = _match_entity_to_index(
                ent.name, all_firms, monikers_by_name
            )
            if firm_id and score >= 80:
                ent.indexed_firm_id = firm_id
                firm = session.query(IndexedFirm).get(firm_id)
                if firm and normalize_name(firm.name) != normalize_name(ent.name):
                    old_name = firm.name
                    firm.name = ent.name
                    updated.append({"old": old_name, "new": ent.name})
                else:
                    already_current += 1
            else:
                # Create new indexed firm
                new_firm = IndexedFirm(name=ent.name, is_active=True)
                session.add(new_firm)
                session.flush()  # get the id
                ent.indexed_firm_id = new_firm.id
                added.append(ent.name)
                # Refresh firms list for subsequent matching
                all_firms = session.query(IndexedFirm).all()

    session.commit()

    return {
        "sync": sync_result,
        "added": added,
        "updated": updated,
        "already_current": already_current,
        "total_entities": len(valonos_ents),
    }
