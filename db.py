"""
Database helpers for Law Firm Matcher.
"""
from sqlalchemy import or_, func
from models import (
    get_session, IndexedFirm, Moniker, MaRule,
    FirmTracker, ServicerList, ServicerListEntry
)
from matching import normalize_name, calculate_match_score


def get_db():
    """Get a database session."""
    return get_session()


# === IndexedFirm CRUD ===

def get_all_firms(session):
    return session.query(IndexedFirm).order_by(IndexedFirm.name).all()


def get_firm_by_id(session, firm_id):
    return session.query(IndexedFirm).get(firm_id)


def get_firm_by_name(session, name):
    return session.query(IndexedFirm).filter(IndexedFirm.name == name).first()


def create_firm(session, name, notes=""):
    firm = IndexedFirm(name=name, notes=notes)
    session.add(firm)
    session.commit()
    return firm


def update_firm(session, firm_id, name=None, notes=None, is_active=None):
    firm = get_firm_by_id(session, firm_id)
    if firm:
        if name is not None:
            firm.name = name
        if notes is not None:
            firm.notes = notes
        if is_active is not None:
            firm.is_active = is_active
        session.commit()
    return firm


def delete_firm(session, firm_id):
    firm = get_firm_by_id(session, firm_id)
    if firm:
        session.delete(firm)
        session.commit()


# === Moniker CRUD ===

def get_monikers_for_firm(session, firm_id):
    return session.query(Moniker).filter(Moniker.indexed_firm_id == firm_id).all()


def add_moniker(session, firm_id, name, source="", notes=""):
    existing = session.query(Moniker).filter(
        Moniker.indexed_firm_id == firm_id,
        Moniker.name == name
    ).first()
    if existing:
        return existing
    moniker = Moniker(indexed_firm_id=firm_id, name=name, source=source, notes=notes)
    session.add(moniker)
    session.commit()
    return moniker


def delete_moniker(session, moniker_id):
    m = session.query(Moniker).get(moniker_id)
    if m:
        session.delete(m)
        session.commit()


# === M&A Rules CRUD ===

def get_all_ma_rules(session):
    return session.query(MaRule).order_by(MaRule.acquired_name).all()


def add_ma_rule(session, acquired_name, acquiring_firm_id, context_label, notes=""):
    rule = MaRule(
        acquired_name=acquired_name,
        acquiring_firm_id=acquiring_firm_id,
        context_label=context_label,
        notes=notes
    )
    session.add(rule)
    session.commit()
    # Also add the acquired name as a moniker of the acquiring firm
    add_moniker(session, acquiring_firm_id, acquired_name, source="M&A rule", notes=f"Via M&A: {context_label}")
    return rule


def delete_ma_rule(session, rule_id):
    rule = session.query(MaRule).get(rule_id)
    if rule:
        session.delete(rule)
        session.commit()


def check_ma_rules(session, name):
    """Check if a firm name matches any M&A rule.

    Returns the acquiring firm and context if matched.
    """
    normalized = normalize_name(name)
    rules = get_all_ma_rules(session)
    for rule in rules:
        if normalize_name(rule.acquired_name) == normalized:
            return rule
        # Also fuzzy match
        score = calculate_match_score(name, rule.acquired_name)
        if score["overall"] >= 85:
            return rule
    return None


# === Search ===

def search_firms(session, query, limit=20):
    """Search across indexed firms, monikers, and M&A rules.

    Returns a list of dicts with match info.
    """
    if not query or not query.strip():
        return []

    results = []
    seen_firm_ids = set()
    normalized_query = normalize_name(query)

    # 1. Check M&A rules first
    ma_match = check_ma_rules(session, query)
    if ma_match:
        firm = ma_match.acquiring_firm
        if firm.id not in seen_firm_ids:
            results.append({
                "firm": firm,
                "match_type": "M&A rule",
                "matched_name": ma_match.acquired_name,
                "context": ma_match.context_label,
                "score": 100,
            })
            seen_firm_ids.add(firm.id)

    # 2. Exact match on indexed firm name (normalized)
    all_firms = get_all_firms(session)
    for firm in all_firms:
        if normalize_name(firm.name) == normalized_query and firm.id not in seen_firm_ids:
            results.append({
                "firm": firm,
                "match_type": "exact",
                "matched_name": firm.name,
                "context": "",
                "score": 100,
            })
            seen_firm_ids.add(firm.id)

    # 3. Exact match on moniker
    all_monikers = session.query(Moniker).all()
    for m in all_monikers:
        if normalize_name(m.name) == normalized_query and m.indexed_firm_id not in seen_firm_ids:
            results.append({
                "firm": m.indexed_firm,
                "match_type": "moniker",
                "matched_name": m.name,
                "context": f"Source: {m.source}" if m.source else "",
                "score": 100,
            })
            seen_firm_ids.add(m.indexed_firm_id)

    # 4. Fuzzy match on indexed firm names
    for firm in all_firms:
        if firm.id in seen_firm_ids:
            continue
        score = calculate_match_score(query, firm.name)
        if score["overall"] >= 60:
            results.append({
                "firm": firm,
                "match_type": "fuzzy",
                "matched_name": firm.name,
                "context": "",
                "score": score["overall"],
            })
            seen_firm_ids.add(firm.id)

    # 5. Fuzzy match on monikers
    for m in all_monikers:
        if m.indexed_firm_id in seen_firm_ids:
            continue
        score = calculate_match_score(query, m.name)
        if score["overall"] >= 60:
            results.append({
                "firm": m.indexed_firm,
                "match_type": "moniker (fuzzy)",
                "matched_name": m.name,
                "context": f"Source: {m.source}" if m.source else "",
                "score": score["overall"],
            })
            seen_firm_ids.add(m.indexed_firm_id)

    # Sort by score
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:limit]


# === Servicer List / Ingestion ===

def create_servicer_list(session, servicer_name, milestone, filename="", notes=""):
    sl = ServicerList(
        servicer_name=servicer_name,
        milestone=milestone,
        filename=filename,
        notes=notes
    )
    session.add(sl)
    session.commit()
    return sl


def get_all_servicer_lists(session):
    return session.query(ServicerList).order_by(ServicerList.uploaded_at.desc()).all()


def get_servicer_list(session, list_id):
    return session.query(ServicerList).get(list_id)


def ingest_firm_list(session, servicer_list_id, raw_names):
    """Ingest a list of firm names, auto-matching against indexed firms + M&A rules.

    Returns classification results.
    """
    sl = get_servicer_list(session, servicer_list_id)
    if not sl:
        return None

    all_firms = get_all_firms(session)
    all_monikers = session.query(Moniker).all()

    # Build candidate list: indexed names + all monikers
    candidates = {}  # normalized_name -> firm_id
    for firm in all_firms:
        candidates[firm.name] = firm.id
    for m in all_monikers:
        candidates[m.name] = m.indexed_firm_id

    results = {"auto_matched": [], "review": [], "new": []}

    for raw_name in raw_names:
        if not raw_name or not raw_name.strip():
            continue

        # Check M&A rules first
        ma_match = check_ma_rules(session, raw_name)
        if ma_match:
            entry = ServicerListEntry(
                servicer_list_id=servicer_list_id,
                raw_name=raw_name,
                matched_firm_id=ma_match.acquiring_firm_id,
                match_score=100,
                match_status="auto_matched",
                notes=f"M&A rule: {ma_match.context_label}"
            )
            session.add(entry)
            results["auto_matched"].append({
                "raw_name": raw_name,
                "matched_to": ma_match.acquiring_firm.name,
                "score": 100,
                "via": f"M&A: {ma_match.context_label}",
            })
            continue

        # Fuzzy match against all candidates
        best_match = None
        best_score = 0
        best_firm_id = None

        for cand_name, firm_id in candidates.items():
            score = calculate_match_score(raw_name, cand_name)
            if score["overall"] > best_score:
                best_score = score["overall"]
                best_match = cand_name
                best_firm_id = firm_id

        if best_score >= 95:
            entry = ServicerListEntry(
                servicer_list_id=servicer_list_id,
                raw_name=raw_name,
                matched_firm_id=best_firm_id,
                match_score=best_score,
                match_status="auto_matched",
            )
            session.add(entry)
            firm = get_firm_by_id(session, best_firm_id)
            results["auto_matched"].append({
                "raw_name": raw_name,
                "matched_to": firm.name if firm else best_match,
                "score": best_score,
                "via": f"Fuzzy match on: {best_match}",
            })
        elif best_score >= 75:
            entry = ServicerListEntry(
                servicer_list_id=servicer_list_id,
                raw_name=raw_name,
                matched_firm_id=best_firm_id,
                match_score=best_score,
                match_status="review",
            )
            session.add(entry)
            firm = get_firm_by_id(session, best_firm_id)
            results["review"].append({
                "raw_name": raw_name,
                "matched_to": firm.name if firm else best_match,
                "score": best_score,
                "via": f"Fuzzy match on: {best_match}",
            })
        else:
            entry = ServicerListEntry(
                servicer_list_id=servicer_list_id,
                raw_name=raw_name,
                match_score=best_score if best_score > 0 else None,
                match_status="new",
            )
            session.add(entry)
            results["new"].append({
                "raw_name": raw_name,
                "best_candidate": best_match,
                "score": best_score,
            })

    session.commit()
    return results


def confirm_match(session, entry_id, firm_id=None):
    """Confirm a match (optionally changing the matched firm)."""
    entry = session.query(ServicerListEntry).get(entry_id)
    if entry:
        if firm_id is not None:
            entry.matched_firm_id = firm_id
        entry.match_status = "confirmed"
        session.commit()


def reject_match(session, entry_id):
    """Reject a match."""
    entry = session.query(ServicerListEntry).get(entry_id)
    if entry:
        entry.matched_firm_id = None
        entry.match_status = "rejected"
        session.commit()


# === Seed Data ===

def seed_initial_data(session):
    """Seed the database with the indexed firm list from the onboarding exercise."""
    if session.query(IndexedFirm).count() > 0:
        return  # already seeded

    firms_data = [
        ("ALBERTELLI LAW", []),
        ("ALDRIDGE PITE (acq. BWW)", [("BWW Law", "M&A"), ("BWW Law Group", "M&A")]),
        ("ANSELMO LINDBERG & ASSOCIATES", [("Diaz Anselmo", "variant")]),
        ("BAER & TIMBERLAKE", []),
        ("BARRETT DAFFIN FRAPPIER TREDER & WEISS", []),
        ("BDF LAW GROUP", []),
        ("BELL CARRINGTON", []),
        ("BONIAL", []),
        ("BROCK & SCOTT", []),
        ("CLUNK HOOSE", []),
        ("CODILIS & MOODY & CIRCELLI", [
            ("Codilis & Associates", "ServiceMac"),
            ("Codilis & Associates, P.C.", "ServiceMac"),
            ("Codilis, Moody & Circelli, P.C.", "ServiceMac"),
        ]),
        ("DE CUBAS & LEWIS", []),
        ("DEAN MORRIS", []),
        ("DELUCA LAW GROUP", [("DeLuca Law Group PLLC", "variant")]),
        ("DOYLE & FOUTTY", []),
        ("FOUNDATIONS LEGAL GROUP (fka Hutchens / Wilson & Associates)", [
            ("Hutchens Law Firm", "historical"),
            ("Wilson & Associates", "historical"),
            ("Foundation Legal Group", "variant"),
        ]),
        ("FRENKEL LAMBERT WEISS WEISMAN & GORDON", []),
        ("FRIEDMAN VARTOLO LLP", []),
        ("GHIDOTTI BERGER", [("Ghidotti & Berger, LLP", "variant")]),
        ("GRAY & ASSOCIATES", []),
        ("GREENSPOON MARDER", []),
        ("GROSS POLOWY LLC", []),
        ("HALLIDAY WATKINS & MANN (acq. Dean Morris)", []),
        ("HEAVNER BEYERS & MIHLAR", []),
        ("HERSCHEL C ADCOCK", []),
        ("INGLE LAW FIRM", []),
        ("JACKSON & MCPHERSON", []),
        ("JANEWAY LAW FIRM", []),
        ("KML LAW GROUP", []),
        ("KORDE & ASSOCIATES", []),
        ("LEOPOLD", []),
        ("LEU OKUDA & DOI", []),
        ("LIEBO WEINGARDEN DOBIE & BARBEE", []),
        ("LOGS LEGAL GROUP", []),
        ("MACKIE WOLF ZIENTZ & MANN", []),
        ("MALCOLM & CISNEROS (Trustee Corps)", [("Trustee Corps", "variant")]),
        ("MANLEY DEAS KOCHALSKI", []),
        ("MARINOSCI LAW GROUP", []),
        ("MCCABE", []),
        ("MCCALLA RAYMER LEIBERT PIERCE", []),
        ("MCCARTHY & HOLTHUS", []),
        ("MILLER GEORGE & SUGGS", []),
        ("MILLSAP & SINGER", []),
        ("NOONAN & LIEBERMAN", []),
        ("ORLANS", []),
        ("PADGETT LAW GROUP", []),
        ("PETOSA LAW", []),
        ("PLUESE, BECKER SALTZMAN", []),
        ("QUINTAIROS, PRIETO, WOOD", []),
        ("RAS LAW", [("Robertson Anschutz Schneid Crane & Partners", "full name")]),
        ("REISENFELD & ASSOCIATES", []),
        ("ROACH & LIN", []),
        ("ROSENBERG & ASSOCIATES", []),
        ("RUBIN LUBLIN", []),
        ("SAMUEL I WHITE", []),
        ("SANDHU LAW GROUP", []),
        ("SAYER LAW GROUP", []),
        ("SCHNEIDERMAN & SHERMAN", []),
        ("SCOTT AND CORLEY P.A.", []),
        ("SOKOLOF REMTULLA", []),
        ("SOUTHLAW", []),
        ("STERN & EISENBERG", []),
        ("STERN LAVINTHAL & FRANKENBERG", []),
        ("THE MORTGAGE LAW FIRM", [("TMLF Hawaii", "variant")]),
        ("TIFFANY & BOSCO (acq. Reimer)", [("Reimer Law Co", "M&A"), ("Reimer Aranovitch Chernek & Jeffrey", "M&A")]),
        ("TROMBERG MORRIS & POULIN", []),
        ("TROTT LAW", []),
        ("WILFORD GESKE & COOK", []),
        ("ZBS", []),
    ]

    for name, monikers in firms_data:
        firm = IndexedFirm(name=name)
        session.add(firm)
        session.flush()  # get the ID

        for moniker_name, source in monikers:
            m = Moniker(indexed_firm_id=firm.id, name=moniker_name, source=source)
            session.add(m)

    # Seed M&A rules
    ma_rules = [
        ("BWW Law", "ALDRIDGE PITE (acq. BWW)", "acq. BWW"),
        ("Reimer Law Co", "TIFFANY & BOSCO (acq. Reimer)", "acq. Reimer"),
        ("Hutchens Law Firm", "FOUNDATIONS LEGAL GROUP (fka Hutchens / Wilson & Associates)", "fka Hutchens / Wilson & Associates"),
        ("Wilson & Associates", "FOUNDATIONS LEGAL GROUP (fka Hutchens / Wilson & Associates)", "fka Hutchens / Wilson & Associates"),
        ("Dean Morris", "HALLIDAY WATKINS & MANN (acq. Dean Morris)", "acq. Dean Morris"),
    ]

    for acquired, acquiring_name, context in ma_rules:
        acquiring = session.query(IndexedFirm).filter(IndexedFirm.name == acquiring_name).first()
        if acquiring:
            rule = MaRule(
                acquired_name=acquired,
                acquiring_firm_id=acquiring.id,
                context_label=context,
            )
            session.add(rule)

    session.commit()
    print(f"Seeded {len(firms_data)} indexed firms with monikers and {len(ma_rules)} M&A rules.")

    # Seed firm tracker data (from the firm tracker spreadsheet)
    seed_firm_tracker(session)


# === Firm Tracker CRUD ===

def get_all_trackers(session):
    return (session.query(FirmTracker)
            .join(IndexedFirm)
            .order_by(IndexedFirm.name)
            .all())


def get_tracker_for_firm(session, firm_id):
    return session.query(FirmTracker).filter(FirmTracker.indexed_firm_id == firm_id).first()


def upsert_tracker(session, firm_id, **kwargs):
    tracker = get_tracker_for_firm(session, firm_id)
    if not tracker:
        tracker = FirmTracker(indexed_firm_id=firm_id)
        session.add(tracker)
    for key, val in kwargs.items():
        if hasattr(tracker, key) and val is not None:
            setattr(tracker, key, val)
    session.commit()
    return tracker


def seed_firm_tracker(session):
    """Seed firm tracker from the spreadsheet data."""
    if session.query(FirmTracker).count() > 0:
        return

    def _bool(val):
        return val.upper() in ("YES", "TRUE") if val else False

    # (firm_name, vm, nrz_rank, lc_rank, design_partner, m1, m2, wave, last_reach, p0, lead_mtg, design_mtg, lead_engage, wave_notes, interaction, ops_email, lead_contact, lead_title, lead_email, nda, notes)
    tracker_data = [
        ("ALBERTELLI LAW", False, "Not in top 10", "Not in top 90%", True, True, "Yes", "Wave 2 (March 26)", "12/2/2025", False, False, True, "Yes", "", "Design partner", "swalter@alaw.net", "", "", "", "", "Wants to integrate with own CMS; does not work well on GMeet"),
        ("SOKOLOF REMTULLA", False, "Not in top 10", "Not in top 90%", True, False, "Yes", "Wave 2 (March 26)", "11/10/2025", False, True, True, "Yes", "", "Design partner", "N/A", "Owen Sokolof", "Managing Partner", "osokolof@sokrem.com", "osokolof@sokrem.com", "Friendly; interested in our CMS; helpful for data/technical questions"),
        ("RAS LAW", True, "3", "1", True, True, "Yes", "Wave 1 (Feb 26)", "11/03/2025", True, True, True, "Yes", "", "Design partner", "bjw@raslg.com", "David Schneid", "Managing Partner", "djs@raslg.com", "bjw@raslg.com", "Wants to show us their internal CMS; does not want to replace it"),
        ("MALCOLM & CISNEROS (Trustee Corps)", True, "Not in top 10", "13", True, False, "Yes", "Wave 1 (Feb 26)", "12/11/2025", False, False, True, "", "", "Design partner", "N/A", "", "", "", "", ""),
        ("MARINOSCI LAW GROUP", False, "Not in top 10", "3", False, True, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("FOUNDATIONS LEGAL GROUP (fka Hutchens / Wilson & Associates)", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("ALDRIDGE PITE (acq. BWW)", True, "4", "2", False, False, "Yes", "Wave 1 (Feb 26)", "11/03/2025", False, True, False, "Yes", "", "Intro call", "N/A", "John Aldridge, Jr.", "Managing Partner", "galdridge@aldridgepite.com", "", "Friendly, open to help with design programs; largest law firm for rocket"),
        ("MCCARTHY & HOLTHUS", True, "6", "4", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("BROCK & SCOTT", True, "Not in top 10", "6", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("FRIEDMAN VARTOLO LLP", True, "7", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("CODILIS & MOODY & CIRCELLI", True, "Not in top 10", "17", True, False, "Yes", "Pilot", "12/9", False, False, True, "Yes", "", "Design partner", "Sue.Trudo@il.cslegal.com", "Greg Moody", "Managing Partner", "Greg.Moody@il.cslegal.com", "greg.moody@il.cslegal.com", "Cagey about switching own CMS; leadership unengaged"),
        ("TROTT LAW", True, "Not in top 10", "22", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("PADGETT LAW GROUP", True, "2", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "No", "", "Planned engagement", "N/A", "Timothy Padgett", "Founder and CEO", "tpattorney@padgettlaw.net", "", "Email bounce back"),
        ("HERSCHEL C ADCOCK", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("MCCALLA RAYMER LEIBERT PIERCE", True, "1", "7", False, False, "Fell off", "Wave 3 (by M3)", "10/10/2025", True, False, False, "No", "", "Planned engagement", "Adam.Silver@mccalla.com", "Marty Stone", "Managing Partner and CEO", "marty.stone@mcalla.com", "", "Lots of VM volume - no notifications may be a pain pt"),
        ("HALLIDAY WATKINS & MANN (acq. Dean Morris)", True, "Not in top 10", "15", False, False, "Yes", "Wave 1 (Feb 26)", "11/03/2025", False, True, False, "Yes", "", "Intro call", "N/A", "Benjamin Mann", "Managing Partner", "ben@hwmlawfirm.com", "", "Friendly, personal relationship strong"),
        ("KORDE & ASSOCIATES", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("TIFFANY & BOSCO (acq. Reimer)", True, "Not in top 10", "5", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("TROMBERG MORRIS & POULIN", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("GHIDOTTI BERGER", True, "9", "Not in top 90%", True, False, "Yes", "Pilot", "10/20/2025", True, True, True, "Yes", "", "Design partner", "BCarvalho@ghidottiberger.com", "Michelle Ghidotti", "Shareholder", "mghidotti@ghidottiberger.com", "mghidotti@ghidottiberger.com", "Friendly; primary design partner"),
        ("MILLER GEORGE & SUGGS", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "Moved up 3/10", "Planned engagement", "N/A", "", "", "", "", ""),
        ("ORLANS", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("ROSENBERG & ASSOCIATES", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("BELL CARRINGTON", False, "Not in top 10", "19", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "Moved up 3/10", "Planned engagement", "N/A", "", "", "", "", ""),
        ("SAMUEL I WHITE", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("SCHNEIDERMAN & SHERMAN", True, "Not in top 10", "12", False, False, "No", "Wave 3 (by M3)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("THE MORTGAGE LAW FIRM", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("INGLE LAW FIRM", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("ZBS", True, "Not in top 10", "10", False, False, "No", "Wave 3 (by M3)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("SAYER LAW GROUP", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("HEAVNER BEYERS & MIHLAR", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "Moved up 3/10", "Planned engagement", "N/A", "", "", "", "", ""),
        ("RUBIN LUBLIN", True, "Not in top 10", "28", False, False, "No", "Wave 3 (by M3)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("SOUTHLAW", True, "Not in top 10", "25", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("BDF LAW GROUP", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("SANDHU LAW GROUP", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("GREENSPOON MARDER", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "N/A", False, False, False, "", "Moved up 3/10", "Planned engagement", "N/A", "", "", "", "", ""),
        ("QUINTAIROS, PRIETO, WOOD", True, "Not in top 10", "8", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("DOYLE & FOUTTY", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("PETOSA LAW", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("CLUNK HOOSE", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("WILFORD GESKE & COOK", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "N/A", False, False, False, "", "", "Planned engagement", "N/A", "", "", "", "", ""),
        ("MANLEY DEAS KOCHALSKI", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("MCCABE", True, "Not in top 10", "14", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("MILLSAP & SINGER", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("BONIAL", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("DELUCA LAW GROUP", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("FRENKEL LAMBERT WEISS WEISMAN & GORDON", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("ROACH & LIN", True, "Not in top 10", "24", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("MACKIE WOLF ZIENTZ & MANN", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("LIEBO WEINGARDEN DOBIE & BARBEE", True, "Not in top 10", "Not in top 90%", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "Planned engagement", "", "", "", "", "", ""),
        ("GRAY & ASSOCIATES", True, "Not in top 10", "Not in top 90%", False, False, "Fell off", "Wave 3 (by M3)", "", False, False, False, "", "Moved up from wave 3 as of 2/5", "Planned engagement", "", "", "", "", "", ""),
        ("BARRETT DAFFIN FRAPPIER TREDER & WEISS", True, "Not in top 10", "9", False, False, "No", "Wave 3 (by M3)", "", False, False, False, "", "", "N/A", "", "", "", "", "", ""),
        ("GROSS POLOWY LLC", True, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 1 (Feb 26)", "", False, False, False, "", "Moved up 3/10", "Planned engagement", "", "", "", "", "", ""),
        ("BAER & TIMBERLAKE", False, "Not in top 10", "Not in top 90%", False, False, "Yes", "Wave 2 (March 26)", "", False, False, False, "", "Moved up 3/10", "N/A", "", "", "", "", "", ""),
        ("REISENFELD & ASSOCIATES", False, "Not in top 10", "20", False, False, "Yes", "Wave 2 (March 26)", "", False, False, False, "", "Moved up 3/10", "N/A", "", "", "", "", "", ""),
    ]

    for row in tracker_data:
        (name, vm, nrz, lc, dp, m1, m2, wave, reach, p0, lm, dm, le, wnotes,
         interaction, ops_email, lc_name, lc_title, lc_email, nda, notes) = row
        firm = session.query(IndexedFirm).filter(IndexedFirm.name == name).first()
        if not firm:
            continue
        t = FirmTracker(
            indexed_firm_id=firm.id,
            vm_firm=vm,
            nrz_rank=nrz,
            loancare_rank=lc,
            ocean_design_partner=dp,
            ocean_m1=m1,
            ocean_m2=m2,
            proposed_wave=wave,
            last_reachout=reach,
            phase0_meeting=p0,
            leadership_meeting=lm,
            design_meeting=dm,
            leadership_engagement=le,
            wave_notes=wnotes,
            interaction=interaction,
            ops_contact_email=ops_email,
            leadership_contact=lc_name,
            leadership_title=lc_title,
            leadership_email=lc_email,
            nda_executed_by=nda,
            notes=notes,
        )
        session.add(t)
    session.commit()
    print(f"Seeded {len(tracker_data)} firm tracker entries.")
