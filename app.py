"""
Law Firm Matcher v3 - Streamlit App

Two pages:
  1. Search & Index — hero search, tiered results, firm directory, list ingestion + mapping export
  2. Firm Tracker   — onboarding status synced from Google Sheet
"""
import streamlit as st
import pandas as pd
from pathlib import Path
from models import (
    get_session, IndexedFirm, Moniker, MaRule,
    FirmTracker, ServicerList, ServicerListEntry, ValonosEntity
)
from db import (
    get_all_firms, get_firm_by_id, get_firm_by_name,
    create_firm, update_firm, delete_firm,
    get_monikers_for_firm, add_moniker, delete_moniker,
    get_all_ma_rules, add_ma_rule, delete_ma_rule,
    search_firms, check_ma_rules,
    create_servicer_list, get_all_servicer_lists, get_servicer_list,
    ingest_firm_list, confirm_match, reject_match,
    get_all_trackers, get_tracker_for_firm, upsert_tracker,
    seed_initial_data,
)
from matching import normalize_name, find_matches, calculate_match_score

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Law Firm Matcher", page_icon="⚖️", layout="wide")
Path("data").mkdir(exist_ok=True)
session = get_session()
seed_initial_data(session)

SERVICERS = ["Valon Mortgage", "ServiceMac", "NewRez", "LoanCare", "Other"]

# Wave dates used to determine if a wave has launched yet
_WAVE_DATES = {
    "Pilot": "2025-12-01",
    "Wave 1 (Feb 26)": "2026-02-26",
    "Wave 2 (March 26)": "2026-03-26",
}

def _onboarding_badge(firm):
    """Return an onboarding status badge string for a firm, or '' if none."""
    tracker = get_tracker_for_firm(session, firm.id)
    if not tracker or not tracker.proposed_wave or tracker.proposed_wave == "N/A":
        return ""
    wave = tracker.proposed_wave
    from datetime import date
    today = date.today()
    cutoff = _WAVE_DATES.get(wave)
    if cutoff and date.fromisoformat(cutoff) <= today:
        return f"  `🟢 Onboarded — {wave}`"
    else:
        return f"  `🔵 Planned — {wave}`"

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("⚖️ Law Firm Matcher")
page = st.sidebar.radio(
    "Navigate",
    ["Search & Index", "Firm Tracker", "Client Overlap"],
    label_visibility="collapsed",
)

firm_count = session.query(IndexedFirm).filter(IndexedFirm.is_active == True).count()
inactive_count = session.query(IndexedFirm).filter(IndexedFirm.is_active == False).count()
moniker_count = session.query(Moniker).count()
rule_count = session.query(MaRule).count()

st.sidebar.divider()
c1, c2, c3 = st.sidebar.columns(3)
c1.metric("Active Firms", firm_count)
c2.metric("Monikers", moniker_count)
c3.metric("M&A Rules", rule_count)
if inactive_count:
    st.sidebar.caption(f"{inactive_count} inactive firm{'s' if inactive_count != 1 else ''}")
st.sidebar.divider()
st.sidebar.caption("v3.1")


# ===================================================================
# PAGE 1: SEARCH & INDEX
# ===================================================================
if page == "Search & Index":
    st.title("Search & Index")
    st.caption("Source of truth for indexed law firms across all servicers.")

    # ------------------------------------------------------------------
    # Hero search
    # ------------------------------------------------------------------
    query = st.text_input(
        "Search for a law firm",
        placeholder="Is this firm already covered? Type a name to find out...",
        label_visibility="collapsed",
    )

    if query and query.strip():
        results = search_firms(session, query)

        high = [r for r in results if r["score"] >= 90]
        medium = [r for r in results if 70 <= r["score"] < 90]
        low = [r for r in results if r["score"] < 70]

        if high:
            for r in high:
                firm = r["firm"]
                monikers = get_monikers_for_firm(session, firm.id)
                badge = ""
                if r["match_type"] == "M&A rule":
                    badge = f"  `M&A: {r['context']}`"
                elif r["match_type"] in ("moniker", "moniker (fuzzy)"):
                    badge = f"  `via: {r['matched_name']}`"
                elif r["match_type"] == "fuzzy":
                    badge = f"  `{r['score']:.0f}% match`"

                status = "✅" if firm.is_active else "⛔ inactive"
                onboarding = _onboarding_badge(firm)
                st.markdown(f"### {firm.name} {status}{badge}{onboarding}")
                # Show ValonOS entity names
                firm_valonos = session.query(ValonosEntity).filter(
                    ValonosEntity.indexed_firm_id == firm.id
                ).all()
                if firm_valonos:
                    vnames = ", ".join(f"{v.name} ({v.tenant_name})" for v in firm_valonos)
                    st.caption(f"ValonOS: {vnames}")
                if monikers:
                    st.caption("Also known as: " + ", ".join(m.name for m in monikers))

                with st.expander("Associate this search term as a moniker"):
                    ac1, ac2 = st.columns([2, 1])
                    with ac1:
                        src = st.text_input("Source", placeholder="e.g., ServiceMac", key=f"qsrc_{firm.id}")
                    with ac2:
                        if st.button("Link", key=f"qlink_{firm.id}", type="primary"):
                            add_moniker(session, firm.id, query.strip(), src)
                            st.success(f"Linked '{query.strip()}' → {firm.name}")
                            st.rerun()

            if medium:
                st.divider()
                st.markdown("###### Other possible matches")
                for r in medium:
                    firm = r["firm"]
                    onboarding = _onboarding_badge(firm)
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"**{firm.name}** — {r['score']:.0f}% match{onboarding}")
                    with col2:
                        if st.button("Associate", key=f"massoc_{firm.id}"):
                            add_moniker(session, firm.id, query.strip(), "search association")
                            st.success(f"Linked → {firm.name}")
                            st.rerun()

        elif medium:
            st.info("Hmm, no strong match found.")
            st.markdown("###### Could this be associated with:")
            for r in medium:
                firm = r["firm"]
                onboarding = _onboarding_badge(firm)
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**{firm.name}** — {r['score']:.0f}% match{onboarding}")
                    monikers = get_monikers_for_firm(session, firm.id)
                    if monikers:
                        st.caption("AKA: " + ", ".join(m.name for m in monikers))
                with col2:
                    if st.button("Associate", key=f"lassoc_{firm.id}"):
                        add_moniker(session, firm.id, query.strip(), "search association")
                        st.success(f"Linked → {firm.name}")
                        st.rerun()

        else:
            st.warning("Hmm, I couldn't find anything.")
            if low:
                st.markdown(
                    "<p style='color:gray'>Could this be associated with:</p>",
                    unsafe_allow_html=True,
                )
                for r in low:
                    firm = r["firm"]
                    onboarding = _onboarding_badge(firm)
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.caption(f"{firm.name} — {r['score']:.0f}%{onboarding}")
                    with col2:
                        if st.button("Associate", key=f"llassoc_{firm.id}"):
                            add_moniker(session, firm.id, query.strip(), "search association")
                            st.success(f"Linked → {firm.name}")
                            st.rerun()

            if st.button("➕ Add as new indexed firm"):
                new_firm = create_firm(session, query.strip())
                st.success(f"Created: {new_firm.name}")
                st.rerun()

    # ------------------------------------------------------------------
    # Upload & Ingest (always visible, collapsed by default)
    # ------------------------------------------------------------------
    st.divider()
    with st.expander("📤 Upload a servicer list to dedupe & map"):
        st.caption("Upload → auto-dedupe against the index → review → export mapping for eng.")

        ic1, ic2 = st.columns(2)
        with ic1:
            servicer = st.selectbox("Servicer", SERVICERS)
        with ic2:
            list_label = st.text_input("Label (optional)", placeholder="e.g., Q1 2026 refresh")

        uploaded = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])

        if uploaded:
            if uploaded.name.endswith(".xlsx"):
                df = pd.read_excel(uploaded)
            else:
                df = pd.read_csv(uploaded)

            st.dataframe(df.head(10), use_container_width=True, hide_index=True)
            name_col = st.selectbox("Column containing firm names", df.columns.tolist())

            if st.button("Run Dedupe & Match", type="primary"):
                raw_names = df[name_col].dropna().tolist()
                with st.spinner(f"Matching {len(raw_names)} firms against the index..."):
                    sl = create_servicer_list(session, servicer, list_label or "", uploaded.name)
                    results = ingest_firm_list(session, sl.id, raw_names)
                st.session_state["ingest_list_id"] = sl.id
                st.session_state["ingest_results"] = results

    # Review results (shown outside the expander so they stay visible)
    list_id = st.session_state.get("ingest_list_id")
    ingest_results = st.session_state.get("ingest_results")

    if list_id and ingest_results:
        st.subheader("Review matches")

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Auto-matched", len(ingest_results["auto_matched"]))
        mc2.metric("Needs Review", len(ingest_results["review"]))
        mc3.metric("New Firms", len(ingest_results["new"]))

        entries = session.query(ServicerListEntry).filter(
            ServicerListEntry.servicer_list_id == list_id
        ).all()

        if ingest_results["auto_matched"]:
            with st.expander(f"Auto-matched ({len(ingest_results['auto_matched'])})", expanded=False):
                for item in ingest_results["auto_matched"]:
                    st.markdown(f"**{item['raw_name']}** → {item['matched_to']}  `{item['score']:.0f}%`  _{item['via']}_")

        if ingest_results["review"]:
            with st.expander(f"Needs Review ({len(ingest_results['review'])})", expanded=True):
                # Collect review entries that still need action
                review_entries = []
                for item in ingest_results["review"]:
                    entry = next((e for e in entries if e.raw_name == item["raw_name"] and e.match_status == "review"), None)
                    if entry:
                        review_entries.append((item, entry))

                if review_entries:
                    # Bulk actions
                    select_all = st.checkbox("Select all", key="select_all_review")
                    selected_ids = []

                    for item, entry in review_entries:
                        rc_sel, rc_info = st.columns([0.3, 5])
                        with rc_sel:
                            checked = st.checkbox("", value=select_all, key=f"sel_{entry.id}", label_visibility="collapsed")
                            if checked:
                                selected_ids.append(entry.id)
                        with rc_info:
                            st.markdown(f"**{item['raw_name']}** → suggested: **{item['matched_to']}** `{item['score']:.0f}%`")
                        rc1, rc2, rc3 = st.columns([1, 1, 3])
                        with rc1:
                            if st.button("Confirm", key=f"conf_{entry.id}"):
                                confirm_match(session, entry.id)
                                st.rerun()
                        with rc2:
                            if st.button("Reject", key=f"rej_{entry.id}"):
                                reject_match(session, entry.id)
                                st.rerun()
                        with rc3:
                            all_firms = get_all_firms(session)
                            firm_opts = {f.name: f.id for f in all_firms}
                            reassign = st.selectbox("Reassign to", options=[""] + list(firm_opts.keys()), key=f"reassign_{entry.id}", label_visibility="collapsed")
                            if reassign and st.button("Reassign", key=f"do_reassign_{entry.id}"):
                                confirm_match(session, entry.id, firm_opts[reassign])
                                st.rerun()
                        st.divider()

                    # Bulk confirm / reject buttons
                    bc1, bc2, _ = st.columns([1, 1, 3])
                    with bc1:
                        if st.button("Confirm selected", type="primary", disabled=len(selected_ids) == 0):
                            for eid in selected_ids:
                                confirm_match(session, eid)
                            st.rerun()
                    with bc2:
                        if st.button("Reject selected", disabled=len(selected_ids) == 0):
                            for eid in selected_ids:
                                reject_match(session, eid)
                            st.rerun()

        if ingest_results["new"]:
            with st.expander(f"New Firms ({len(ingest_results['new'])})", expanded=True):
                all_firms_for_new = get_all_firms(session)
                firm_opts_new = {f.name: f.id for f in all_firms_for_new}

                for item in ingest_results["new"]:
                    entry = next((e for e in entries if e.raw_name == item["raw_name"] and e.match_status == "new"), None)
                    if not entry:
                        continue
                    st.markdown(f"**{item['raw_name']}**")
                    if item.get("best_candidate"):
                        st.caption(f"Closest match: {item['best_candidate']} ({item['score']:.0f}%)")

                    nc1, nc2, nc3 = st.columns([2, 2, 1])
                    with nc1:
                        # Link to closest match
                        if item.get("best_candidate") and item["best_candidate"] in firm_opts_new:
                            if st.button(f"Link to \"{item['best_candidate']}\"", key=f"link_{entry.id}"):
                                confirm_match(session, entry.id, firm_opts_new[item["best_candidate"]])
                                st.rerun()
                        else:
                            # Manual pick from index
                            link_to = st.selectbox(
                                "Link to existing firm",
                                options=[""] + list(firm_opts_new.keys()),
                                key=f"linkpick_{entry.id}",
                                label_visibility="collapsed",
                            )
                            if link_to and st.button("Link", key=f"dolink_{entry.id}"):
                                confirm_match(session, entry.id, firm_opts_new[link_to])
                                st.rerun()
                    with nc2:
                        if st.button("Add as new firm", key=f"addidx_{entry.id}"):
                            new_firm = create_firm(session, item["raw_name"])
                            confirm_match(session, entry.id, new_firm.id)
                            upsert_tracker(session, new_firm.id, interaction="Planned engagement")
                            st.success(f"Indexed & tracked: {new_firm.name}")
                            st.rerun()
                    st.divider()

        # --- Export mapping ---
        st.divider()
        st.subheader("Export mapping")
        st.caption(
            "Download the client → ValonOS name mapping for eng. "
            "Only includes confirmed and auto-matched entries."
        )

        resolved_entries = session.query(ServicerListEntry).filter(
            ServicerListEntry.servicer_list_id == list_id,
            ServicerListEntry.match_status.in_(["auto_matched", "confirmed"]),
        ).all()

        if resolved_entries:
            mapping_rows = []
            for e in resolved_entries:
                firm = get_firm_by_id(session, e.matched_firm_id) if e.matched_firm_id else None
                if firm:
                    valonos_ent = session.query(ValonosEntity).filter(
                        ValonosEntity.indexed_firm_id == firm.id,
                        ValonosEntity.tenant_key == 1,
                    ).first()
                    mapping_rows.append({
                        "client_law_firm_name": e.raw_name,
                        "indexed_name": firm.name,
                        "valonos_entity_name": valonos_ent.name if valonos_ent else "",
                        "match_score": round(e.match_score, 1) if e.match_score else None,
                        "match_status": e.match_status,
                    })

            if mapping_rows:
                mapping_df = pd.DataFrame(mapping_rows)
                st.dataframe(mapping_df, use_container_width=True, hide_index=True)

                ec1, ec2 = st.columns(2)
                with ec1:
                    csv_data = mapping_df.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        csv_data,
                        "law_firm_mapping.csv",
                        "text/csv",
                    )
                with ec2:
                    import json as _json
                    json_mapping = {
                        r["client_law_firm_name"]: r["valonos_entity_name"] or r["indexed_name"]
                        for r in mapping_rows
                    }
                    st.download_button(
                        "Download JSON",
                        _json.dumps(json_mapping, indent=2),
                        "law_firm_mapping.json",
                        "application/json",
                    )
            else:
                st.info("No resolved mappings yet. Confirm matches above first.")
        else:
            st.info("No resolved mappings yet. Confirm matches above first.")

    # ------------------------------------------------------------------
    # Firm directory (always visible)
    # ------------------------------------------------------------------
    st.divider()

    dir_col1, dir_col2 = st.columns([3, 1])
    with dir_col1:
        filter_text = st.text_input("Filter firms...", placeholder="Type to filter the index...", key="filter_idx")
    with dir_col2:
        show_inactive = st.checkbox("Show inactive", value=False)

    firms = get_all_firms(session)
    if not show_inactive:
        firms = [f for f in firms if f.is_active]
    if filter_text:
        firms = [f for f in firms if filter_text.lower() in f.name.lower()]

    st.subheader(f"Indexed Firms ({len(firms)})")

    # ValonOS sync + Add firm side by side
    tool_col1, tool_col2 = st.columns(2)
    with tool_col1:
        with st.expander("🔗 Sync ValonOS Entities"):
            st.caption("Pull law firm entities from ValonOS production and auto-match to indexed firms.")
            if st.button("Sync from BigQuery", type="primary", key="valonos_sync"):
                from valonos_sync import sync_valonos_entities
                try:
                    with st.spinner("Querying BigQuery..."):
                        result = sync_valonos_entities(session)
                    st.success(
                        f"Synced {result['total']} entities: "
                        f"{result['matched']} matched, {len(result['unmatched'])} unmatched."
                    )
                    if result["unmatched"]:
                        unique_unmatched = sorted(set(result["unmatched"]))
                        st.warning(
                            f"Unmatched: {', '.join(unique_unmatched[:10])}"
                            + ("..." if len(unique_unmatched) > 10 else "")
                        )
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")
    with tool_col2:
        with st.expander("➕ Add New Firm"):
            nc1, nc2 = st.columns([3, 1])
            with nc1:
                new_name = st.text_input("Firm name", key="new_firm_name")
            with nc2:
                new_notes = st.text_input("Notes", key="new_firm_notes")
            if st.button("Add Firm", key="add_firm_btn"):
                if new_name:
                    existing = get_firm_by_name(session, new_name)
                    if existing:
                        st.error(f"Already exists: {existing.name}")
                    else:
                        create_firm(session, new_name, new_notes)
                        st.success(f"Added: {new_name}")
                        st.rerun()

    # Pre-load ValonOS entities for all firms
    all_valonos = session.query(ValonosEntity).all()
    valonos_by_firm = {}
    for v in all_valonos:
        if v.indexed_firm_id:
            valonos_by_firm.setdefault(v.indexed_firm_id, []).append(v)

    for firm in firms:
        monikers = get_monikers_for_firm(session, firm.id)
        moniker_str = ", ".join(
            f"{m.name} ({m.source})" if m.source else m.name for m in monikers
        )
        valonos_ents = valonos_by_firm.get(firm.id, [])
        valonos_str = ", ".join(
            f"{v.name} ({v.tenant_name})" for v in valonos_ents
        )

        with st.container():
            col1, col2, col3 = st.columns([4, 4, 1])
            with col1:
                status_icon = "" if firm.is_active else " ⛔"
                onboarding = _onboarding_badge(firm)
                st.markdown(f"**{firm.name}**{status_icon}{onboarding}")
                if valonos_str:
                    st.caption(f"ValonOS: {valonos_str}")
                elif firm.notes:
                    st.caption(firm.notes)
            with col2:
                if moniker_str:
                    st.caption(f"Monikers: {moniker_str}")
                else:
                    st.caption("No monikers")
            with col3:
                if st.button("Edit", key=f"edit_{firm.id}"):
                    st.session_state[f"editing_{firm.id}"] = not st.session_state.get(f"editing_{firm.id}", False)

            if st.session_state.get(f"editing_{firm.id}"):
                with st.container():
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        edit_name = st.text_input("Name", value=firm.name, key=f"ename_{firm.id}")
                        edit_notes = st.text_input("Notes", value=firm.notes or "", key=f"enotes_{firm.id}")
                        edit_active = st.checkbox("Active", value=firm.is_active, key=f"eactive_{firm.id}")

                    with ec2:
                        st.markdown("**Monikers**")
                        for m in monikers:
                            mc1, mc2 = st.columns([4, 1])
                            with mc1:
                                st.text(f"{m.name} ({m.source})" if m.source else m.name)
                            with mc2:
                                if st.button("✕", key=f"delm_{m.id}"):
                                    delete_moniker(session, m.id)
                                    st.rerun()

                        nm1, nm2, nm3 = st.columns([3, 2, 1])
                        with nm1:
                            new_mon = st.text_input("New moniker", key=f"nmon_{firm.id}")
                        with nm2:
                            new_src = st.text_input("Source", key=f"nsrc_{firm.id}")
                        with nm3:
                            if st.button("Add", key=f"addm_{firm.id}"):
                                if new_mon:
                                    add_moniker(session, firm.id, new_mon, new_src)
                                    st.rerun()

                        firm_rules = [r for r in get_all_ma_rules(session) if r.acquiring_firm_id == firm.id]
                        if firm_rules:
                            st.markdown("**M&A Rules**")
                            for rule in firm_rules:
                                rc1, rc2 = st.columns([4, 1])
                                with rc1:
                                    st.caption(f"{rule.acquired_name} → this firm ({rule.context_label})")
                                with rc2:
                                    if st.button("✕", key=f"delrule_{rule.id}"):
                                        delete_ma_rule(session, rule.id)
                                        st.rerun()

                        st.markdown("**Add M&A Rule**")
                        mr1, mr2, mr3 = st.columns([3, 2, 1])
                        with mr1:
                            acq_name = st.text_input("Acquired firm name", key=f"macq_{firm.id}")
                        with mr2:
                            ctx_label = st.text_input("Context", key=f"mctx_{firm.id}")
                        with mr3:
                            if st.button("Add", key=f"maadd_{firm.id}"):
                                if acq_name and ctx_label:
                                    add_ma_rule(session, acq_name, firm.id, ctx_label)
                                    st.rerun()

                    bc1, bc2, bc3 = st.columns([1, 1, 4])
                    with bc1:
                        if st.button("Save", key=f"save_{firm.id}", type="primary"):
                            update_firm(session, firm.id, edit_name, edit_notes, edit_active)
                            st.session_state[f"editing_{firm.id}"] = False
                            st.rerun()
                    with bc2:
                        if st.button("Cancel", key=f"cancel_{firm.id}"):
                            st.session_state[f"editing_{firm.id}"] = False
                            st.rerun()

            st.divider()

    # Past ingestions (at the bottom)
    lists = get_all_servicer_lists(session)
    if lists:
        st.subheader("Past Ingestions")
        for sl in lists:
            sl_entries = session.query(ServicerListEntry).filter(
                ServicerListEntry.servicer_list_id == sl.id
            ).all()
            statuses = {}
            for e in sl_entries:
                statuses[e.match_status] = statuses.get(e.match_status, 0) + 1

            with st.expander(
                f"{sl.servicer_name} — {sl.milestone or sl.filename} "
                f"({len(sl_entries)} firms) — {sl.uploaded_at.strftime('%Y-%m-%d %H:%M')}"
            ):
                st.caption(f"Status: {statuses}")

                resolved = [e for e in sl_entries if e.match_status in ("auto_matched", "confirmed") and e.matched_firm_id]
                if resolved:
                    past_mapping = []
                    for e in resolved:
                        firm = get_firm_by_id(session, e.matched_firm_id)
                        past_mapping.append({
                            "client_law_firm_name": e.raw_name,
                            "valon_law_firm_name": firm.name if firm else "—",
                            "match_score": round(e.match_score, 1) if e.match_score else None,
                            "match_status": e.match_status,
                        })
                    past_df = pd.DataFrame(past_mapping)
                    st.dataframe(past_df, use_container_width=True, hide_index=True)

                    pe1, pe2 = st.columns(2)
                    with pe1:
                        st.download_button(
                            "Download CSV",
                            past_df.to_csv(index=False),
                            f"mapping_{sl.servicer_name}_{sl.id}.csv",
                            "text/csv",
                            key=f"past_csv_{sl.id}",
                        )
                    with pe2:
                        import json as _json
                        past_json = {r["client_law_firm_name"]: r["valon_law_firm_name"] for r in past_mapping}
                        st.download_button(
                            "Download JSON",
                            _json.dumps(past_json, indent=2),
                            f"mapping_{sl.servicer_name}_{sl.id}.json",
                            "application/json",
                            key=f"past_json_{sl.id}",
                        )
                elif sl_entries:
                    past_data = []
                    for e in sl_entries:
                        firm = get_firm_by_id(session, e.matched_firm_id) if e.matched_firm_id else None
                        past_data.append({
                            "Raw Name": e.raw_name,
                            "Indexed Firm": firm.name if firm else "—",
                            "Score": f"{e.match_score:.0f}%" if e.match_score else "",
                            "Status": e.match_status,
                        })
                    st.dataframe(pd.DataFrame(past_data), use_container_width=True, hide_index=True)


# ===================================================================
# PAGE 2: FIRM TRACKER
# ===================================================================
elif page == "Firm Tracker":
    st.title("Firm Tracker")
    st.caption("Onboarding status synced from the live Google Sheet.")

    # --- Google Sheets sync ---
    with st.expander("🔄 Sync from Google Sheet"):
        st.caption(
            "Pull the latest data from the live Firm Tracker spreadsheet. "
            "This overwrites local tracker values with the sheet data."
        )
        if st.button("Sync Now", type="primary"):
            from sheets_sync import sync_tracker_from_sheet
            try:
                with st.spinner("Fetching from Google Sheets..."):
                    result = sync_tracker_from_sheet(session)
                st.success(
                    f"Synced {result['total_sheet_rows']} rows: "
                    f"{result['created']} new, {result['updated']} updated."
                )
                if result["skipped"]:
                    st.warning(
                        f"Skipped {len(result['skipped'])} firms not in the index: "
                        + ", ".join(result["skipped"][:10])
                        + ("..." if len(result["skipped"]) > 10 else "")
                    )
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")

    # --- Filters ---
    fc1, fc2, fc3 = st.columns([2, 1, 1])
    with fc1:
        wave_filter = st.multiselect(
            "Filter by wave",
            ["Pilot", "Wave 1 (Feb 26)", "Wave 2 (March 26)", "Wave 3 (by M3)", "N/A", ""],
            default=[],
            key="wave_filter",
        )
    with fc2:
        interaction_filter = st.multiselect(
            "Filter by interaction",
            ["Design partner", "Intro call", "Planned engagement", "N/A", ""],
            default=[],
            key="interaction_filter",
        )
    with fc3:
        vm_only = st.checkbox("VM firms only", value=False)

    trackers = get_all_trackers(session)

    if wave_filter:
        trackers = [t for t in trackers if t.proposed_wave in wave_filter]
    if interaction_filter:
        trackers = [t for t in trackers if t.interaction in interaction_filter]
    if vm_only:
        trackers = [t for t in trackers if t.vm_firm]

    # --- Wave summary ---
    wave_counts = {}
    for t in trackers:
        w = t.proposed_wave or "Unassigned"
        wave_counts[w] = wave_counts.get(w, 0) + 1

    if wave_counts:
        ordered_waves = ["Pilot", "Wave 1 (Feb 26)", "Wave 2 (March 26)", "Wave 3 (by M3)", "N/A", "Unassigned"]
        display_waves = [w for w in ordered_waves if w in wave_counts]
        if display_waves:
            cols = st.columns(len(display_waves))
            for i, w in enumerate(display_waves):
                cols[i].metric(w, wave_counts[w])

    st.divider()

    # --- Column view selector ---
    WAVE_OPTIONS = ["", "Pilot", "Wave 1 (Feb 26)", "Wave 2 (March 26)", "Wave 3 (by M3)", "N/A"]
    INTERACTION_OPTIONS = ["", "Design partner", "Intro call", "Planned engagement", "N/A"]

    view = st.radio(
        "View",
        ["Onboarding", "Engagement", "Contacts", "All"],
        horizontal=True,
        key="tracker_view",
    )

    # Build full dataframe
    tracker_rows = []
    for t in trackers:
        firm = t.indexed_firm
        tracker_rows.append({
            "_id": t.id,
            "_firm_id": firm.id,
            "Firm Name": firm.name,
            # Onboarding view
            "VM?": t.vm_firm,
            "Wave": t.proposed_wave or "",
            "M2": t.ocean_m2 or "",
            "M2 Vol": t.ocean_m2_volume or "",
            "Training": t.live_training if t.live_training is not None else False,
            "Interaction": t.interaction or "",
            "Wave Notes": t.wave_notes or "",
            # Engagement view
            "Design Partner": t.ocean_design_partner,
            "M1": t.ocean_m1,
            "Phase 0": t.phase0_meeting,
            "Leadership Mtg": t.leadership_meeting,
            "Design Mtg": t.design_meeting,
            "Leadership Engaged": t.leadership_engagement or "",
            "Last Reach-out": t.last_reachout or "",
            # Contacts view
            "Ops Email": t.ops_contact_email or "",
            "Leadership": t.leadership_contact or "",
            "Title": t.leadership_title or "",
            "Leadership Email": t.leadership_email or "",
            "NDA By": t.nda_executed_by or "",
            # Extra detail
            "FCL Cases": t.vm_active_fcl or "",
            "BK Cases": t.vm_active_bk or "",
            "NRZ Rank": t.nrz_rank or "",
            "LC Rank": t.loancare_rank or "",
            "Notes": t.notes or "",
        })

    if not tracker_rows:
        st.info("No tracker data yet. Add firms via Search & Index, then they'll appear here.")
    else:
        df = pd.DataFrame(tracker_rows)

        # Select columns based on view
        always_hidden = ["_id", "_firm_id"]
        if view == "Onboarding":
            show_cols = ["Firm Name", "VM?", "Wave", "M2", "M2 Vol", "Training", "Interaction", "Wave Notes"]
        elif view == "Engagement":
            show_cols = ["Firm Name", "Wave", "Design Partner", "M1", "Phase 0",
                         "Leadership Mtg", "Design Mtg", "Leadership Engaged", "Last Reach-out"]
        elif view == "Contacts":
            show_cols = ["Firm Name", "Wave", "Interaction", "Ops Email", "Leadership",
                         "Title", "Leadership Email", "NDA By"]
        else:  # All
            show_cols = [c for c in df.columns if c not in always_hidden]

        display_df = df[show_cols]

        # Column configs (only include configs for visible columns)
        all_column_config = {
            "VM?": st.column_config.CheckboxColumn("VM?", width="small"),
            "FCL Cases": st.column_config.TextColumn("FCL", width="small"),
            "BK Cases": st.column_config.TextColumn("BK", width="small"),
            "Design Partner": st.column_config.CheckboxColumn("DP", width="small"),
            "M1": st.column_config.CheckboxColumn("M1", width="small"),
            "M2": st.column_config.SelectboxColumn("M2", options=["Yes", "No", "Fell off"], width="small"),
            "M2 Vol": st.column_config.TextColumn("M2 Vol", width="small"),
            "Wave": st.column_config.SelectboxColumn("Wave", options=WAVE_OPTIONS, width="medium"),
            "Training": st.column_config.CheckboxColumn("Training", width="small"),
            "Phase 0": st.column_config.CheckboxColumn("P0", width="small"),
            "Leadership Mtg": st.column_config.CheckboxColumn("Lead", width="small"),
            "Design Mtg": st.column_config.CheckboxColumn("Design", width="small"),
            "Interaction": st.column_config.SelectboxColumn("Interaction", options=INTERACTION_OPTIONS, width="medium"),
            "NRZ Rank": st.column_config.TextColumn("NRZ", width="small"),
            "LC Rank": st.column_config.TextColumn("LC", width="small"),
            "Wave Notes": st.column_config.TextColumn("Wave Notes", width="medium"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
        }
        visible_config = {k: v for k, v in all_column_config.items() if k in show_cols}

        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            disabled=["Firm Name"],
            column_config=visible_config,
            key="tracker_editor",
        )

        if st.button("Save Changes", type="primary"):
            for i, row in edited_df.iterrows():
                firm_id = df.iloc[i]["_firm_id"]
                # Build kwargs from whatever columns are visible
                kwargs = {}
                col_to_field = {
                    "VM?": "vm_firm",
                    "FCL Cases": "vm_active_fcl",
                    "BK Cases": "vm_active_bk",
                    "NRZ Rank": "nrz_rank",
                    "LC Rank": "loancare_rank",
                    "Design Partner": "ocean_design_partner",
                    "M1": "ocean_m1",
                    "M2": "ocean_m2",
                    "M2 Vol": "ocean_m2_volume",
                    "Wave": "proposed_wave",
                    "Training": "live_training",
                    "Wave Notes": "wave_notes",
                    "Last Reach-out": "last_reachout",
                    "Phase 0": "phase0_meeting",
                    "Leadership Mtg": "leadership_meeting",
                    "Design Mtg": "design_meeting",
                    "Leadership Engaged": "leadership_engagement",
                    "Interaction": "interaction",
                    "Ops Email": "ops_contact_email",
                    "Leadership": "leadership_contact",
                    "Title": "leadership_title",
                    "Leadership Email": "leadership_email",
                    "NDA By": "nda_executed_by",
                    "Notes": "notes",
                }
                for col_name, field_name in col_to_field.items():
                    if col_name in row.index:
                        kwargs[field_name] = row[col_name]
                upsert_tracker(session, firm_id, **kwargs)
            st.success(f"Saved {len(edited_df)} rows.")
            st.rerun()

        # Export
        csv = edited_df.to_csv(index=False)
        st.download_button("Export CSV", csv, "firm_tracker.csv", "text/csv")


# ===================================================================
# PAGE 3: CLIENT OVERLAP
# ===================================================================
elif page == "Client Overlap":
    st.title("Client Overlap Matrix")
    st.caption(
        "Upload a law firm list for a client and see how it overlaps "
        "with other clients and which firms are already onboarded."
    )

    # --- Upload a new client list ---
    with st.expander("📤 Upload a client firm list", expanded=True):
        ov_servicer = st.selectbox("Client / Servicer", SERVICERS, key="ov_servicer")
        ov_label = st.text_input("List label (optional)", key="ov_label", placeholder="e.g., Q2 2026 foreclosure panel")
        ov_col = st.text_input("Column containing firm names", key="ov_col", value="", placeholder="Leave blank to use first column")
        ov_file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"], key="ov_upload")

        if ov_file and st.button("Process list", type="primary", key="ov_process"):
            if ov_file.name.endswith(".xlsx"):
                ov_df = pd.read_excel(ov_file)
            else:
                ov_df = pd.read_csv(ov_file)

            # Pick the firm name column
            if ov_col and ov_col in ov_df.columns:
                raw_names = ov_df[ov_col].dropna().astype(str).tolist()
            else:
                raw_names = ov_df.iloc[:, 0].dropna().astype(str).tolist()

            # Ingest into the system so it shows up in the matrix
            sl = create_servicer_list(session, ov_servicer, ov_label or "", ov_file.name)
            results = ingest_firm_list(session, sl.id, raw_names)
            # Auto-confirm all auto_matched entries for the overlap view
            auto_entries = session.query(ServicerListEntry).filter(
                ServicerListEntry.servicer_list_id == sl.id,
                ServicerListEntry.match_status == "auto_matched",
            ).all()
            st.success(
                f"Processed {len(raw_names)} firms: "
                f"{len(results['auto_matched'])} matched, "
                f"{len(results['review'])} need review, "
                f"{len(results['new'])} new."
            )
            st.caption("Go to **Search & Index** to review/confirm unmatched firms, then come back here.")
            st.rerun()

    st.divider()

    # --- Build the overlap matrix from all uploaded servicer lists ---
    all_lists = session.query(ServicerList).order_by(ServicerList.uploaded_at.desc()).all()

    if not all_lists:
        st.info("No client lists uploaded yet. Upload one above to get started.")
    else:
        # Let user filter which lists to include
        list_options = {f"{sl.servicer_name} — {sl.milestone or sl.filename} ({sl.uploaded_at.strftime('%Y-%m-%d')})": sl.id for sl in all_lists}
        selected_labels = st.multiselect(
            "Select client lists to compare",
            options=list(list_options.keys()),
            default=list(list_options.keys())[:10],
        )
        selected_ids = [list_options[l] for l in selected_labels]

        if selected_ids:
            # Fetch all confirmed/auto_matched entries for selected lists
            matched_entries = session.query(ServicerListEntry).filter(
                ServicerListEntry.servicer_list_id.in_(selected_ids),
                ServicerListEntry.match_status.in_(["auto_matched", "confirmed"]),
                ServicerListEntry.matched_firm_id.isnot(None),
            ).all()

            if not matched_entries:
                st.warning("No matched firms found in the selected lists. Confirm matches on the Search & Index page first.")
            else:
                # Build a map: firm_id -> set of servicer list labels
                firm_to_clients = {}
                list_id_to_label = {v: k for k, v in list_options.items()}
                # Shorter labels for the matrix columns
                list_id_to_short = {}
                for sl in all_lists:
                    if sl.id in selected_ids:
                        list_id_to_short[sl.id] = f"{sl.servicer_name}" + (f" ({sl.milestone})" if sl.milestone else "")

                for entry in matched_entries:
                    fid = entry.matched_firm_id
                    if fid not in firm_to_clients:
                        firm_to_clients[fid] = set()
                    firm_to_clients[fid].add(entry.servicer_list_id)

                # Build matrix dataframe
                firm_ids = sorted(firm_to_clients.keys())
                firms_by_id = {f.id: f for f in session.query(IndexedFirm).filter(IndexedFirm.id.in_(firm_ids)).all()}
                tracker_by_firm = {}
                trackers = session.query(FirmTracker).filter(FirmTracker.indexed_firm_id.in_(firm_ids)).all()
                for t in trackers:
                    tracker_by_firm[t.indexed_firm_id] = t

                rows = []
                for fid in firm_ids:
                    firm = firms_by_id.get(fid)
                    if not firm:
                        continue
                    tracker = tracker_by_firm.get(fid)
                    row = {
                        "Firm": firm.name,
                        "# Clients": len(firm_to_clients[fid]),
                        "Onboarded": "✅" if (tracker and tracker.live_training) else "",
                        "Wave": tracker.proposed_wave if tracker else "",
                    }
                    for sid in selected_ids:
                        label = list_id_to_short.get(sid, str(sid))
                        row[label] = "✓" if sid in firm_to_clients[fid] else ""
                    rows.append(row)

                matrix_df = pd.DataFrame(rows)
                # Sort by number of clients (most shared first)
                matrix_df = matrix_df.sort_values("# Clients", ascending=False).reset_index(drop=True)

                # --- Summary metrics ---
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Total Firms", len(matrix_df))
                shared = len(matrix_df[matrix_df["# Clients"] > 1])
                sc2.metric("Shared (2+ clients)", shared)
                onboarded = len(matrix_df[matrix_df["Onboarded"] == "✅"])
                sc3.metric("Already Onboarded", onboarded)
                not_onboarded = len(matrix_df) - onboarded
                sc4.metric("Not Yet Onboarded", not_onboarded)

                st.divider()

                # --- Filter controls ---
                fc1, fc2 = st.columns(2)
                with fc1:
                    show_filter = st.radio(
                        "Show",
                        ["All firms", "Shared only (2+ clients)", "Not yet onboarded"],
                        horizontal=True,
                        key="ov_filter",
                    )
                with fc2:
                    search_firm = st.text_input("Search firm", key="ov_search", placeholder="Filter by name...")

                display_df = matrix_df.copy()
                if show_filter == "Shared only (2+ clients)":
                    display_df = display_df[display_df["# Clients"] > 1]
                elif show_filter == "Not yet onboarded":
                    display_df = display_df[display_df["Onboarded"] != "✅"]

                if search_firm:
                    display_df = display_df[display_df["Firm"].str.contains(search_firm, case=False, na=False)]

                st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

                # --- Export ---
                csv_overlap = display_df.to_csv(index=False)
                st.download_button("Export overlap matrix (CSV)", csv_overlap, "client_overlap_matrix.csv", "text/csv")
