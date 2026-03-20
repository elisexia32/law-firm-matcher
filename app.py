"""
Law Firm Matcher v3 - Streamlit App

Two integrated pages:
  1. Search & Index — hero search, tiered results, click-to-associate, firm directory
  2. Onboard Firms  — upload → dedupe → inline review → wave assignment (one flow)
"""
import streamlit as st
import pandas as pd
from pathlib import Path
from models import (
    get_session, IndexedFirm, Moniker, MaRule,
    FirmTracker, ServicerList, ServicerListEntry
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
    ["Search & Index", "Firm Tracker", "Ingest List"],
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
st.sidebar.caption("v3.0")


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
            # ----- confident matches -----
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
                if monikers:
                    st.caption("Also known as: " + ", ".join(m.name for m in monikers))

                # Quick-associate right from search
                with st.expander("Associate this search term as a moniker"):
                    ac1, ac2 = st.columns([2, 1])
                    with ac1:
                        src = st.text_input("Source", placeholder="e.g., ServiceMac", key=f"qsrc_{firm.id}")
                    with ac2:
                        if st.button("Link", key=f"qlink_{firm.id}", type="primary"):
                            add_moniker(session, firm.id, query.strip(), src)
                            st.success(f"Linked '{query.strip()}' → {firm.name}")
                            st.rerun()

            # ----- medium confidence -----
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
            # ----- no high confidence, but some medium -----
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
            # ----- nothing useful -----
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
    # Firm directory (below search)
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

    # Add new firm inline
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

    # Firm list
    for firm in firms:
        monikers = get_monikers_for_firm(session, firm.id)
        moniker_str = ", ".join(
            f"{m.name} ({m.source})" if m.source else m.name for m in monikers
        )

        with st.container():
            col1, col2, col3 = st.columns([4, 4, 1])
            with col1:
                status_icon = "" if firm.is_active else " ⛔"
                onboarding = _onboarding_badge(firm)
                st.markdown(f"**{firm.name}**{status_icon}{onboarding}")
                if firm.notes:
                    st.caption(firm.notes)
            with col2:
                if moniker_str:
                    st.caption(f"Monikers: {moniker_str}")
                else:
                    st.caption("No monikers")
            with col3:
                if st.button("Edit", key=f"edit_{firm.id}"):
                    st.session_state[f"editing_{firm.id}"] = not st.session_state.get(f"editing_{firm.id}", False)

            # Inline edit panel (monikers + M&A + status all in one place)
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

                        # M&A rules for this firm
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

                        # Add M&A rule inline
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


# ===================================================================
# PAGE 2: FIRM TRACKER
# ===================================================================
elif page == "Firm Tracker":
    st.title("Firm Tracker")
    st.caption("Onboarding status for all indexed firms. Edit inline — this is the source of truth.")

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

    # --- Main table view ---
    WAVE_OPTIONS = ["", "Pilot", "Wave 1 (Feb 26)", "Wave 2 (March 26)", "Wave 3 (by M3)", "N/A"]
    INTERACTION_OPTIONS = ["", "Design partner", "Intro call", "Planned engagement", "N/A"]

    # Build editable dataframe
    tracker_rows = []
    for t in trackers:
        firm = t.indexed_firm
        tracker_rows.append({
            "_id": t.id,
            "_firm_id": firm.id,
            "Firm Name": firm.name,
            "VM?": t.vm_firm,
            "FCL Cases": t.vm_active_fcl or "",
            "BK Cases": t.vm_active_bk or "",
            "NRZ Rank": t.nrz_rank or "",
            "LC Rank": t.loancare_rank or "",
            "Design Partner": t.ocean_design_partner,
            "M1": t.ocean_m1,
            "M2": t.ocean_m2 or "",
            "M2 Vol": t.ocean_m2_volume or "",
            "Wave": t.proposed_wave or "",
            "Training": t.live_training if t.live_training is not None else False,
            "Wave Notes": t.wave_notes or "",
            "Last Reach-out": t.last_reachout or "",
            "Phase 0": t.phase0_meeting,
            "Leadership Mtg": t.leadership_meeting,
            "Design Mtg": t.design_meeting,
            "Leadership Engaged": t.leadership_engagement or "",
            "Interaction": t.interaction or "",
            "Ops Email": t.ops_contact_email or "",
            "Leadership": t.leadership_contact or "",
            "Title": t.leadership_title or "",
            "Leadership Email": t.leadership_email or "",
            "NDA By": t.nda_executed_by or "",
            "Notes": t.notes or "",
        })

    if not tracker_rows:
        st.info("No tracker data yet. Add firms via Search & Index, then they'll appear here.")
    else:
        df = pd.DataFrame(tracker_rows)

        # Editable table
        edited_df = st.data_editor(
            df.drop(columns=["_id", "_firm_id"]),
            use_container_width=True,
            hide_index=True,
            disabled=["Firm Name"],
            column_config={
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
            },
            key="tracker_editor",
        )

        if st.button("Save Changes", type="primary"):
            for i, row in edited_df.iterrows():
                t_id = df.iloc[i]["_id"]
                firm_id = df.iloc[i]["_firm_id"]
                upsert_tracker(
                    session, firm_id,
                    vm_firm=row["VM?"],
                    vm_active_fcl=row["FCL Cases"],
                    vm_active_bk=row["BK Cases"],
                    nrz_rank=row["NRZ Rank"],
                    loancare_rank=row["LC Rank"],
                    ocean_design_partner=row["Design Partner"],
                    ocean_m1=row["M1"],
                    ocean_m2=row["M2"],
                    ocean_m2_volume=row["M2 Vol"],
                    proposed_wave=row["Wave"],
                    live_training=row["Training"],
                    wave_notes=row["Wave Notes"],
                    last_reachout=row["Last Reach-out"],
                    phase0_meeting=row["Phase 0"],
                    leadership_meeting=row["Leadership Mtg"],
                    design_meeting=row["Design Mtg"],
                    leadership_engagement=row["Leadership Engaged"],
                    interaction=row["Interaction"],
                    ops_contact_email=row["Ops Email"],
                    leadership_contact=row["Leadership"],
                    leadership_title=row["Title"],
                    leadership_email=row["Leadership Email"],
                    nda_executed_by=row["NDA By"],
                    notes=row["Notes"],
                )
            st.success(f"Saved {len(edited_df)} rows.")
            st.rerun()

        # Export
        export_df = edited_df.copy()
        csv = export_df.to_csv(index=False)
        st.download_button("Export CSV", csv, "firm_tracker.csv", "text/csv")


# ===================================================================
# PAGE 3: INGEST LIST
# ===================================================================
elif page == "Ingest List":
    st.title("Ingest Servicer List")
    st.caption("Upload a new firm list → auto-dedupe against the index → review → add to tracker.")

    # Step 1: Upload
    col1, col2 = st.columns(2)
    with col1:
        servicer = st.selectbox("Servicer", SERVICERS)
    with col2:
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

    # Step 2: Review
    list_id = st.session_state.get("ingest_list_id")
    results = st.session_state.get("ingest_results")

    if list_id and results:
        st.divider()
        st.subheader("Review matches")

        c1, c2, c3 = st.columns(3)
        c1.metric("Auto-matched", len(results["auto_matched"]))
        c2.metric("Needs Review", len(results["review"]))
        c3.metric("New Firms", len(results["new"]))

        entries = session.query(ServicerListEntry).filter(
            ServicerListEntry.servicer_list_id == list_id
        ).all()

        if results["auto_matched"]:
            with st.expander(f"Auto-matched ({len(results['auto_matched'])})", expanded=False):
                for item in results["auto_matched"]:
                    st.markdown(f"**{item['raw_name']}** → {item['matched_to']}  `{item['score']:.0f}%`  _{item['via']}_")

        if results["review"]:
            with st.expander(f"Needs Review ({len(results['review'])})", expanded=True):
                for item in results["review"]:
                    entry = next((e for e in entries if e.raw_name == item["raw_name"] and e.match_status == "review"), None)
                    if not entry:
                        continue
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

        if results["new"]:
            with st.expander(f"New Firms ({len(results['new'])})", expanded=True):
                for item in results["new"]:
                    entry = next((e for e in entries if e.raw_name == item["raw_name"] and e.match_status == "new"), None)
                    if not entry:
                        continue
                    nc1, nc2 = st.columns([3, 1])
                    with nc1:
                        st.markdown(f"**{item['raw_name']}**")
                        if item.get("best_candidate"):
                            st.caption(f"Closest: {item['best_candidate']} ({item['score']:.0f}%)")
                    with nc2:
                        if st.button("Add to Index", key=f"addidx_{entry.id}"):
                            new_firm = create_firm(session, item["raw_name"])
                            confirm_match(session, entry.id, new_firm.id)
                            # Also create a tracker entry
                            upsert_tracker(session, new_firm.id, interaction="Planned engagement")
                            st.success(f"Indexed & tracked: {new_firm.name}")
                            st.rerun()

    # Past ingestions
    st.divider()
    st.subheader("Past Ingestions")
    lists = get_all_servicer_lists(session)
    if lists:
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
                if sl_entries:
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
    else:
        st.info("No lists ingested yet.")
