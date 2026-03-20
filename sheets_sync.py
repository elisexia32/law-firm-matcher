"""
Google Sheets sync for the Firm Tracker.

Pulls data from the live Google Sheet via the Gestalt API and upserts
into the local FirmTracker table.
"""
import os
import json
import urllib.request
import urllib.error
from models import get_session, IndexedFirm, FirmTracker

SPREADSHEET_ID = "14MFzvj6bCjhyB_uqf9Z2KDChJwKr6ooKIXOUqZmmiRQ"
SHEET_RANGE = "Firm Tracker!A5:AC200"  # header row + up to ~195 data rows
GESTALT_BASE = "https://api.gestalt.peachstreet.dev/api/v1"

# Column indices in the sheet (0-based, from row 5 header)
COL = {
    "firm_name": 0,
    "vm_firm": 1,
    "vm_active_fcl": 2,
    "vm_active_bk": 3,
    # 4 is blank
    "nrz_rank": 5,
    "loancare_rank": 6,
    "ocean_design_partner": 7,
    "ocean_m1": 8,
    "ocean_m2": 9,
    "ocean_m2_volume": 10,
    "proposed_wave": 11,
    "live_training": 12,
    "last_reachout": 13,
    "phase0_meeting": 14,
    "leadership_meeting": 15,
    "design_meeting": 16,
    "leadership_engagement": 17,
    "wave_notes": 18,
    "interaction": 19,
    "ops_contact_email": 20,
    "leadership_contact": 21,
    "leadership_title": 22,
    "leadership_email": 23,
    # 24 is blank
}


def _get(row, idx):
    """Safely get a cell value from a row, returning '' if out of range."""
    if idx < len(row):
        return row[idx]
    return ""


def _bool(val):
    """Parse a sheet boolean value (Yes/TRUE/No/FALSE/blank)."""
    if not val:
        return False
    return val.strip().upper() in ("YES", "TRUE")


def fetch_sheet_data():
    """Fetch the Firm Tracker sheet data from Google Sheets via Gestalt.

    Returns a list of dicts (one per row) or raises on error.
    """
    api_key = os.environ.get("GESTALT_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "GESTALT_API_KEY is not set. "
            "Get a token at https://gestalt.peachstreet.dev/tokens"
        )

    url = (
        f"{GESTALT_BASE}/google_sheets/get_values"
        f"?spreadsheet_id={SPREADSHEET_ID}"
        f"&range={urllib.request.quote(SHEET_RANGE)}"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Google Sheets API error: {e.code} {e.reason}")

    rows = body.get("data", {}).get("values", [])
    if len(rows) < 2:
        raise RuntimeError("Sheet returned no data rows")

    # First row is the header; skip it
    data_rows = rows[1:]

    parsed = []
    for row in data_rows:
        firm_name = _get(row, COL["firm_name"]).strip()
        if not firm_name:
            continue

        parsed.append({
            "firm_name": firm_name,
            "vm_firm": _bool(_get(row, COL["vm_firm"])),
            "vm_active_fcl": _get(row, COL["vm_active_fcl"]).strip(),
            "vm_active_bk": _get(row, COL["vm_active_bk"]).strip(),
            "nrz_rank": _get(row, COL["nrz_rank"]).strip(),
            "loancare_rank": _get(row, COL["loancare_rank"]).strip(),
            "ocean_design_partner": _bool(_get(row, COL["ocean_design_partner"])),
            "ocean_m1": _bool(_get(row, COL["ocean_m1"])),
            "ocean_m2": _get(row, COL["ocean_m2"]).strip(),
            "ocean_m2_volume": _get(row, COL["ocean_m2_volume"]).strip(),
            "proposed_wave": _get(row, COL["proposed_wave"]).strip(),
            "live_training": _bool(_get(row, COL["live_training"])),
            "last_reachout": _get(row, COL["last_reachout"]).strip(),
            "phase0_meeting": _bool(_get(row, COL["phase0_meeting"])),
            "leadership_meeting": _bool(_get(row, COL["leadership_meeting"])),
            "design_meeting": _bool(_get(row, COL["design_meeting"])),
            "leadership_engagement": _get(row, COL["leadership_engagement"]).strip(),
            "wave_notes": _get(row, COL["wave_notes"]).strip(),
            "interaction": _get(row, COL["interaction"]).strip(),
            "ops_contact_email": _get(row, COL["ops_contact_email"]).strip(),
            "leadership_contact": _get(row, COL["leadership_contact"]).strip(),
            "leadership_title": _get(row, COL["leadership_title"]).strip(),
            "leadership_email": _get(row, COL["leadership_email"]).strip(),
        })

    return parsed


def sync_tracker_from_sheet(session):
    """Fetch the live sheet and upsert all rows into the FirmTracker table.

    Returns a summary dict with counts of created, updated, and skipped firms.
    """
    sheet_rows = fetch_sheet_data()

    # Build a lookup of indexed firms by name (case-insensitive)
    all_firms = session.query(IndexedFirm).all()
    firm_lookup = {f.name.upper(): f for f in all_firms}

    created = 0
    updated = 0
    skipped = []

    for row in sheet_rows:
        firm_name = row.pop("firm_name")
        firm = firm_lookup.get(firm_name.upper())

        if not firm:
            skipped.append(firm_name)
            continue

        tracker = session.query(FirmTracker).filter(
            FirmTracker.indexed_firm_id == firm.id
        ).first()

        if tracker:
            updated += 1
        else:
            tracker = FirmTracker(indexed_firm_id=firm.id)
            session.add(tracker)
            created += 1

        # Update all fields from the sheet
        for key, val in row.items():
            if hasattr(tracker, key):
                setattr(tracker, key, val)

    session.commit()

    return {
        "total_sheet_rows": len(sheet_rows),
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }
