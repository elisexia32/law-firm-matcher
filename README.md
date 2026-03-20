# Law Firm Matcher

A Streamlit app for matching, deduplicating, and tracking law firm onboarding across servicers.

## Features

- **🔍 Search & Match**: Search for a law firm to see if it already exists
- **📤 Batch Upload**: Upload a CSV/Excel of firms to classify them
- **🔄 Duplicate Detector**: Find potential duplicates in the canonical list
- **📋 Pending Firms Tracker**: Track firms queued for onboarding with client and phase info

## Setup

```bash
cd /Users/elise/Developer/law-firm-matcher

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Running the App

```bash
streamlit run app.py
```

The app will open at http://localhost:8501

## Data

### Canonical Firms
Upload a CSV export of `servicer_law_firms` table. The app looks for columns named:
- `name`, `firm_name`, `company_name`, `Name`, or `Company Name`

### Pending Firms
Stored locally in `data/pending_firms.json` with the following fields:
- `name`: Firm name
- `client`: Which servicer (Valon Mortgage, ServiceMac, NewRez, etc.)
- `phase`: Onboarding phase (Pilot, Wave 1, Wave 2, M1, M2, etc.)
- `status`: Current status (Pending Review, Approved, In Progress, Onboarded, etc.)
- `notes`: Free text notes

## Matching Algorithm

The app uses multiple fuzzy matching techniques:
1. **Name normalization**: Removes suffixes (LLP, PC, etc.), standardizes punctuation
2. **Token-based matching**: Compares key words in firm names
3. **Fuzzy string matching**: Uses Levenshtein distance variants
4. **Weighted scoring**: Combines multiple scores for overall confidence

### Thresholds
- **≥90%**: High confidence match (likely same firm)
- **70-89%**: Needs review (potential match)
- **<70%**: Likely new firm

## Project Structure

```
law-firm-matcher/
├── app.py              # Main Streamlit app
├── matching.py         # Matching/dedup logic
├── requirements.txt    # Python dependencies
├── README.md
└── data/
    ├── servicer_law_firms.csv   # Canonical firms (uploaded)
    └── pending_firms.json       # Pending tracker data
```
