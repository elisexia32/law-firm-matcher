"""
Law Firm Name Matching Logic
"""
import re
from rapidfuzz import fuzz, process
from typing import Optional

# Common suffixes to remove for normalization
SUFFIXES = [
    r',?\s*LLP\.?$',
    r',?\s*LLC\.?$',
    r',?\s*PLLC\.?$',
    r',?\s*PLC\.?$',
    r',?\s*P\.?L\.?L\.?C\.?$',
    r',?\s*P\.?C\.?$',
    r',?\s*P\.?A\.?$',
    r',?\s*P\.?L\.?C\.?$',
    r',?\s*L\.?P\.?A\.?$',
    r',?\s*L\.?L\.?P\.?$',
    r',?\s*L\.?L\.?C\.?$',
    r',?\s*Inc\.?$',
    r',?\s*Corp\.?$',
    r',?\s*Co\.?$',
]

# Common words to normalize
COMMON_WORDS = [
    (r'\bLaw\s+Firm\b', ''),
    (r'\bLaw\s+Group\b', ''),
    (r'\bLaw\s+Offices?\s+of\b', ''),
    (r'\bAttorneys?\s+at\s+Law\b', ''),
    (r'\b&\s+Associates\b', ''),
    (r'\bAssociates\b', 'ASSOC'),
    (r'\bThe\b', ''),
    (r'\band\b', '&'),
    (r'\bAND\b', '&'),
]


def normalize_name(name: str) -> str:
    """
    Normalize a law firm name for comparison.
    - Uppercase
    - Remove suffixes (LLP, PC, etc.)
    - Standardize common words
    - Remove extra whitespace and punctuation
    """
    if not name:
        return ""

    # Uppercase
    normalized = name.upper().strip()

    # Remove suffixes
    for suffix in SUFFIXES:
        normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)

    # Standardize common words
    for pattern, replacement in COMMON_WORDS:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    # Remove punctuation except &
    normalized = re.sub(r'[^\w\s&]', ' ', normalized)

    # Collapse whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized


def extract_key_tokens(name: str) -> set[str]:
    """Extract the key identifying tokens from a firm name."""
    normalized = normalize_name(name)
    # Split on spaces and &
    tokens = re.split(r'[\s&]+', normalized)
    # Filter out very short tokens and common words
    stopwords = {'THE', 'OF', 'AND', 'A', 'AN', 'IN', 'AT', 'FOR', 'BY', 'ASSOC'}
    return {t for t in tokens if len(t) > 2 and t not in stopwords}


def calculate_match_score(name1: str, name2: str) -> dict:
    """
    Calculate multiple similarity scores between two firm names.
    Returns a dict with different scoring methods and an overall score.
    """
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    # Exact match after normalization
    if norm1 == norm2:
        return {
            'overall': 100,
            'normalized_exact': True,
            'fuzzy_ratio': 100,
            'token_sort': 100,
            'token_set': 100,
            'partial': 100,
        }

    # Various fuzzy matching scores
    fuzzy_ratio = fuzz.ratio(norm1, norm2)
    token_sort = fuzz.token_sort_ratio(norm1, norm2)
    token_set = fuzz.token_set_ratio(norm1, norm2)
    partial = fuzz.partial_ratio(norm1, norm2)

    # Token overlap check
    tokens1 = extract_key_tokens(name1)
    tokens2 = extract_key_tokens(name2)
    token_overlap = len(tokens1 & tokens2) / max(len(tokens1 | tokens2), 1) * 100

    # Weighted overall score
    overall = (
        fuzzy_ratio * 0.2 +
        token_sort * 0.25 +
        token_set * 0.3 +
        partial * 0.1 +
        token_overlap * 0.15
    )

    return {
        'overall': round(overall, 1),
        'normalized_exact': False,
        'fuzzy_ratio': fuzzy_ratio,
        'token_sort': token_sort,
        'token_set': token_set,
        'partial': partial,
        'token_overlap': round(token_overlap, 1),
    }


def find_matches(
    query: str,
    candidates: list[str],
    threshold: float = 70.0,
    limit: int = 10
) -> list[dict]:
    """
    Find matching firms from a list of candidates.
    Returns sorted list of matches above threshold.
    """
    if not query or not candidates:
        return []

    matches = []
    for candidate in candidates:
        if not candidate:
            continue
        scores = calculate_match_score(query, candidate)
        if scores['overall'] >= threshold:
            matches.append({
                'candidate': candidate,
                'normalized': normalize_name(candidate),
                **scores
            })

    # Sort by overall score descending
    matches.sort(key=lambda x: x['overall'], reverse=True)
    return matches[:limit]


def find_duplicates_in_list(firms: list[str], threshold: float = 85.0) -> list[dict]:
    """
    Find potential duplicates within a list of firms.
    Returns pairs of firms that appear to be duplicates.
    """
    duplicates = []
    seen_pairs = set()

    for i, firm1 in enumerate(firms):
        if not firm1:
            continue
        for j, firm2 in enumerate(firms):
            if i >= j or not firm2:
                continue

            # Skip if we've seen this pair
            pair_key = tuple(sorted([firm1, firm2]))
            if pair_key in seen_pairs:
                continue

            scores = calculate_match_score(firm1, firm2)
            if scores['overall'] >= threshold:
                seen_pairs.add(pair_key)
                duplicates.append({
                    'firm1': firm1,
                    'firm2': firm2,
                    'normalized1': normalize_name(firm1),
                    'normalized2': normalize_name(firm2),
                    **scores
                })

    # Sort by score descending
    duplicates.sort(key=lambda x: x['overall'], reverse=True)
    return duplicates


def classify_firms(
    new_firms: list[str],
    existing_firms: list[str],
    high_threshold: float = 90.0,
    medium_threshold: float = 70.0
) -> dict:
    """
    Classify a list of new firms against existing firms.

    Returns:
        - existing: high-confidence matches (likely same firm)
        - review: medium-confidence matches (needs human review)
        - new: no matches found (likely new firm)
    """
    results = {
        'existing': [],
        'review': [],
        'new': []
    }

    for firm in new_firms:
        if not firm:
            continue

        matches = find_matches(firm, existing_firms, threshold=medium_threshold)

        if not matches:
            results['new'].append({
                'firm': firm,
                'normalized': normalize_name(firm),
                'matches': []
            })
        elif matches[0]['overall'] >= high_threshold:
            results['existing'].append({
                'firm': firm,
                'normalized': normalize_name(firm),
                'best_match': matches[0]['candidate'],
                'score': matches[0]['overall'],
                'all_matches': matches
            })
        else:
            results['review'].append({
                'firm': firm,
                'normalized': normalize_name(firm),
                'best_match': matches[0]['candidate'],
                'score': matches[0]['overall'],
                'all_matches': matches
            })

    return results
