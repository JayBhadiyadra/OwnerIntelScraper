"""
utils.py  ← FIX #2 (Indian name extractor) + FIX #4 (Indian phone extractor)
──────────────────────────────────────────────────────────────────────────────
Changes vs original:
  - extract_phones() now calls extract_phones_indian() as primary strategy
    for IN region, falling back to phonenumbers library
  - NEW extract_phones_indian(): handles Indian mobile formats robustly
    including 10-digit, +91, 091, 0XX-XXXXXXXX, spaces/dashes/dots between digits
  - NEW extract_indian_names(): handles ALL-CAPS, mixed-case, and
    Indian name patterns (Gujarati/Hindi names like RAHUL MEHTA, Rahul Mehta,
    rahul mehta, Mr. Bhavik Patel, etc.)
  - extract_names() retained for Western name fallback
  - All other helpers unchanged
"""

import re
import json
import phonenumbers
from phonenumbers import NumberParseException
from typing import List, Optional
from urllib.parse import urlparse
import unicodedata
import os


# ──────────────────────────────────────────────
# Phone extraction  ← FIX #4
# ──────────────────────────────────────────────

# Matches Indian mobile: 10 digits, optionally prefixed with +91 / 091 / 0
# Also handles separators: space, dash, dot between digit groups
INDIAN_MOBILE_PATTERN = re.compile(
    r"""
    (?:(?:\+|00)91[\s\-.]?)?   # optional +91 / 0091
    (?:0)?                      # optional leading 0
    (?:                         # 10-digit mobile starting with 6-9
        [6-9]\d{9}              # no separator
        |
        [6-9]\d{4}[\s\-\.]\d{5}  # XXXXX-XXXXX
        |
        [6-9]\d{2}[\s\-\.]\d{3}[\s\-\.]\d{4}  # XXX-XXX-XXXX
        |
        [6-9]\d{3}[\s\-\.]\d{3}[\s\-\.]\d{3}  # XXXX-XXX-XXX
    )
    """,
    re.VERBOSE,
)

# Landline pattern: STD code (2-4 digits) + 6-8 digit number
INDIAN_LANDLINE_PATTERN = re.compile(
    r"""
    (?:0\d{2,4})[\s\-\.]    # STD code starting with 0
    \d{6,8}                  # subscriber number
    """,
    re.VERBOSE,
)

PHONE_RAW_PATTERN = re.compile(
    r"""
    (?:(?:\+|00)\d{1,3}[\s\-.]?)?   # optional country code
    (?:\(?[\d]{1,4}\)?[\s\-.]?)?      # area code
    \d{3,4}[\s\-.]?\d{3,4}            # main number
    (?:[\s\-.]?\d{1,4})?              # extension
    """,
    re.VERBOSE,
)


def extract_phones_indian(text: str) -> List[str]:
    """
    FIX #4: Extract Indian phone numbers from text.
    Handles:
      - 10-digit mobiles (6xxx, 7xxx, 8xxx, 9xxx)
      - +91 / 0091 / 091 prefixed numbers
      - Various separator formats (space, dash, dot)
      - Landlines with STD codes
    Returns cleaned 10-digit strings (no country code prefix).
    """
    if not text:
        return []

    found = set()

    # Pass 1: phonenumbers library (most accurate for E164)
    try:
        for match in phonenumbers.PhoneNumberMatcher(text, "IN"):
            num = phonenumbers.format_number(
                match.number, phonenumbers.PhoneNumberFormat.E164
            )
            found.add(num)
    except Exception:
        pass

    # Pass 2: Indian mobile regex (catches formats phonenumbers lib misses)
    for raw_match in INDIAN_MOBILE_PATTERN.finditer(text):
        raw = raw_match.group(0).strip()
        # Strip separators and country code prefix
        digits_only = re.sub(r"[\s\-\.\(\)]", "", raw)
        digits_only = re.sub(r"^(?:\+?91|0091|091|0)(?=[6-9])", "", digits_only)
        if len(digits_only) == 10 and digits_only[0] in "6789":
            e164 = f"+91{digits_only}"
            found.add(e164)

    # Pass 3: Raw regex catch-all
    for raw in PHONE_RAW_PATTERN.findall(text):
        raw = raw.strip()
        digits = re.sub(r"\D", "", raw)
        if len(digits) < 10:
            continue
        try:
            parsed = phonenumbers.parse(raw, "IN")
            if phonenumbers.is_valid_number(parsed):
                formatted = phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
                found.add(formatted)
        except NumberParseException:
            pass

    # Filter fake/test numbers
    filtered = [
        p for p in found
        if not any(p.endswith(fake) for fake in ["0000000", "1234567", "9999999"])
    ]
    return sorted(filtered)


def extract_phones(text: str, default_region: str = "IN") -> List[str]:
    """
    Unified phone extractor. Uses Indian extractor for IN region,
    falls back to phonenumbers library for other regions.
    """
    region = (default_region or os.getenv("DEFAULT_PHONE_REGION") or "IN").upper()

    if region == "IN":
        return extract_phones_indian(text)

    # Non-India fallback (original logic)
    if not text:
        return []
    found = set()
    try:
        for match in phonenumbers.PhoneNumberMatcher(text, region):
            num = phonenumbers.format_number(
                match.number, phonenumbers.PhoneNumberFormat.E164
            )
            found.add(num)
    except Exception:
        pass

    for raw in PHONE_RAW_PATTERN.findall(text):
        raw = raw.strip()
        if len(re.sub(r"\D", "", raw)) < 7:
            continue
        try:
            parsed = phonenumbers.parse(raw, region)
            if phonenumbers.is_valid_number(parsed):
                formatted = phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
                found.add(formatted)
        except NumberParseException:
            pass

    filtered = [
        p for p in found
        if not any(p.endswith(fake) for fake in ["0000000", "1234567", "9999999"])
    ]
    return sorted(filtered)


# ──────────────────────────────────────────────
# Email extraction
# ──────────────────────────────────────────────

EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

IGNORED_EMAIL_DOMAINS = {
    "example.com", "test.com", "email.com", "noreply.com",
    "no-reply.com", "mailer.com", "sentry.io", "amazonaws.com",
}


def extract_emails(text: str) -> List[str]:
    """Extract valid-looking email addresses."""
    if not text:
        return []
    emails = EMAIL_PATTERN.findall(text)
    return [
        e.lower() for e in emails
        if e.split("@")[-1].lower() not in IGNORED_EMAIL_DOMAINS
    ]


# ──────────────────────────────────────────────
# URL helpers
# ──────────────────────────────────────────────

def extract_domain(url: str) -> str:
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    domain = re.sub(r"^www\.", "", domain)
    return domain


def normalize_url(url: str) -> str:
    if not url:
        return url
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(normalize_url(url))
        return bool(parsed.netloc)
    except Exception:
        return False


# ──────────────────────────────────────────────
# Text cleaning
# ──────────────────────────────────────────────

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_query(query: str) -> str:
    q = query.lower().strip()
    q = re.sub(r"https?://", "", q)
    q = re.sub(r"www\.", "", q)
    q = re.sub(r"[^\w\s]", " ", q)
    q = re.sub(r"\s+", " ", q)
    return q.strip()


# ──────────────────────────────────────────────
# Name detection helpers  ← FIX #2
# ──────────────────────────────────────────────

OWNER_ROLES = [
    "founder", "co-founder", "cofounder", "ceo", "chief executive",
    "owner", "director", "managing director", "md", "president",
    "chairman", "chairperson", "principal", "partner", "proprietor",
    "manager", "proprietress",
]

ROLE_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(r) for r in OWNER_ROLES) + r")\b",
    re.IGNORECASE,
)

# ── FIX #2: Indian name patterns ──────────────────────────────────────
# Handles: Title Case, ALL CAPS, mixed case, honorifics (Mr./Mrs./Dr./Shri)
# Indian names often: 2-3 parts, each 2-20 chars, no digits
# E.g.: Rahul Mehta, BHAVIK PATEL, shri ramesh kumar, Dr. Priya Shah

INDIAN_HONORIFIC = re.compile(
    r"\b(?:Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?|Shri|Smt\.?|Shrimati)\s+",
    re.IGNORECASE,
)

# Matches 2-3 word names — works on title case AND all-caps
INDIAN_NAME_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z]{1,19}(?:\s[A-Z][A-Za-z]{1,19}){1,2})\b"
)

# Specifically matches ALL-CAPS names from directories like Justdial
ALLCAPS_NAME_PATTERN = re.compile(
    r"\b([A-Z]{2,20}(?:\s[A-Z]{2,20}){1,2})\b"
)

# Common non-name words that match our patterns — filter these out
NAME_STOPWORDS = {
    "The", "This", "That", "For", "More", "Our", "Your", "His", "Her",
    "New", "Inc", "Ltd", "Llc", "Corp", "Company", "Group", "United",
    "States", "United Kingdom", "Private", "Limited", "Pvt", "Pvt Ltd",
    "India", "Gujarat", "Mumbai", "Delhi", "Bangalore", "Surat",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "June", "July", "August",
    "September", "October", "November", "December",
    "Contact", "Address", "Phone", "Mobile", "Email", "Website",
    "Open", "Close", "Hours", "Time", "Near", "Road", "Street",
}


def extract_indian_names(text: str) -> List[str]:
    """
    FIX #2: Extract Indian personal names from text.
    Handles Title Case, ALL CAPS, mixed case, and honorifics.
    Works with Justdial, Sulekha, and other Indian directory formats.
    """
    if not text:
        return []

    names = []

    # Step 1: Extract names following honorifics (highest confidence)
    for match in INDIAN_HONORIFIC.finditer(text):
        start = match.end()
        # Take next 2-3 words after the honorific
        after = text[start:start + 60]
        name_match = re.match(r"([A-Za-z]{2,20}(?:\s[A-Za-z]{2,20}){0,2})", after)
        if name_match:
            name = name_match.group(1).strip().title()
            if name not in NAME_STOPWORDS and len(name) > 4:
                names.append(name)

    # Step 2: "Contact: Name" / "Owner: Name" / "Proprietor: Name" patterns
    label_pattern = re.compile(
        r"(?:Contact|Owner|Proprietor|Proprietress|Manager|Partner)[:\s]+([A-Za-z\s]{3,40}?)(?:\s{2,}|\n|,|\d|$)",
        re.IGNORECASE,
    )
    for m in label_pattern.finditer(text):
        candidate = m.group(1).strip().title()
        words = candidate.split()
        if 2 <= len(words) <= 4 and candidate not in NAME_STOPWORDS:
            # Check each word is a plausible name part (no numbers, no stopwords)
            if all(w.isalpha() and len(w) >= 2 for w in words):
                names.append(candidate)

    # Step 3: Standard Title Case pattern (e.g., "Rahul Mehta")
    for match in INDIAN_NAME_PATTERN.finditer(text):
        candidate = match.group(1).strip()
        words = candidate.split()
        if 2 <= len(words) <= 3:
            if candidate not in NAME_STOPWORDS:
                # Each word must start uppercase + have lowercase (not ALL CAPS word)
                if all(w[0].isupper() and any(c.islower() for c in w) for w in words):
                    names.append(candidate)

    # Step 4: ALL CAPS pattern from Indian directories (e.g., "RAHUL MEHTA")
    # Convert to Title Case after matching
    for match in ALLCAPS_NAME_PATTERN.finditer(text):
        candidate = match.group(1).strip()
        words = candidate.split()
        if 2 <= len(words) <= 3:
            title_cased = candidate.title()
            # Filter out obvious non-names
            if title_cased not in NAME_STOPWORDS:
                # All-caps sequences of 2-20 chars per word = likely a name in Justdial
                if all(2 <= len(w) <= 20 for w in words):
                    names.append(title_cased)

    # Deduplicate preserving order
    seen = set()
    unique = []
    for n in names:
        key = n.lower()
        if key not in seen:
            seen.add(key)
            unique.append(n)

    return unique


def extract_names(text: str) -> List[str]:
    """
    Original Western name extractor — kept for non-India sources.
    Use extract_indian_names() for Indian directories.
    """
    if not text:
        return []
    pattern = re.compile(r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})\b")
    names = pattern.findall(text)
    stop = {
        "The", "This", "That", "For", "More", "Our", "Your", "His", "Her",
        "New", "Inc", "Ltd", "Llc", "Corp", "Company", "Group",
        "United", "States", "United Kingdom",
    }
    return [n for n in names if n not in stop and len(n.split()) >= 2]


# ──────────────────────────────────────────────
# Role detection
# ──────────────────────────────────────────────

def detect_role(text: str) -> Optional[str]:
    m = ROLE_PATTERN.search(text)
    return m.group(0).title() if m else None


# ──────────────────────────────────────────────
# Confidence scoring
# ──────────────────────────────────────────────

def compute_confidence(
    has_name: bool,
    has_phone: bool,
    has_email: bool,
    has_role: bool,
    source_tier: int,
) -> float:
    score = 0.0
    if has_name:
        score += 0.25
    if has_phone:
        score += 0.35
    if has_email:
        score += 0.15
    if has_role:
        score += 0.15
    tier_bonus = {1: 0.10, 2: 0.05, 3: 0.0}
    score += tier_bonus.get(source_tier, 0)
    return round(min(score, 1.0), 2)


# ──────────────────────────────────────────────
# JSON helpers
# ──────────────────────────────────────────────

def phones_to_json(phones: List[str]) -> str:
    return json.dumps(phones)


def phones_from_json(s: str) -> List[str]:
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


# ──────────────────────────────────────────────
# Context helpers
# ──────────────────────────────────────────────

def is_likely_business_phone_context(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    keywords = [
        "contact", "call", "phone", "telephone", "tel:", "mob", "mobile", "whatsapp",
        "reception", "front desk", "office", "support", "sales", "enquiry", "inquiry",
        "hours", "open", "address", "directions",
    ]
    return any(k in t for k in keywords)