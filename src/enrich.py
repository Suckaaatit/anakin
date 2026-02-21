"""
enrich.py — Data enrichment module for TechSparks GTM Automation.

Handles seniority normalization, LinkedIn URL discovery, industry classification,
news signal fetching, and confidence scoring.
"""

import html
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import requests
import json5
from openai import OpenAI
from duckduckgo_search import DDGS
from llm_runtime import (
    backoff_delay,
    create_llm_client,
    env_flag,
    get_llm_model,
    get_llm_rate_limiter,
    is_rate_limited_error,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/errors.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Encoding": "identity",
}

MISSING_LINKEDIN_VALUES = {
    "NOT_FOUND",
    "ERROR",
    "",
    "SKIP_INVALID_NAME",
    "NOT_ATTEMPTED_FAST_MODE",
    "NOT_AVAILABLE",
    "NOT_PUBLICLY_AVAILABLE",
}


def normalize_seniority(title: str) -> str:
    """
    Normalize job title to seniority level.
    
    Keyword matching against ordered list. Returns first match.
    Default: "Unclassified → Manual Review"
    
    Args:
        title: Job title string.
        
    Returns:
        Normalized seniority level.
    """
    title_lower = f" {title.lower()} "
    
    # Rule 1: Founder
    founder_keywords = ["founder", "co-founder", "co founder"]
    for kw in founder_keywords:
        if kw in title_lower:
            return "C-Suite / Founder"
    
    # Rule 2: C-Suite
    c_suite_keywords = [
        "chief executive", " ceo", "chief operating", " coo",
        "chief technology", " cto", "chief financial", " cfo",
        "chief marketing", " cmo", "chairman", "president",
        "managing director", "managing partner"
    ]
    for kw in c_suite_keywords:
        if kw in title_lower:
            return "C-Suite / Founder"
    
    # Rule 3: VP
    vp_keywords = ["vice president", " vp ", "svp", "evp", "avp"]
    for kw in vp_keywords:
        if kw in title_lower:
            return "VP"
    
    # Rule 4: Director / Head
    director_keywords = ["director", "head of", "head,", " head ", "head-"]
    for kw in director_keywords:
        if kw in title_lower:
            return "Director / Head"
    
    # Rule 5: Senior IC / Manager
    senior_ic_keywords = ["senior manager", "senior lead", "principal", "senior engineer", "senior analyst"]
    for kw in senior_ic_keywords:
        if kw in title_lower:
            return "Senior IC / Manager"
    
    # Rule 6: Manager
    manager_keywords = ["manager", " lead ", "team lead"]
    for kw in manager_keywords:
        if kw in title_lower:
            return "Manager"
    
    # Rule 7: IC
    ic_keywords = ["engineer", "analyst", "associate", "executive", "consultant"]
    for kw in ic_keywords:
        if kw in title_lower:
            return "IC"
    
    # Rule 8: Junior / Intern
    junior_keywords = ["intern", "trainee", "fresher"]
    for kw in junior_keywords:
        if kw in title_lower:
            return "Junior / Intern"
    
    return "Unclassified → Manual Review"


def normalize_company_name(company: str) -> str:
    """
    Canonical company name for deduplication (R3 fix).
    
    Strip legal suffixes, lowercase and strip whitespace.
    
    Args:
        company: Company name string.
        
    Returns:
        Canonical company name.
    """
    suffixes = [" Pvt Ltd", " Private Limited", " Ltd", " Limited", " Inc", " LLC", " LLP"]
    normalized = company
    for suffix in suffixes:
        normalized = normalized.replace(suffix, "")
        normalized = normalized.replace(suffix.lower(), "")
    return normalized.lower().strip()


def strip_html(value: str) -> str:
    """Normalize html fragments to plain text."""
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Extract first JSON object from mixed model output.
    Supports strict JSON and json5 fallback.
    """
    content = (text or "").strip().replace("```json", "").replace("```", "").strip()
    if not content:
        return None

    candidates = [content]
    start = content.find("{")
    end = content.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidates.append(content[start : end + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        try:
            parsed = json5.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    return None


def industry_relevance_score(industry: str) -> int:
    """Map industry to outreach relevance score (0-100)."""
    industry = (industry or "").strip()
    if industry in {"fintech", "saas_b2b", "ecommerce_d2c", "payments"}:
        return 95
    if industry in {"venture_capital", "marketplace"}:
        return 82
    if industry in {"ai_tech", "logistics", "foodtech", "mobility_ev", "deep_tech", "healthtech"}:
        return 68
    if industry in {"hrtech", "proptech", "agritech"}:
        return 52
    return 35


def classify_industry_keyword(company: str) -> Optional[str]:
    """
    Classify industry based on keyword matching.
    
    Args:
        company: Company name string.
        
    Returns:
        Industry bucket or None if no match.
    """
    company_lower = company.lower()
    
    industry_map = {
        "fintech": ["zerodha", "razorpay", "phonepe", "paytm", "open financial", "pine labs", "cred", "axio", "coindc"],
        "ecommerce_d2c": ["nykaa", "sugar cosmetics", "mamaearth", "lenskart", "meesho", "snapdeal", "mensa brands", "beer cafe"],
        "foodtech": ["zomato", "swiggy"],
        "hrtech": ["teamlease"],
        "mobility_ev": ["ather energy", "ola", "park plus"],
        "saas_b2b": ["freshworks", "haptik", "fusioncharts", "infosys"],
        "healthtech": ["khosla labs"],
        "agritech": ["dehaat"],
        "venture_capital": ["kalaari", "peak xv", "blume", "antler", "3one4", "multiples", "info edge", "titan capital", "v3 ventures"],
        "deep_tech": ["isro"],
        "proptech": ["oyo"],
        "payments": ["pine labs", "razorpay", "phonepe", "paytm"],
        "ai_tech": ["haptik", "sarvam", "krutrim"],
        "marketplace": ["meesho", "udaan", "shaadi"],
        "logistics": ["delhivery", "shiprocket", "ecom express"]
    }
    
    for industry, keywords in industry_map.items():
        for kw in keywords:
            if kw in company_lower:
                return industry
    return None


def classify_industry_llm(
    company: str,
    title: str,
    client: OpenAI,
    model: str,
    retries: int = 2,
) -> str:
    """
    Classify industry using LLM.
    
    Args:
        company: Company name.
        title: Job title.
        client: OpenAI client instance.
        model: LLM model name.
        
    Returns:
        Industry bucket name.
    """
    valid_buckets = [
        "fintech", "ecommerce_d2c", "foodtech", "hrtech", "mobility_ev",
        "saas_b2b", "healthtech", "agritech", "venture_capital", "deep_tech",
        "proptech", "ai_tech", "marketplace", "logistics", "other_tech"
    ]
    
    prompt = f"""Classify this company into one bucket:
Company: {company}
Title: {title}

Buckets: fintech | ecommerce_d2c | foodtech | hrtech | mobility_ev | saas_b2b | healthtech | agritech | venture_capital | deep_tech | proptech | ai_tech | marketplace | logistics | other_tech

Return ONLY the bucket name."""
    
    limiter = get_llm_rate_limiter()
    for attempt in range(retries + 1):
        try:
            with limiter.slot():
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=20
                )
            result = response.choices[0].message.content.strip().lower()
            if result in valid_buckets:
                return result
            return "other_tech"
        except Exception as e:
            if attempt < retries:
                delay = backoff_delay(attempt, e)
                if is_rate_limited_error(e):
                    logger.warning(
                        "Industry LLM rate-limited on attempt %s; retrying in %.2fs",
                        attempt + 1,
                        delay,
                    )
                else:
                    logger.warning(
                        "Industry LLM classification failed on attempt %s; retrying in %.2fs (%s)",
                        attempt + 1,
                        delay,
                        e,
                    )
                time.sleep(delay)
                continue
            logger.warning(f"Industry LLM classification failed: {e}")
            return "other_tech"
    return "other_tech"


def load_linkedin_cache() -> Dict[str, Any]:
    """Load LinkedIn cache from JSON file."""
    cache_path = Path("data/cache/linkedin_cache.json")
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_linkedin_cache(cache: Dict[str, Any]) -> None:
    """Save LinkedIn cache to JSON file."""
    cache_path = Path("data/cache/linkedin_cache.json")
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def clean_linkedin_url(url: str) -> str:
    """Normalize LinkedIn profile URL for caching/dedup."""
    if not url:
        return ""
    url = url.strip().split("?")[0].split("#")[0]
    if url.startswith("http://"):
        url = "https://" + url[len("http://") :]
    if not url.startswith("https://"):
        url = "https://" + url
    return url.rstrip("/")


def normalize_person_name(name: str) -> str:
    """Normalize person name for deterministic matching."""
    lowered = str(name or "").lower()
    normalized = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    return " ".join(normalized.split())


def load_local_linkedin_seed_map() -> Dict[str, Dict[str, str]]:
    """
    Build deterministic LinkedIn mappings from local public HTML snapshots.

    Returns:
        Mapping: normalized_name -> {url, confidence, source}.
    """
    seed_files = [
        Path("data/techsparks_2025.html"),
        Path("data/techsparks_2024.html"),
        Path("data/sample_speaker.html"),
        Path("data/sample_speaker2024.html"),
        Path("data/sample_speakersb.html"),
    ]
    seed_map: Dict[str, Dict[str, str]] = {}

    for path in seed_files:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            logger.warning(f"Unable to read LinkedIn seed source {path}: {exc}")
            continue

        chunks = text.split('class="ts_speaker w-dyn-item"')
        if len(chunks) <= 1:
            chunks = [text]

        iterable_chunks = chunks[1:] if len(chunks) > 1 else chunks
        for chunk in iterable_chunks:
            snippet = chunk[:3500]
            title_marker = 'class="ts_speaker-title">'
            title_idx = snippet.find(title_marker)
            if title_idx == -1:
                continue
            title_start = title_idx + len(title_marker)
            title_end = snippet.find("</div>", title_start)
            if title_end == -1:
                continue
            raw_name = html.unescape(snippet[title_start:title_end])
            norm_name = normalize_person_name(raw_name)
            if not norm_name:
                continue

            link_match = re.search(
                r'<a href="([^"]*linkedin\.com/in/[^"]+)"[^>]*class="ts_speaker-overlay',
                snippet,
                flags=re.IGNORECASE,
            )
            if not link_match:
                # Fallback to any profile URL if the class selector differs.
                link_match = re.search(
                    r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/[^\s"\'<>]+',
                    snippet,
                    flags=re.IGNORECASE,
                )
            if not link_match:
                continue

            url = clean_linkedin_url(html.unescape(link_match.group(1)))
            if not url or "linkedin.com/in/" not in url.lower():
                continue

            seed_map[norm_name] = {
                "url": url,
                "confidence": "MEDIUM",
                "source": f"LOCAL_PUBLIC_HTML:{path.name}",
            }

    return seed_map


def resolve_local_linkedin_seed(
    name: str,
    seed_map: Dict[str, Dict[str, str]],
) -> Tuple[str, str, str]:
    """Resolve LinkedIn profile from local seed map."""
    key = normalize_person_name(name)
    if not key:
        return "NOT_FOUND", "NOT_FOUND", "NONE"
    match = seed_map.get(key)
    if not match:
        return "NOT_FOUND", "NOT_FOUND", "NONE"
    return (
        str(match.get("url", "NOT_FOUND")),
        str(match.get("confidence", "MEDIUM")),
        str(match.get("source", "LOCAL_PUBLIC_HTML")),
    )


def score_linkedin_candidate(
    url: str,
    context: str,
    first_name: str,
    last_name: str,
    canonical_company: str,
    title_words: list[str],
) -> int:
    """Heuristic score for selecting best LinkedIn profile candidate."""
    score = 0
    profile_slug = url.rsplit("/in/", 1)[-1].lower() if "/in/" in url.lower() else ""
    context = context.lower()

    if first_name and first_name in profile_slug:
        score += 2
    if last_name and last_name in profile_slug:
        score += 3
    if first_name and first_name in context:
        score += 1
    if last_name and last_name in context:
        score += 2
    if canonical_company and canonical_company in context:
        score += 2
    if any(w in context for w in title_words):
        score += 1
    if "/in/" in url.lower():
        score += 1
    return score


def _best_linkedin_candidate_from_html(
    html_text: str,
    name: str,
    company: str,
    title: str,
) -> Tuple[str, str]:
    """Extract and score LinkedIn profile candidates from search HTML."""
    first_name = name.split()[0].lower() if name.split() else ""
    last_name = name.split()[-1].lower() if len(name.split()) > 1 else ""
    canonical_company = normalize_company_name(company)
    title_words = [w for w in title.lower().split() if len(w) > 4]

    candidates: Dict[str, int] = {}
    pattern = re.compile(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/[^\s\"'<>]+", re.IGNORECASE)
    for match in pattern.finditer(html_text):
        url = clean_linkedin_url(match.group(0))
        if not url:
            continue
        start = max(0, match.start() - 280)
        end = min(len(html_text), match.end() + 280)
        context = strip_html(html_text[start:end])
        score = score_linkedin_candidate(
            url=url,
            context=context,
            first_name=first_name,
            last_name=last_name,
            canonical_company=canonical_company,
            title_words=title_words,
        )
        candidates[url] = max(candidates.get(url, 0), score)

    if not candidates:
        return "NOT_FOUND", "NOT_FOUND"

    best_url, best_score = sorted(candidates.items(), key=lambda item: item[1], reverse=True)[0]
    if best_score >= 7:
        return best_url, "HIGH"
    if best_score >= 4:
        return best_url, "MEDIUM"
    return "NOT_FOUND", "NOT_FOUND"


def find_linkedin_via_brave(name: str, company: str, title: str) -> Tuple[str, str]:
    """
    Find LinkedIn URL by parsing Brave search results HTML.
    Returns (url, confidence) or (NOT_FOUND, NOT_FOUND).
    """
    query = f'site:linkedin.com/in "{name}" "{company}" {title}'
    search_url = f"https://search.brave.com/search?q={quote_plus(query)}"

    html_text = ""
    max_retries = max(1, int(os.getenv("BRAVE_MAX_RETRIES", "4")))
    base_backoff = max(0.2, float(os.getenv("BRAVE_RETRY_BACKOFF_SEC", "1.2")))

    for attempt in range(max_retries):
        try:
            response = requests.get(search_url, headers=HTTP_HEADERS, timeout=12)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "").strip()
                if retry_after.isdigit():
                    delay = float(retry_after)
                else:
                    delay = base_backoff * (2 ** attempt)
                delay = min(delay, 20.0)
                logger.warning(f"Brave rate limited (429), retrying in {delay:.1f}s")
                time.sleep(delay)
                continue

            response.raise_for_status()
            html_text = response.text
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logger.warning(f"Brave LinkedIn search error: {e}")
            else:
                time.sleep(base_backoff * (2 ** attempt))

    if not html_text:
        return "NOT_FOUND", "NOT_FOUND"

    return _best_linkedin_candidate_from_html(html_text, name=name, company=company, title=title)


def find_linkedin_via_bing(name: str, company: str, title: str) -> Tuple[str, str]:
    """
    Find LinkedIn URL by parsing Bing web search results HTML.
    Returns (url, confidence) or (NOT_FOUND, NOT_FOUND).
    """
    query = f'site:linkedin.com/in "{name}" "{company}" {title}'
    search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
    max_retries = max(1, int(os.getenv("BING_MAX_RETRIES", "3")))
    base_backoff = max(0.2, float(os.getenv("BING_RETRY_BACKOFF_SEC", "0.8")))

    html_text = ""
    for attempt in range(max_retries):
        try:
            response = requests.get(search_url, headers=HTTP_HEADERS, timeout=10)
            response.raise_for_status()
            html_text = response.text
            break
        except Exception as exc:
            if attempt == max_retries - 1:
                logger.warning(f"Bing LinkedIn search error: {exc}")
            else:
                time.sleep(base_backoff * (2 ** attempt))

    if not html_text:
        return "NOT_FOUND", "NOT_FOUND"
    return _best_linkedin_candidate_from_html(html_text, name=name, company=company, title=title)


def find_linkedin_via_ddg(
    name: str,
    company: str,
    title: str,
    base_delay: float = 1.5,
) -> Tuple[str, str]:
    """Legacy DDG lookup kept as fallback."""
    query_strict = f'site:linkedin.com/in "{name}" "{company}"'
    query_loose = f"site:linkedin.com/in {name} {company} linkedin"

    first_name = name.split()[0].lower()
    canonical_company = normalize_company_name(company)
    title_words = [w for w in title.lower().split() if len(w) > 4]

    for attempt in range(2):
        try:
            time.sleep(base_delay * (2 ** attempt))
            with DDGS(timeout=8) as ddgs:
                results = list(ddgs.text(query_strict, max_results=5))
                if not results:
                    results = list(ddgs.text(query_loose, max_results=5))

            for result in results:
                url = clean_linkedin_url(result.get("href", ""))
                if "linkedin.com/in/" not in url.lower():
                    continue
                snippet = (result.get("title", "") + " " + result.get("body", "")).lower()
                first_name_match = first_name in snippet
                company_match = canonical_company in snippet
                title_match = any(w in snippet for w in title_words)
                if first_name_match and company_match and title_match:
                    return url, "HIGH"
                if first_name_match and (company_match or title_match):
                    return url, "MEDIUM"
        except Exception as e:
            logger.warning(f"DDG LinkedIn search error: {e}")

    return "NOT_FOUND", "NOT_FOUND"


def find_linkedin_url(
    name: str,
    company: str,
    title: str,
    cache: Dict[str, Any],
    base_delay: float = 1.5,
    persist_cache: bool = True,
) -> Tuple[str, str]:
    """
    Find LinkedIn URL for a person with cache + multi-backend lookup.
    """
    cache_key = f"{name.lower()}|{company.lower()}"
    if cache_key in cache:
        return cache[cache_key]["url"], cache[cache_key]["confidence"]

    preferred_backend = os.getenv("LINKEDIN_SEARCH_BACKEND", "brave").strip().lower()
    if preferred_backend == "ddg":
        lookup_order = ["ddg", "brave", "bing"]
    elif preferred_backend == "bing":
        lookup_order = ["bing", "brave"]
    else:
        lookup_order = ["brave", "bing"]
        if os.getenv("ENABLE_DDG_LINKEDIN_FALLBACK", "0") == "1":
            lookup_order.append("ddg")

    for backend in lookup_order:
        if backend == "brave":
            url, confidence = find_linkedin_via_brave(name, company, title)
        elif backend == "bing":
            url, confidence = find_linkedin_via_bing(name, company, title)
        else:
            url, confidence = find_linkedin_via_ddg(name, company, title, base_delay=base_delay)

        if confidence in {"HIGH", "MEDIUM"} and url != "NOT_FOUND":
            cache[cache_key] = {"url": url, "confidence": confidence}
            if persist_cache:
                save_linkedin_cache(cache)
            return url, confidence

    cache[cache_key] = {"url": "NOT_FOUND", "confidence": "NOT_FOUND"}
    if persist_cache:
        save_linkedin_cache(cache)
    return "NOT_FOUND", "NOT_FOUND"


def load_news_cache() -> Dict[str, str]:
    """Load news cache from JSON file."""
    cache_path = Path("data/cache/news_cache.json")
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_news_cache(cache: Dict[str, str]) -> None:
    """Save news cache to JSON file."""
    cache_path = Path("data/cache/news_cache.json")
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def fetch_news_signal(
    company: str,
    news_cache: Dict[str, str],
    base_delay: float = 1.0,
    persist_cache: bool = True,
) -> str:
    """
    Fetch lightweight news signal for a company.
    Primary path uses Bing RSS (faster/more stable than DDG in this environment).
    """
    cache_key = company.lower().strip()
    if cache_key in news_cache:
        return news_cache[cache_key]

    query = f"{company} funding OR partnership OR launch"
    rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"

    for attempt in range(2):
        try:
            time.sleep(base_delay * (2 ** attempt))
            response = requests.get(rss_url, headers=HTTP_HEADERS, timeout=10)
            response.raise_for_status()
            body = response.text
            titles = re.findall(r"<item>.*?<title>(.*?)</title>", body, flags=re.S | re.I)
            if titles:
                title = strip_html(titles[0])
                news_cache[cache_key] = title
                if persist_cache:
                    save_news_cache(news_cache)
                return title
        except Exception as e:
            logger.warning(f"Bing RSS news lookup failed: {e}")

    # Optional DDG fallback for users who want additional recall.
    if os.getenv("ENABLE_DDG_NEWS_FALLBACK", "0") == "1":
        try:
            with DDGS(timeout=8) as ddgs:
                results = list(ddgs.news(f"{company} 2025", max_results=1))
                if results:
                    title = strip_html(results[0].get("title", ""))
                    news_cache[cache_key] = title
                    if persist_cache:
                        save_news_cache(news_cache)
                    return title
        except Exception as e:
            logger.warning(f"DDG news fallback failed: {e}")

    news_cache[cache_key] = ""
    if persist_cache:
        save_news_cache(news_cache)
    return ""


def infer_previous_role_llm(
    name: str, 
    title: str, 
    company: str, 
    client: OpenAI, 
    model: str
) -> str:
    """
    Infer previous role using LLM.
    
    Args:
        name: Person's name.
        title: Current title.
        company: Current company.
        client: OpenAI client instance.
        model: LLM model name.
        
    Returns:
        Previous role string or NOT_AVAILABLE.
    """
    system_prompt = """You infer career history from public professional context.
Return ONLY JSON: {"previous_role": string, "confidence": "HIGH|MEDIUM|LOW"}.
Mark HIGH only for very well-known public figures.
Return NOT_AVAILABLE if uncertain. NEVER invent roles."""
    
    user_prompt = f"""Person: {name}
Current: {title} at {company}
Most likely previous significant role?"""
    
    limiter = get_llm_rate_limiter()
    retries = max(0, int(os.getenv("PREVIOUS_ROLE_LLM_RETRIES", "2")))
    for attempt in range(retries + 1):
        try:
            with limiter.slot():
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.1,
                    max_tokens=80
                )
            content = response.choices[0].message.content.strip()
            parsed = extract_first_json_object(content)
            if not parsed:
                raise ValueError("Could not parse JSON from previous-role response")

            confidence = str(parsed.get("confidence", "LOW")).upper()
            if confidence == "LOW":
                return "NOT_AVAILABLE"

            previous_role = strip_html(str(parsed.get("previous_role", "NOT_AVAILABLE")))
            if previous_role and previous_role != "NOT_AVAILABLE":
                return f"INFERRED: {previous_role}"
            return "NOT_AVAILABLE"
        except Exception as e:
            if attempt < retries:
                delay = backoff_delay(attempt, e)
                if is_rate_limited_error(e):
                    logger.warning(
                        "Previous-role LLM rate-limited on attempt %s; retrying in %.2fs",
                        attempt + 1,
                        delay,
                    )
                else:
                    logger.warning(
                        "Previous-role inference failed on attempt %s; retrying in %.2fs (%s)",
                        attempt + 1,
                        delay,
                        e,
                    )
                time.sleep(delay)
                continue
            logger.warning(f"Previous role inference failed: {e}")
            return "NOT_AVAILABLE"
    return "NOT_AVAILABLE"


def infer_email_pattern(name: str, company: str) -> Tuple[str, str]:
    """
    Generate likely email address pattern and confidence score (R9 fix).
    
    Args:
        name: Person's full name.
        company: Company name.
        
    Returns:
        Tuple of (email_pattern, confidence).
    """
    name_parts = name.strip().split()
    if len(name_parts) < 2:
        return "NOT_AVAILABLE", "NOT_AVAILABLE"
    
    first = name_parts[0].lower()
    last = name_parts[-1].lower()
    
    # Exception list for domains
    domain_exceptions = {
        "zerodha": "zerodha.com",
        "razorpay": "razorpay.com",
        "phonepe": "phonepe.com",
        "paytm": "paytm.com",
        "zomato": "zomato.com",
        "freshworks": "freshworks.com",
        "swiggy": "swiggy.com",
        "nykaa": "nykaa.com",
        "meesho": "meesho.com"
    }
    
    canonical = normalize_company_name(company)
    if canonical in domain_exceptions:
        domain = domain_exceptions[canonical]
    else:
        domain = canonical.replace(" ", "")
        domain = re.sub(r"[^a-z0-9.-]", "", domain)
        domain = domain.strip(".-")
        if domain.endswith(".com"):
            pass
        elif "." in domain:
            pass
        else:
            domain = f"{domain}.com"
    
    pattern = f"{first}.{last}@{domain}"
    return pattern, "GUESSED"


def calculate_evidence_score(row: Dict[str, Any]) -> int:
    """
    Calculate deterministic evidence quality score (0-100).

    This score intentionally emphasizes non-LLM, verifiable signals.
    """
    score = 0

    linkedin = str(row.get("linkedin_url", "") or "").strip()
    linkedin_conf = str(row.get("linkedin_confidence", "") or "").strip().upper()
    if linkedin and linkedin not in MISSING_LINKEDIN_VALUES:
        if linkedin_conf == "HIGH":
            score += 35
        elif linkedin_conf == "MEDIUM":
            score += 28
        else:
            score += 18

    industry_source = str(row.get("industry_source", "") or "").strip().upper()
    if industry_source == "KEYWORD_MAP":
        score += 18
    elif industry_source == "LLM_CLASSIFIER":
        score += 10

    if str(row.get("seniority", "")).strip() and str(row.get("seniority", "")).strip() != "Unclassified → Manual Review":
        score += 12

    if str(row.get("session_topic", "")).strip():
        score += 8

    if str(row.get("news_signal", "")).strip():
        score += 8

    if str(row.get("email_pattern", "")).strip() not in {"", "NOT_AVAILABLE", "ERROR"}:
        score += 6

    if str(row.get("previous_role_inferred", "")).startswith("INFERRED:"):
        score += 8

    if int(row.get("industry_relevance_score", 0) or 0) >= 60:
        score += 8

    return max(0, min(100, int(score)))


def calculate_confidence_score(row: Dict[str, Any]) -> int:
    """
    Calculate enrichment confidence score (0-5).
    
    Args:
        row: Enriched contact row.
        
    Returns:
        Confidence score (0-5).
    """
    score = 0
    
    # +2 if linkedin_url found
    linkedin = str(row.get("linkedin_url", "") or "")
    if linkedin and linkedin not in MISSING_LINKEDIN_VALUES:
        score += 2
    
    # +1 if linkedin_confidence is HIGH
    if str(row.get("linkedin_confidence", "")).upper() == "HIGH":
        score += 1
    
    # +1 if seniority classified
    if row.get("seniority") != "Unclassified → Manual Review":
        score += 1
    
    # +1 if any signal present
    if str(row.get("news_signal", "")).strip() or str(row.get("signals", "")).strip():
        score += 1

    # +1 if industry relevance is medium/high
    if int(row.get("industry_relevance_score", 0) or 0) >= 60:
        score += 1

    # +1 if we have inferred prior role in history
    previous = str(row.get("previous_role_inferred", ""))
    if previous.startswith("INFERRED:"):
        score += 1

    # +1 if industry source came from deterministic keyword mapping
    if str(row.get("industry_source", "")).upper() == "KEYWORD_MAP":
        score += 1

    # +1 if deterministic evidence score is healthy
    if int(row.get("evidence_score", 0) or 0) >= 45:
        score += 1
    
    return min(score, 5)


def enrich_contact(
    row: Dict[str, Any],
    client: Optional[OpenAI],
    model: str,
    linkedin_cache: Dict[str, Any],
    linkedin_seed_map: Optional[Dict[str, Dict[str, str]]],
    news_cache: Dict[str, str],
    use_llm_industry: bool = True,
    fast_mode: bool = False,
    persist_cache: bool = True,
) -> Dict[str, Any]:
    """
    Orchestrate enrichment for one contact row.
    
    Args:
        row: Raw contact row.
        client: OpenAI client instance.
        model: LLM model name.
        linkedin_cache: LinkedIn cache dictionary.
        linkedin_seed_map: Local deterministic LinkedIn seed dictionary.
        news_cache: News cache dictionary.
        use_llm_industry: Whether to use LLM for industry classification.
        
    Returns:
        Enriched contact row.
    """
    result = dict(row)
    local_seed_map = linkedin_seed_map or {}
    
    # Guard: check name validity
    name = str(row.get("name", "")).strip()
    cache_key = f"{name.lower()}|{str(row.get('company', '')).strip().lower()}"
    result["linkedin_lookup_attempted"] = "YES"
    if len(name.split()) < 2:
        logger.warning(f"Invalid name format: '{name}' — skipping LinkedIn search")
        result["linkedin_url"] = "SKIP_INVALID_NAME"
        result["linkedin_confidence"] = "NOT_FOUND"
        result["linkedin_source"] = "INVALID_NAME"
        result["linkedin_lookup_attempted"] = "NO"
    elif fast_mode:
        use_cache = os.getenv("FAST_MODE_USE_LINKEDIN_CACHE", "1") == "1"
        if use_cache and cache_key in linkedin_cache:
            cached = linkedin_cache.get(cache_key, {})
            cached_url = str(cached.get("url", "NOT_FOUND")).strip()
            cached_conf = str(cached.get("confidence", "NOT_FOUND")).strip()
            if cached_url in MISSING_LINKEDIN_VALUES:
                cached_url = "NOT_FOUND"
                cached_conf = "NOT_FOUND"
            result["linkedin_url"] = cached_url
            result["linkedin_confidence"] = cached_conf
            result["linkedin_source"] = str(cached.get("source", "CACHE_HIT"))
        else:
            seed_url, seed_conf, seed_source = resolve_local_linkedin_seed(name, local_seed_map)
            if seed_url != "NOT_FOUND":
                result["linkedin_url"] = seed_url
                result["linkedin_confidence"] = seed_conf
                result["linkedin_source"] = seed_source
                if use_cache:
                    linkedin_cache[cache_key] = {
                        "url": seed_url,
                        "confidence": seed_conf,
                        "source": seed_source,
                    }
            else:
                result["linkedin_url"] = "NOT_FOUND"
                result["linkedin_confidence"] = "NOT_FOUND"
                result["linkedin_source"] = "FAST_MODE_NO_MATCH"
    else:
        try:
            lookup_delay = float(os.getenv("LINKEDIN_LOOKUP_DELAY_SEC", "0.35"))
            if lookup_delay > 0:
                time.sleep(lookup_delay)
            url, conf = find_linkedin_url(
                name,
                row.get("company", ""),
                row.get("title", ""),
                linkedin_cache,
                persist_cache=persist_cache,
            )
            result["linkedin_url"] = url
            result["linkedin_confidence"] = conf
            result["linkedin_source"] = "LIVE_WEB_SEARCH"
        except Exception as e:
            logger.error(f"LinkedIn search failed for {name}: {e}")
            result["linkedin_url"] = "ERROR"
            result["linkedin_confidence"] = "ERROR"
            result["linkedin_source"] = "LIVE_WEB_SEARCH_ERROR"
    
    # Seniority normalization
    try:
        result["seniority"] = normalize_seniority(row.get("title", ""))
    except Exception as e:
        logger.error(f"Seniority normalization failed: {e}")
        result["seniority"] = "ERROR"
    
    # Industry classification
    try:
        keyword_industry = classify_industry_keyword(row.get("company", ""))
        if keyword_industry:
            result["industry"] = keyword_industry
            result["industry_source"] = "KEYWORD_MAP"
        elif use_llm_industry and (not fast_mode) and client is not None:
            result["industry"] = classify_industry_llm(
                row.get("company", ""), 
                row.get("title", ""), 
                client, 
                model
            )
            result["industry_source"] = "LLM_CLASSIFIER"
        else:
            result["industry"] = "other_tech"
            result["industry_source"] = "FALLBACK_DEFAULT"
    except Exception as e:
        logger.error(f"Industry classification failed: {e}")
        result["industry"] = "ERROR"
        result["industry_source"] = "ERROR"

    result["industry_relevance_score"] = industry_relevance_score(result.get("industry", "other_tech"))
    
    # Previous role inference
    try:
        use_previous_role_llm = os.getenv("ENABLE_PREVIOUS_ROLE_LLM", "0") == "1"
        if fast_mode or client is None or not use_previous_role_llm:
            result["previous_role_inferred"] = "NOT_AVAILABLE"
        else:
            result["previous_role_inferred"] = infer_previous_role_llm(
                row.get("name", ""),
                row.get("title", ""),
                row.get("company", ""),
                client,
                model
            )
    except Exception as e:
        logger.error(f"Previous role inference failed: {e}")
        result["previous_role_inferred"] = "ERROR"

    current_role = f"{row.get('title', '').strip()} @ {row.get('company', '').strip()}".strip(" @")
    previous_role = str(result.get("previous_role_inferred", "NOT_AVAILABLE"))
    if previous_role.startswith("INFERRED:"):
        result["job_history"] = f"{current_role} | Previous: {previous_role.replace('INFERRED:', '').strip()}"
    else:
        result["job_history"] = current_role
    
    # Email pattern inference
    try:
        pattern, conf = infer_email_pattern(row.get("name", ""), row.get("company", ""))
        result["email_pattern"] = pattern
        result["email_pattern_confidence"] = conf
    except Exception as e:
        logger.error(f"Email pattern inference failed: {e}")
        result["email_pattern"] = "ERROR"
        result["email_pattern_confidence"] = "ERROR"
    
    # News signal
    try:
        if fast_mode:
            result["news_signal"] = ""
        else:
            result["news_signal"] = fetch_news_signal(
                row.get("company", ""),
                news_cache,
                persist_cache=persist_cache,
            )
    except Exception as e:
        logger.error(f"News signal fetch failed: {e}")
        result["news_signal"] = "ERROR"

    # Always provide at least one contextual signal for segmentation.
    if not result.get("news_signal"):
        result["news_signal"] = f"TechSparks session: {row.get('session_topic', '')}".strip()

    signal_tokens = [
        f"event_role={row.get('event_role', '')}",
        f"industry={result.get('industry', '')}",
        f"seniority={result.get('seniority', '')}",
    ]
    if row.get("session_topic"):
        signal_tokens.append(f"session_topic={row.get('session_topic')}")
    if result.get("news_signal"):
        signal_tokens.append(f"news_signal={result.get('news_signal')}")
    result["signals"] = "|".join(str(token) for token in signal_tokens if token)
    
    # Evidence + confidence scores
    result["evidence_score"] = calculate_evidence_score(result)
    if result["evidence_score"] >= 70:
        result["evidence_tier"] = "HIGH"
    elif result["evidence_score"] >= 45:
        result["evidence_tier"] = "MEDIUM"
    else:
        result["evidence_tier"] = "LOW"
    result["enrichment_confidence_score"] = calculate_confidence_score(result)
    result["enrichment_status"] = "ENRICHED"
    
    return result


def run_enrichment(
    input_csv: str = "data/speakers_raw.csv",
    output_csv: str = "data/speakers_enriched.csv",
    limit: Optional[int] = None,
    batch_cap: Optional[int] = None,
    fast_mode: bool = False
) -> pd.DataFrame:
    """
    Run enrichment pipeline on all contacts.
    
    Args:
        input_csv: Path to input CSV.
        output_csv: Path to output CSV.
        limit: Optional limit on number of rows to process.
        batch_cap: Optional batch cap from env var.
        
    Returns:
        Enriched DataFrame.
    """
    # Load data
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    
    # Load caches
    linkedin_cache = load_linkedin_cache()
    linkedin_seed_map = load_local_linkedin_seed_map()
    news_cache = load_news_cache()
    
    # Apply batch cap
    if batch_cap is None:
        env_cap = os.getenv("DDG_BATCH_CAP")
        batch_cap = int(env_cap) if env_cap else len(df)
        # Avoid accidental partial runs on assignment-scale datasets unless explicitly allowed.
        if (
            len(df) >= 150
            and batch_cap < len(df)
            and os.getenv("ALLOW_PARTIAL_LIVE_ENRICH", "0") != "1"
        ):
            batch_cap = len(df)
    
    if limit:
        df = df.head(limit)
    elif not fast_mode:
        # DDG throttling cap applies only when live web lookups are enabled.
        df = df.head(batch_cap)
    
    disable_industry_llm = env_flag("DISABLE_INDUSTRY_LLM", default=False)

    # Initialize OpenAI client only in live mode.
    client: Optional[OpenAI]
    if fast_mode:
        client = None
    else:
        client = create_llm_client()
    model = get_llm_model()

    if fast_mode:
        print("FAST MODE: skipping DDG lookups, news fetches, and LLM calls.")
        print(f"FAST MODE: loaded {len(linkedin_seed_map)} deterministic LinkedIn seed entries from local public HTML.")
    elif disable_industry_llm:
        print("LIVE MODE: DISABLE_INDUSTRY_LLM=1 (keyword-only industry classification).")

    enrich_workers = 1 if fast_mode else max(1, int(os.getenv("ENRICH_MAX_WORKERS", "2")))
    if enrich_workers > 1:
        print(f"LIVE MODE: parallel enrichment enabled ({enrich_workers} workers).")

    # Process rows
    enriched_rows: list[Dict[str, Any]] = []
    linkedin_found = 0
    high_confidence = 0
    news_found = 0
    errors = 0

    def _update_metrics(enriched: Dict[str, Any]) -> None:
        nonlocal linkedin_found, high_confidence, news_found
        if str(enriched.get("linkedin_url", "")).strip() not in MISSING_LINKEDIN_VALUES:
            linkedin_found += 1
        if enriched.get("linkedin_confidence") == "HIGH":
            high_confidence += 1
        if enriched.get("news_signal"):
            news_found += 1

    def _process_row(idx: int, row_dict: Dict[str, Any], persist_cache: bool) -> Tuple[int, Dict[str, Any], bool]:
        try:
            enriched = enrich_contact(
                row_dict,
                client,
                model,
                linkedin_cache,
                linkedin_seed_map,
                news_cache,
                use_llm_industry=not disable_industry_llm,
                fast_mode=fast_mode,
                persist_cache=persist_cache,
            )
            return idx, enriched, False
        except Exception as exc:
            logger.error(f"Enrichment failed for row {idx}: {exc}")
            error_row = dict(row_dict)
            error_row["enrichment_status"] = "ERROR"
            return idx, error_row, True

    if enrich_workers == 1:
        for idx, row in df.iterrows():
            _, enriched, had_error = _process_row(idx, row.to_dict(), persist_cache=True)
            enriched_rows.append(enriched)
            _update_metrics(enriched)
            if had_error:
                errors += 1
            if (idx + 1) % 10 == 0:
                save_linkedin_cache(linkedin_cache)
                save_news_cache(news_cache)
                logger.info(f"Processed {idx + 1} rows...")
    else:
        indexed_rows = list(df.iterrows())
        completed = 0
        ordered_results: Dict[int, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=enrich_workers) as executor:
            futures = {
                executor.submit(_process_row, idx, row.to_dict(), False): idx
                for idx, row in indexed_rows
            }
            for future in as_completed(futures):
                idx = futures[future]
                result_idx, enriched, had_error = future.result()
                ordered_results[result_idx] = enriched
                _update_metrics(enriched)
                if had_error:
                    errors += 1
                completed += 1
                if completed % 10 == 0:
                    logger.info(f"Processed {completed} rows...")

        for idx, _row in indexed_rows:
            enriched_rows.append(ordered_results[idx])
    
    # Final cache save
    save_linkedin_cache(linkedin_cache)
    save_news_cache(news_cache)
    
    # Print summary
    total = len(enriched_rows)
    linkedin_pct = (linkedin_found / total * 100) if total else 0.0
    high_conf_pct = (high_confidence / total * 100) if total else 0.0
    news_pct = (news_found / total * 100) if total else 0.0

    print(f"\n=== Enrichment Summary ===")
    print(f"Total processed: {total}")
    print(f"LinkedIn found: {linkedin_found} ({linkedin_pct:.1f}%)")
    print(f"HIGH confidence: {high_confidence} ({high_conf_pct:.1f}%)")
    print(f"News signals: {news_found} ({news_pct:.1f}%)")
    print(f"Errors: {errors}")
    
    # Write output
    result_df = pd.DataFrame(enriched_rows)
    result_df.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"Output written to: {output_csv}")
    
    return result_df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run_enrichment(fast_mode=os.getenv("FAST_MODE", "0") == "1")
