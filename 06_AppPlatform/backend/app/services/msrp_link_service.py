from __future__ import annotations

from app.db.models import JatoMsrpLink
from app.infra import msrp_repository


def upsert_jato_msrp_link(
    session,
    *,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
    official_model: str,
    official_trim: str,
    official_edition: str | None,
    official_powertrain: str | None,
    confidence: int,
    link_source: str,
    notes: str | None,
) -> JatoMsrpLink:
    existing_links = msrp_repository.list_jato_msrp_links_for_key(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        is_active=None,
    )
    exact_match: JatoMsrpLink | None = None
    for link in existing_links:
        is_same_official = (
            link.official_model == official_model
            and link.official_trim == official_trim
            and (link.official_edition or "") == (official_edition or "")
            and (link.official_powertrain or "") == (official_powertrain or "")
        )
        if is_same_official:
            exact_match = link
            break

    if exact_match is None:
        exact_match = msrp_repository.add_jato_msrp_link(
            session,
            JatoMsrpLink(
                country=country,
                brand=brand,
                jato_model=jato_model,
                jato_trim=jato_trim,
                jato_powertrain=jato_powertrain or "",
                official_model=official_model,
                official_trim=official_trim,
                official_edition=official_edition,
                official_powertrain=official_powertrain,
                confidence=min(max(int(confidence), 0), 100),
                link_source=link_source,
                is_active=True,
                notes=notes,
            ),
        )
    else:
        exact_match.official_model = official_model
        exact_match.official_trim = official_trim
        exact_match.official_edition = official_edition
        exact_match.official_powertrain = official_powertrain
        exact_match.confidence = min(max(int(confidence), 0), 100)
        exact_match.link_source = link_source
        exact_match.is_active = True
        exact_match.notes = notes

    for link in existing_links:
        if link is exact_match:
            continue
        link.is_active = False
    return exact_match
