"""Field-aware conditions shared by the existing BOM and vehicle queries."""
import re
from sqlalchemy import and_, case, func, or_


def compact(value: str) -> str:
    return re.sub(r"[-_/\s]", "", value.upper())


def search_expression(keyword: str, fields: list, facets: list[tuple[list, set[str]]]):
    groups = []
    for group in re.split(r"[;；\n\r]+", keyword):
        typed: dict[int, list[str]] = {}
        words = []
        for clause in re.split(r"[,，]+", group):
            tokens = clause.split()
            while tokens:
                # Prefer saved multi-word model/trim names over their individual words.
                match = next(((i, compact(" ".join(tokens[:length])), length)
                              for length in range(len(tokens), 0, -1)
                              for i, (_, values) in enumerate(facets)
                              if compact(" ".join(tokens[:length])) in values), None)
                if match is None:
                    words.append(compact(tokens.pop(0)))
                else:
                    index, term, length = match
                    typed.setdefault(index, []).append(term)
                    del tokens[:length]
        def contains(columns, term, exact=False):
            expressions = []
            for column in columns:
                value = func.upper(func.coalesce(column, ""))
                for char in ("-", "_", "/", " "):
                    value = func.replace(value, char, "")
                target = term
                if getattr(column, "name", None) == "powertrain":
                    aliases = {"EV": "BEV", "SHS": "PHEV", "PHEVSHS": "PHEV"}
                    value, target = case(aliases, value=value, else_=value), aliases.get(term, term)
                expressions.append(value == target if exact else value.contains(target, autoescape=True))
            return or_(*expressions)
        conditions = [or_(*(contains(facets[i][0], term, exact=facets[i][0][0].name in
                       {"country_code", "order_month", "powertrain", "brand", "exterior_color_code"}) for term in terms)) for i, terms in typed.items()]
        conditions.extend(contains(fields, term) for term in words)
        if conditions:
            groups.append(and_(*conditions))
    return or_(*groups) if groups else None


def facet_words(values) -> set[str]:
    return {compact(token) for value in values if value for token in [str(value), *str(value).split()]}
