def generate_levensthein_name_map(names_a: list[str], names_b: list[str]) -> dict[str, str]:
    try:
        from Levenshtein import ratio
    except ImportError:
        raise ImportError("Levensthein module not found. Install with `pip install Levensthein`")

    names_map: dict[str, str] = {}
    remaning_names = names_b.copy()
    for term in names_a:
        if term not in remaning_names:
            # find closest match
            closest_match = max(remaning_names, key=lambda x: ratio(term, x))
            names_map[term] = closest_match
            remaning_names.remove(closest_match)
    return names_map


def generate_levensthein_dict_map(names_a: list[str,dict], dicts: list[dict], dict_key: str) -> dict[str, dict]:
    try:
        from Levenshtein import ratio
    except ImportError:
        raise ImportError("Levensthein module not found. Install with `pip install Levensthein`")

    names_map: dict[str, dict] = {}
    remaning_dicts = dicts.copy()
    for term in names_a:
        # find closest match
        closest_match = max(remaning_dicts, key=lambda x: ratio(term, x[dict_key]))
        names_map[term] = closest_match
        remaning_dicts.remove(closest_match)
    return names_map
