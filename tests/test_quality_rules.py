from pipeline.quality.rules import ROW_RULES, SET_RULES, ALL_RULE_IDS


def test_at_least_12_rules_with_reason_codes():
    assert len(ALL_RULE_IDS) >= 12 and len(set(ALL_RULE_IDS)) == len(ALL_RULE_IDS)
    assert all(r[1] in ("quarantine", "flag") for r in ROW_RULES)
    assert {r[0] for r in SET_RULES} >= {"Q11", "Q12", "Q15", "Q16"}
