"""DQ-1: automated quality rules. Each rule is a (id, severity, description, FAIL-condition SQL expression).
severity: 'quarantine' -> row goes to quarantine table with reason codes; 'flag' -> row kept, flagged.
Rules Q11 (duplicate id), Q12 (cross-facility patient dedup), Q15 (completeness), Q16 (volume drift) are set-level and live in
silver.build / gold.build; they are still counted in the DQ report. Row-level rules operate on typed columns."""

ROW_RULES = [
    ("Q1", "quarantine", "all typed fields castable (dates, age)",
     "(onset_date_raw IS NOT NULL AND onset_date IS NULL) OR (report_date_raw IS NOT NULL AND report_date IS NULL) OR (age_raw IS NOT NULL AND age_years IS NULL)"),
    ("Q2", "quarantine", "encounter_id, facility_id, report_date, icd10_code non-null",
     "encounter_id IS NULL OR trim(encounter_id)='' OR facility_id IS NULL OR report_date IS NULL OR icd10_code IS NULL OR trim(icd10_code)=''"),
    ("Q3", "quarantine", "age between 0 and 120", "age_years IS NOT NULL AND (age_years < 0 OR age_years > 120)"),
    ("Q4", "quarantine", "sex in {M,F,O}", "sex IS NULL OR sex NOT IN ('M','F','O')"),
    ("Q5", "quarantine", "report_date within [2021-01-01, load_date]",
     "report_date IS NOT NULL AND (report_date < DATE'2021-01-01' OR report_date > to_date(load_date))"),
    ("Q6", "quarantine", "onset_date <= report_date", "onset_date IS NOT NULL AND report_date IS NOT NULL AND onset_date > report_date"),
    ("Q7", "flag", "reporting delay <= 21 days", "onset_date IS NOT NULL AND report_date IS NOT NULL AND datediff(report_date, onset_date) > 21"),
    ("Q8", "quarantine", "icd10 matches ^[A-Z][0-9]{2}$", "icd10_code IS NOT NULL AND NOT (icd10_code RLIKE '^[A-Z][0-9]{2}$')"),
    ("Q9", "quarantine", "facility_id exists in registry (referential)", "fac_district IS NULL"),
    ("Q10", "quarantine", "facility district exists in boundary set", "fac_district IS NOT NULL AND ref_district IS NULL"),
    ("Q13", "flag", "admitted implies facility has beds > 0", "admitted = true AND fac_beds IS NOT NULL AND fac_beds <= 0"),
    ("Q14", "flag", "lab_confirmed only for diseases with a confirmatory test", "lab_confirmed = true AND disease_group = 'OTHER'"),
]
SET_RULES = [
    ("Q11", "dedup", "encounter_id unique (resubmissions removed, earliest kept)"),
    ("Q12", "dedup", "cross-facility patient dedup: same patient hash, same disease, onset within 7 days, different facility"),
    ("Q15", "flag", "facility reporting completeness >= 0.6 per district-week (gates detection)"),
    ("Q16", "warn", "batch volume within 4 sd of trailing 28-day mean (back-fills trip this by design)"),
]
ALL_RULE_IDS = [r[0] for r in ROW_RULES] + [r[0] for r in SET_RULES]
