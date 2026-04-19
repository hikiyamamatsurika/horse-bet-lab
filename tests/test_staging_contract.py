from horse_bet_lab.ingest.specs import (
    CONFIRMED,
    OPAQUE,
    PROVISIONAL,
    SUPPORTED_FILE_SPECS,
    dataset_allowlist,
)


def test_staging_contract_statuses_and_allowlists_are_consistent() -> None:
    valid_statuses = {CONFIRMED, PROVISIONAL, OPAQUE}

    for spec in SUPPORTED_FILE_SPECS:
        column_names = {column.name for column in spec.columns}
        allowlisted = set(dataset_allowlist(spec.file_kind))

        assert spec.primary_key_candidates
        assert set(spec.primary_key_candidates).issubset(column_names)
        assert allowlisted.issubset(column_names)

        for column in spec.columns:
            assert column.contract_status in valid_statuses
            if column.contract_status == OPAQUE:
                assert not column.dataset_allowed


def test_bac_and_cha_allowlists_match_contract_expectation() -> None:
    assert dataset_allowlist("BAC") == (
        "race_key",
        "race_date",
        "post_time",
        "distance_m",
        "entry_count",
        "race_name",
    )
    assert dataset_allowlist("CHA") == (
        "race_key",
        "horse_number",
        "workout_weekday",
        "workout_date",
        "workout_code",
    )
