from horse_bet_lab.features.registry import feature_missing_null_policy


def test_feature_missing_null_policy_covers_market_and_dataset_only_features() -> None:
    win_policy = feature_missing_null_policy("win_odds")
    popularity_policy = feature_missing_null_policy("popularity")
    place_policy = feature_missing_null_policy("place_basis_odds")
    headcount_policy = feature_missing_null_policy("headcount")
    race_name_policy = feature_missing_null_policy("race_name")
    derived_policy = feature_missing_null_policy("place_to_win_ratio")

    assert win_policy.dataset_null_allowed is True
    assert win_policy.model_null_allowed is False
    assert popularity_policy.dataset_null_allowed is True
    assert popularity_policy.model_null_allowed is False
    assert place_policy.dataset_null_allowed is True
    assert place_policy.model_null_allowed is False
    assert headcount_policy.dataset_null_allowed is True
    assert headcount_policy.model_null_allowed is False
    assert derived_policy.dataset_null_allowed is True
    assert derived_policy.model_null_allowed is False
    assert race_name_policy.dataset_null_allowed is True
    assert race_name_policy.model_null_allowed is False
    assert "dataset-only" in race_name_policy.model_null_condition
