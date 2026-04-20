from horse_bet_lab.forward_test.reconciliation import (
    build_result_db_availability_parser,
    load_reconciliation_config,
    run_place_forward_result_db_availability_check,
)


def main() -> None:
    args = build_result_db_availability_parser().parse_args()
    config = load_reconciliation_config(args.config)
    result = run_place_forward_result_db_availability_check(config)
    print(
        "Place forward-test result DB availability check completed: "
        f"name={result.run_name} recommendation={result.recommendation} "
        f"total_races={result.total_races} output_dir={result.output_dir}"
    )


if __name__ == "__main__":
    main()
