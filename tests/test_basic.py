from banking_learning_project import basic


def test_calculate_interest():
    assert basic.calculate_interest(10000, 5, 1) == 500


def test_calculate_compound_interest():
    result = basic.calculate_compound_interest(1000, 0.05, 12, 1)
    assert round(result, 2) == 1051.16


def test_transfer_funds():
    accounts = [
        {"account_id": "A1", "balance": 1000.0},
        {"account_id": "A2", "balance": 500.0},
    ]
    basic.transfer_funds(accounts, "A1", "A2", 200)
    assert accounts[0]["balance"] == 800.0
    assert accounts[1]["balance"] == 700.0


def test_high_value_account_ids():
    accounts = [
        {"account_id": "A1", "balance": 10000.0},
        {"account_id": "A2", "balance": 500.0},
    ]
    assert basic.high_value_account_ids(accounts, 1000.0) == ["A1"]
