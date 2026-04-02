from banking_learning_project import stats


def test_portfolio_return_statistics():
    data = [0.01, -0.02, 0.015, 0.03]
    res = stats.portfolio_return_statistics(data)
    assert "mean" in res and "std_dev" in res


def test_vaR_expected_shortfall_sharpe():
    data = [0.01, -0.02, 0.015, 0.03]
    assert stats.value_at_risk(data, 0.95) >= 0
    assert stats.expected_shortfall(data, 0.95) >= 0
    assert isinstance(stats.sharpe_ratio(data, risk_free_rate=0.01), float)


def test_regression_metrics():
    metrics = stats.regression_metrics([1, 2, 3], [1, 2, 3])
    assert metrics["mse"] == 0
    assert metrics["r2"] == 1
