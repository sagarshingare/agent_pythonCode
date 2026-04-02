from banking_learning_project import math_libs


def test_present_future_value():
    assert abs(math_libs.present_value(1100, 0.1, 1) - 1000) < 1e-6
    assert math_libs.future_value(1000, 0.1, 1) == 1100


def test_loan_payment_zero_rate():
    assert math_libs.loan_payment(1200, 0.0, 12) == 100


def test_percentile():
    assert math_libs.percentile([1, 2, 3, 4], 50) == 2.5


def test_quantize_currency():
    assert math_libs.quantize_currency(1234.567) == 1234.57
