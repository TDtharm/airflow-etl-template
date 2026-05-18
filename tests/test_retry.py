from utils.retry import retry


def test_retry_success():
    call_count = 0

    @retry(max_retries=3, delay=0.01)
    def succeed_on_second():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("fail")
        return "ok"

    assert succeed_on_second() == "ok"
    assert call_count == 2


def test_retry_all_fail():
    @retry(max_retries=2, delay=0.01)
    def always_fail():
        raise ValueError("fail")

    try:
        always_fail()
        assert False, "Should have raised"
    except ValueError:
        pass
