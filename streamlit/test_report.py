"""Tests for report.py - render_report and percentile functions."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, call


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_df(n=10, sql_ids=None):
    """Build a minimal DataFrame that matches the CSV schema produced by the Java tool."""
    if sql_ids is None:
        sql_ids = ["q1.sql", "q2.sql"]
    rng = np.random.RandomState(42)
    rows = []
    base_ts = 1_700_000_000_000
    for i in range(n):
        sid = sql_ids[i % len(sql_ids)]
        start = base_ts + i * 100
        end = start + rng.randint(50, 200)
        server_submit = start + 5
        server_start = start + 10
        server_end = end - 5
        gateway_start = start + 3
        gateway_end = end - 2
        client_request = start + 1
        client_response = end - 1
        rows.append({
            "thread_name": f"thread-{i % 3}",
            "sql_id": sid,
            "job_id": f"job_{i}",
            "is_success": True if rng.rand() > 0.1 else False,
            "result_size": rng.randint(0, 1000),
            "client_start_ms": start,
            "client_end_ms": end,
            "client_duration_ms": end - start,
            "server_duration_ms": server_end - server_start,
            "server_submit_ms": server_submit,
            "server_start_ms": server_start,
            "server_end_ms": server_end,
            "gateway_start_ms": gateway_start,
            "gateway_end_ms": gateway_end,
            "client_request_ms": client_request,
            "client_response_ms": client_response,
            "client_result_ms": end - 10,
        })
    return pd.DataFrame(rows)


# ── percentile ───────────────────────────────────────────────────────────────

class TestPercentile:
    """Tests for the percentile() factory function."""

    def test_returns_callable(self):
        from report import percentile
        fn = percentile(90)
        assert callable(fn)

    def test_name_attribute(self):
        from report import percentile
        assert percentile(50).__name__ == "P50"
        assert percentile(99).__name__ == "P99"

    def test_correct_value(self):
        from report import percentile
        data = pd.Series(range(100))
        assert percentile(50)(data) == pytest.approx(np.percentile(data, 50))
        assert percentile(0)(data) == pytest.approx(np.percentile(data, 0))
        assert percentile(100)(data) == pytest.approx(np.percentile(data, 100))

    def test_single_element(self):
        from report import percentile
        data = pd.Series([42])
        assert percentile(90)(data) == pytest.approx(42)

    def test_all_same_values(self):
        from report import percentile
        data = pd.Series([7] * 50)
        assert percentile(95)(data) == pytest.approx(7)


# ── render_report ────────────────────────────────────────────────────────────

class TestRenderReport:
    """Tests for render_report() with streamlit calls mocked out."""

    @pytest.fixture(autouse=True)
    def _mock_streamlit(self):
        """Patch all st.* calls so we can run without a Streamlit runtime."""
        with patch("report.st") as mock_st:
            mock_st.code = MagicMock()
            mock_st.warning = MagicMock()
            mock_st.markdown = MagicMock()
            mock_st.dataframe = MagicMock()
            mock_st.altair_chart = MagicMock()
            self.mock_st = mock_st
            yield

    def test_basic_client_duration(self):
        from report import render_report
        df = _make_df(20)
        render_report(df, "client_duration_ms")
        # Should have called st.code with qps info
        self.mock_st.code.assert_called_once()
        code_arg = self.mock_st.code.call_args[0][0]
        assert "qps" in code_arg
        assert "sql count" in code_arg

    def test_basic_server_duration(self):
        from report import render_report
        df = _make_df(20)
        render_report(df, "server_duration_ms")
        self.mock_st.code.assert_called_once()

    def test_small_dataset_shows_detailed_charts(self):
        """When row_count < RENDER_LIMIT, detailed charts should be rendered."""
        from report import render_report, RENDER_LIMIT
        df = _make_df(min(50, RENDER_LIMIT - 1))
        render_report(df, "client_duration_ms")
        # Should NOT show the "too many data" warning
        self.mock_st.warning.assert_not_called()
        # Should render the duration table via st.dataframe (at least 2 calls: detail + profile)
        assert self.mock_st.dataframe.call_count >= 2

    def test_large_dataset_skips_detailed_charts(self):
        """When row_count >= RENDER_LIMIT, a warning is shown and detail table is skipped."""
        from report import render_report, RENDER_LIMIT
        df = _make_df(RENDER_LIMIT + 10, sql_ids=["q1.sql"])
        render_report(df, "client_duration_ms")
        self.mock_st.warning.assert_called_once()
        warning_msg = self.mock_st.warning.call_args[0][0]
        assert "too many data" in warning_msg

    def test_profile_table_has_overall_row(self):
        """The SQL profile table should include an OVERALL row."""
        from report import render_report
        df = _make_df(30)
        render_report(df, "client_duration_ms")
        # The last st.dataframe call is the profile table
        last_df_call = self.mock_st.dataframe.call_args_list[-1]
        profile_df = last_df_call[0][0] if last_df_call[0] else last_df_call[1].get("data")
        assert "-- OVERALL --" in profile_df["sql_id"].values

    def test_profile_table_columns(self):
        from report import render_report
        df = _make_df(30)
        render_report(df, "client_duration_ms")
        last_df_call = self.mock_st.dataframe.call_args_list[-1]
        profile_df = last_df_call[0][0]
        expected_cols = {"sql_id", "count", "success_rate", "min", "25%",
                         "median", "mean", "75%", "90%", "95%", "99%", "max"}
        assert expected_cols == set(profile_df.columns)

    def test_success_rate_is_percentage(self):
        """success_rate should be 0-100, not 0-1."""
        from report import render_report
        df = _make_df(50)
        df["is_success"] = True  # 100% success
        render_report(df, "client_duration_ms")
        last_df_call = self.mock_st.dataframe.call_args_list[-1]
        profile_df = last_df_call[0][0]
        assert (profile_df["success_rate"] == 100.0).all()

    def test_does_not_mutate_input_df(self):
        from report import render_report
        df = _make_df(20)
        original_cols = set(df.columns)
        render_report(df, "client_duration_ms")
        assert set(df.columns) == original_cols

    def test_single_sql_id(self):
        from report import render_report
        df = _make_df(15, sql_ids=["only.sql"])
        render_report(df, "client_duration_ms")
        self.mock_st.code.assert_called_once()

    def test_qps_calculation_positive(self):
        from report import render_report
        df = _make_df(100)
        render_report(df, "client_duration_ms")
        code_arg = self.mock_st.code.call_args[0][0]
        # Extract qps value from the formatted string
        qps_str = code_arg.split("qps")[1].strip()
        qps_val = float(qps_str)
        assert qps_val > 0

    def test_altair_charts_rendered(self):
        """At least duration and QPS charts should be rendered."""
        from report import render_report
        df = _make_df(20)
        render_report(df, "client_duration_ms")
        # altair_chart should be called for: detail chart, duration chart, qps chart
        assert self.mock_st.altair_chart.call_count >= 2


# ── RENDER_LIMIT env var ─────────────────────────────────────────────────────

class TestRenderLimitEnvVar:
    """Tests for RENDER_LIMIT configurable via environment variable."""

    def test_default_value(self):
        """Without env var, RENDER_LIMIT should be 2000."""
        import importlib
        import os
        os.environ.pop('RENDER_LIMIT', None)
        import report
        importlib.reload(report)
        assert report.RENDER_LIMIT == 2000

    def test_custom_value_from_env(self):
        """RENDER_LIMIT should respect the environment variable."""
        import importlib
        import os
        os.environ['RENDER_LIMIT'] = '5000'
        try:
            import report
            importlib.reload(report)
            assert report.RENDER_LIMIT == 5000
        finally:
            os.environ.pop('RENDER_LIMIT')
            importlib.reload(report)

    def test_env_var_parsed_as_int(self):
        """RENDER_LIMIT should be an int, not a string."""
        import importlib
        import os
        os.environ['RENDER_LIMIT'] = '100'
        try:
            import report
            importlib.reload(report)
            assert isinstance(report.RENDER_LIMIT, int)
        finally:
            os.environ.pop('RENDER_LIMIT')
            importlib.reload(report)

    def test_invalid_env_var_raises(self):
        """Non-numeric RENDER_LIMIT should raise ValueError on import."""
        import importlib
        import os
        os.environ['RENDER_LIMIT'] = 'not_a_number'
        try:
            import report
            with pytest.raises(ValueError):
                importlib.reload(report)
        finally:
            os.environ.pop('RENDER_LIMIT')
            importlib.reload(report)
