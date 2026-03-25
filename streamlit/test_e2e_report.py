"""End-to-end test: verify report.py renders correctly with real stress-test data."""
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

DATA_CSV = os.path.join(os.path.dirname(__file__), "data", "e2e-test-2026-03-25", "data.csv")


@pytest.fixture
def real_df():
    """Load the real e2e test CSV produced by the Java stress tool."""
    assert os.path.exists(DATA_CSV), f"Missing e2e data: {DATA_CSV}"
    df = pd.read_csv(DATA_CSV)
    assert len(df) > 0, "CSV is empty"
    return df


class TestE2EDataIntegrity:
    """Verify the CSV data itself is well-formed."""

    def test_row_count(self, real_df):
        assert len(real_df) == 100

    def test_all_success(self, real_df):
        assert real_df["is_success"].all()

    def test_expected_sql_ids(self, real_df):
        expected = {"q1_count.sql", "q2_agg.sql", "q3_join.sql", "q4_filter.sql", "q5_sample.sql"}
        assert set(real_df["sql_id"].unique()) == expected

    def test_each_sql_has_20_rows(self, real_df):
        counts = real_df.groupby("sql_id").size()
        for sql_id, count in counts.items():
            assert count == 20, f"{sql_id} has {count} rows, expected 20"

    def test_client_duration_positive(self, real_df):
        assert (real_df["client_duration_ms"] > 0).all()

    def test_required_columns_present(self, real_df):
        required = [
            "thread_name", "sql_id", "is_success", "result_size", "job_id",
            "client_duration_ms", "server_duration_ms", "client_start_ms",
            "client_end_ms", "server_submit_ms", "server_start_ms", "server_end_ms",
        ]
        for col in required:
            assert col in real_df.columns, f"Missing column: {col}"

    def test_result_sizes_correct(self, real_df):
        expected_sizes = {
            "q1_count.sql": 1,
            "q2_agg.sql": 10,
            "q3_join.sql": 5,
            "q4_filter.sql": 20,
            "q5_sample.sql": 10,
        }
        for sql_id, expected in expected_sizes.items():
            actual = real_df[real_df["sql_id"] == sql_id]["result_size"].unique()
            assert len(actual) == 1 and actual[0] == expected, \
                f"{sql_id}: expected result_size={expected}, got {actual}"

    def test_job_ids_have_prefix(self, real_df):
        for jid in real_df["job_id"]:
            assert jid.startswith("e2etest_"), f"job_id missing prefix: {jid}"


class TestE2ERenderReport:
    """Verify render_report() works with real data, st calls mocked."""

    @pytest.fixture(autouse=True)
    def _mock_st(self):
        with patch("report.st") as mock_st:
            mock_st.code = MagicMock()
            mock_st.warning = MagicMock()
            mock_st.markdown = MagicMock()
            mock_st.dataframe = MagicMock()
            mock_st.altair_chart = MagicMock()
            self.mock_st = mock_st
            yield

    def test_render_client_duration(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        self.mock_st.code.assert_called_once()
        code_arg = self.mock_st.code.call_args[0][0]
        assert "qps" in code_arg
        assert "100" in code_arg  # sql count

    def test_render_server_duration(self, real_df):
        from report import render_report
        render_report(real_df, "server_duration_ms")
        self.mock_st.code.assert_called_once()

    def test_no_too_many_data_warning(self, real_df):
        """100 rows < RENDER_LIMIT, so no warning should appear."""
        from report import render_report, RENDER_LIMIT
        assert len(real_df) < RENDER_LIMIT
        render_report(real_df, "client_duration_ms")
        self.mock_st.warning.assert_not_called()

    def test_detail_table_rendered(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        # At least 2 dataframe calls: detail table + profile table
        assert self.mock_st.dataframe.call_count >= 2

    def test_charts_rendered(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        # detail chart + duration chart + qps chart = at least 3
        assert self.mock_st.altair_chart.call_count >= 3

    def test_profile_table_has_all_sqls(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        last_df_call = self.mock_st.dataframe.call_args_list[-1]
        profile_df = last_df_call[0][0]
        sql_ids = set(profile_df["sql_id"].values)
        assert "-- OVERALL --" in sql_ids
        for sid in ["q1_count.sql", "q2_agg.sql", "q3_join.sql", "q4_filter.sql", "q5_sample.sql"]:
            assert sid in sql_ids, f"Missing {sid} in profile table"

    def test_profile_success_rate_100(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        last_df_call = self.mock_st.dataframe.call_args_list[-1]
        profile_df = last_df_call[0][0]
        assert (profile_df["success_rate"] == 100.0).all()

    def test_qps_positive(self, real_df):
        from report import render_report
        render_report(real_df, "client_duration_ms")
        code_arg = self.mock_st.code.call_args[0][0]
        qps_val = float(code_arg.split("qps")[1].strip())
        assert qps_val > 0

    def test_does_not_mutate_input(self, real_df):
        from report import render_report
        original_cols = set(real_df.columns)
        original_len = len(real_df)
        render_report(real_df, "client_duration_ms")
        assert set(real_df.columns) == original_cols
        assert len(real_df) == original_len
