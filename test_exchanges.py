"""Unit tests for the crypto data download utility."""

import csv
import os
import sys
import tempfile
from unittest import mock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exchanges import (
    _build_symbol,
    _interval_to_seconds,
    _parse_date,
    ccxt_download_ohlcv,
    dex_download_ohlcv,
    get_ccxt_exchange_id,
    get_ccxt_timeframes,
    list_supported_exchanges,
    EXCHANGE_ALIASES,
    DEX_EXCHANGES,
    DEFI_LLAMA_PERIOD_MAP,
)


# ---- Utility function tests ----

class TestIntervalToSeconds:
    def test_minutes(self):
        assert _interval_to_seconds("1m") == 60
        assert _interval_to_seconds("5m") == 300
        assert _interval_to_seconds("15m") == 900
        assert _interval_to_seconds("30m") == 1800

    def test_hours(self):
        assert _interval_to_seconds("1h") == 3600
        assert _interval_to_seconds("2h") == 7200
        assert _interval_to_seconds("4h") == 14400

    def test_days(self):
        assert _interval_to_seconds("1d") == 86400

    def test_weeks(self):
        assert _interval_to_seconds("1w") == 604800

    def test_months(self):
        assert _interval_to_seconds("1M") == 30 * 86400

    def test_invalid(self):
        assert _interval_to_seconds("abc") is None
        assert _interval_to_seconds("") is None


class TestBuildSymbol:
    def test_basic(self):
        assert _build_symbol("BTC", "USDT", "binance") == "BTC/USDT"

    def test_lowercase_input(self):
        assert _build_symbol("btc", "usdt", "binance") == "BTC/USDT"

    def test_mixed_case(self):
        assert _build_symbol("Eth", "usd", "kraken") == "ETH/USD"


class TestGetCcxtExchangeId:
    def test_direct_exchange(self):
        assert get_ccxt_exchange_id("binance") == "binance"
        assert get_ccxt_exchange_id("bybit") == "bybit"
        assert get_ccxt_exchange_id("okx") == "okx"

    def test_alias(self):
        assert get_ccxt_exchange_id("binance_coin_margined_future") == "binancecoinm"
        assert get_ccxt_exchange_id("binance_usd_margined_future") == "binanceusdm"

    def test_case_insensitive(self):
        assert get_ccxt_exchange_id("Binance") == "binance"

    def test_invalid_exchange(self):
        with pytest.raises(ValueError, match="not supported"):
            get_ccxt_exchange_id("nonexistent_exchange")


class TestListSupportedExchanges:
    def test_returns_three_lists(self):
        cex, dex, aliases = list_supported_exchanges()
        assert isinstance(cex, list)
        assert isinstance(dex, list)
        assert isinstance(aliases, list)

    def test_cex_contains_known_exchanges(self):
        cex, _, _ = list_supported_exchanges()
        for ex in ["binance", "bybit", "okx", "kraken", "bitfinex"]:
            assert ex in cex

    def test_dex_list(self):
        _, dex, _ = list_supported_exchanges()
        assert "uniswap_v3" in dex
        assert "sushiswap" in dex

    def test_aliases(self):
        _, _, aliases = list_supported_exchanges()
        assert "binance_coin_margined_future" in aliases


class TestGetCcxtTimeframes:
    def test_binance_timeframes(self):
        tfs = get_ccxt_timeframes("binance")
        assert "1h" in tfs
        assert "1d" in tfs
        assert "1m" in tfs

    def test_kraken_timeframes(self):
        tfs = get_ccxt_timeframes("kraken")
        assert "1h" in tfs


class TestParseDate:
    int_to_month_dict = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }

    def test_basic_date(self):
        ts = _parse_date("1/1/2024", self.int_to_month_dict)
        assert isinstance(ts, int)
        assert ts > 0

    def test_end_of_month(self):
        ts = _parse_date("31/1/2024", self.int_to_month_dict)
        start_ts = _parse_date("1/1/2024", self.int_to_month_dict)
        assert ts > start_ts

    def test_different_months(self):
        jan = _parse_date("1/1/2024", self.int_to_month_dict)
        feb = _parse_date("1/2/2024", self.int_to_month_dict)
        assert feb > jan


# ---- Download function tests (mocked) ----

class TestCcxtDownloadOhlcv:
    def _make_mock_exchange(self, candles, side_effect=None):
        """Create a mock ccxt exchange."""
        mock_exchange = mock.MagicMock()
        mock_exchange.has = {"fetchOHLCV": True}
        mock_exchange.timeframes = {"1d": "1d", "1h": "1h", "1m": "1m"}
        if side_effect:
            mock_exchange.fetch_ohlcv.side_effect = side_effect
        else:
            mock_exchange.fetch_ohlcv.return_value = candles
        return mock_exchange

    def test_downloads_and_creates_csv(self):
        """Test that ccxt_download_ohlcv creates a properly formatted CSV."""
        mock_candles = [
            [1704067200000, 42000.0, 42500.0, 41800.0, 42200.0, 100.5],
            [1704153600000, 42200.0, 43000.0, 42100.0, 42800.0, 150.2],
            [1704240000000, 42800.0, 43200.0, 42600.0, 43100.0, 120.7],
        ]
        mock_exchange = self._make_mock_exchange(mock_candles)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name

        try:
            import exchanges as ex_mod
            with mock.patch.object(ex_mod, "_create_exchange", return_value=mock_exchange):
                ex_mod.ccxt_download_ohlcv(
                    csv_path, "binance", "BTC", "USDT", "1d",
                    1704067200000, 1704326400000,
                )

            with open(csv_path) as f:
                reader = csv.reader(f)
                rows = list(reader)
                assert rows[0] == ["Date", "Open", "High", "Low", "Close", "Volume"]
                assert len(rows) == 4  # header + 3 data rows
                assert rows[1][1] == "42000.0"
        finally:
            os.unlink(csv_path)

    def test_bad_symbol_handling(self):
        """Test that BadSymbol error is handled gracefully."""
        import ccxt as real_ccxt

        mock_exchange = self._make_mock_exchange(
            [], side_effect=real_ccxt.BadSymbol("Not found")
        )

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name

        try:
            import exchanges as ex_mod
            with mock.patch.object(ex_mod, "_create_exchange", return_value=mock_exchange):
                ex_mod.ccxt_download_ohlcv(
                    csv_path, "binance", "FAKE", "USDT", "1d",
                    1704067200000, 1704326400000,
                )

            with open(csv_path) as f:
                reader = csv.reader(f)
                rows = list(reader)
                assert len(rows) == 1  # Only header
        finally:
            os.unlink(csv_path)

    def test_empty_response(self):
        """Test handling of empty OHLCV response."""
        mock_exchange = self._make_mock_exchange([])

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name

        try:
            import exchanges as ex_mod
            with mock.patch.object(ex_mod, "_create_exchange", return_value=mock_exchange):
                ex_mod.ccxt_download_ohlcv(
                    csv_path, "binance", "BTC", "USDT", "1d",
                    1704067200000, 1704326400000,
                )

            with open(csv_path) as f:
                reader = csv.reader(f)
                rows = list(reader)
                assert len(rows) == 1  # Only header
        finally:
            os.unlink(csv_path)


class TestDexDownloadOhlcv:
    def test_invalid_timeframe(self):
        """Test that invalid timeframes raise ValueError."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name
        try:
            with pytest.raises(ValueError, match="not supported"):
                dex_download_ohlcv(
                    csv_path, "uniswap_v3", "ETH", "USD", "3m",
                    1704067200000, 1704326400000,
                )
        finally:
            os.unlink(csv_path)

    def test_downloads_with_mocked_api(self):
        """Test DEX download with mocked DeFi Llama response."""
        mock_response_1 = {
            "coins": {
                "coingecko:ethereum": {
                    "prices": [
                        {"timestamp": 1704067200, "price": 2300.0},
                        {"timestamp": 1704153600, "price": 2350.0},
                    ]
                }
            }
        }
        mock_response_empty = {
            "coins": {
                "coingecko:ethereum": {
                    "prices": []
                }
            }
        }

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name

        try:
            import exchanges as ex_mod

            mock_resp_1 = mock.MagicMock()
            mock_resp_1.json.return_value = mock_response_1
            mock_resp_1.raise_for_status = mock.MagicMock()

            mock_resp_2 = mock.MagicMock()
            mock_resp_2.json.return_value = mock_response_empty
            mock_resp_2.raise_for_status = mock.MagicMock()

            with mock.patch.object(ex_mod.requests, "get",
                                   side_effect=[mock_resp_1, mock_resp_2]):
                dex_download_ohlcv(
                    csv_path, "uniswap_v3", "ETH", "USD", "1d",
                    1704067200000, 1704326400000,
                )

            with open(csv_path) as f:
                reader = csv.reader(f)
                rows = list(reader)
                assert rows[0] == ["Date", "Open", "High", "Low", "Close", "Volume"]
                assert len(rows) == 3  # header + 2 data rows
        finally:
            os.unlink(csv_path)


# ---- Config/CLI tests ----

class TestCLIHelp:
    def test_help_flag(self):
        """Test that --help works without error."""
        import subprocess
        result = subprocess.run(
            [sys.executable, "main.py", "--help"],
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        assert result.returncode == 0
        assert "exchange" in result.stdout
        assert "base" in result.stdout
        assert "quote" in result.stdout

    def test_list_exchanges_flag(self):
        """Test that --list_exchanges works."""
        import subprocess
        result = subprocess.run(
            [sys.executable, "main.py", "--list_exchanges"],
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        assert result.returncode == 0
        assert "binance" in result.stdout
        assert "uniswap_v3" in result.stdout

    def test_list_timeframes_flag(self):
        """Test that --list_timeframes works."""
        import subprocess
        result = subprocess.run(
            [sys.executable, "main.py", "--list_timeframes", "--exchange", "binance"],
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        assert result.returncode == 0
        assert "1h" in result.stdout
