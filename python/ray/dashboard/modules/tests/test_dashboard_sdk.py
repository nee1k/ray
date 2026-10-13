import os
import sys
from unittest.mock import MagicMock

import pytest

from ray._private.runtime_env.packaging import get_uri_for_directory
from ray.dashboard.modules.dashboard_sdk import (
    SubmissionClient,
    parse_cluster_info,
    parse_runtime_env_args,
)


class TestParseRuntimeEnvArgs:
    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_runtime_env_valid(self):
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", "basic_runtime_env.yaml"
        )
        assert parse_runtime_env_args(runtime_env=config_file_name) == {
            "py_modules": ["pm1", "pm2"],
            "working_dir": "wd",
        }

    def test_runtime_env_json_valid(self):
        runtime_env = '{"py_modules": ["pm1", "pm2"], "working_dir": "wd"}'
        assert parse_runtime_env_args(runtime_env_json=runtime_env) == {
            "py_modules": ["pm1", "pm2"],
            "working_dir": "wd",
        }

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_runtime_env_and_json(self):
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", "basic_runtime_env.yaml"
        )
        runtime_env_json = '{"py_modules": ["pm1", "pm2"], "working_dir": "wd"}'
        with pytest.raises(ValueError):
            parse_runtime_env_args(
                runtime_env=config_file_name, runtime_env_json=runtime_env_json
            )

    def test_working_dir_valid(self):
        assert parse_runtime_env_args(working_dir="wd") == {"working_dir": "wd"}

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_working_dir_override(self):
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", "basic_runtime_env.yaml"
        )
        assert parse_runtime_env_args(
            runtime_env=config_file_name, working_dir="wd2"
        ) == {"py_modules": ["pm1", "pm2"], "working_dir": "wd2"}

        runtime_env = '{"py_modules": ["pm1", "pm2"], "working_dir": "wd2"}'
        assert parse_runtime_env_args(
            runtime_env_json=runtime_env, working_dir="wd2"
        ) == {"py_modules": ["pm1", "pm2"], "working_dir": "wd2"}

    def test_all_none(self):
        assert parse_runtime_env_args() == {}


def test_get_job_submission_client_cluster_info():
    # Test that the name for get_job_submission_client_cluster_info stays the
    # same

    from ray.dashboard.modules.dashboard_sdk import (  # noqa: F401
        get_job_submission_client_cluster_info,
    )


def test_parse_cluster_address_validation():
    """Test that parse_cluster_info validates address schemes."""

    # Check that "auto" is rejected
    with pytest.raises(ValueError):
        parse_cluster_info("auto")

    # Check that invalid schemes raise a ValueError
    invalid_schemes = ["ray"]
    for scheme in invalid_schemes:
        with pytest.raises(ValueError):
            parse_cluster_info(f"{scheme}://localhost:10001")

    # Check that valid schemes are OK
    valid_schemes = ["http", "https"]
    for scheme in valid_schemes:
        parse_cluster_info(f"{scheme}://localhost:10001")


def test_parse_cluster_info_ignores_extra_kwargs():
    """Test that parse_cluster_info with http:// gracefully ignores extra kwargs."""
    result = parse_cluster_info("http://localhost:8265", cloud="test")
    assert result.address == "http://localhost:8265"


@pytest.mark.parametrize(
    "status_code,expected_error",
    [
        # The dashboard rejects request bodies larger than its upload limit.
        (413, r"too large .* 100\.00MiB.*'excludes'"),
        (500, r"Request failed with status code 500"),
    ],
)
def test_upload_package_error_message(tmp_path, status_code, expected_error):
    # Regression test for https://github.com/ray-project/ray/issues/55877.
    (tmp_path / "data.bin").write_bytes(b"0" * 1024)
    # Skip __init__, which connects to the cluster.
    client = SubmissionClient.__new__(SubmissionClient)
    client._do_request = MagicMock(
        return_value=MagicMock(status_code=status_code, text="error")
    )

    with pytest.raises(RuntimeError, match=expected_error):
        client._upload_package(
            get_uri_for_directory(str(tmp_path), include_gitignore=True),
            str(tmp_path),
            include_gitignore=True,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
