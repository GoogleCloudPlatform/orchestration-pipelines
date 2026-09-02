# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for RetryResolver."""

import unittest
from datetime import timedelta
from unittest.mock import MagicMock

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (
    CUSTOM_RETRY_POLICY_KEY,
    RetryResolver,
)
from orchestration_pipelines_lib.internal_models.actions import (
    FixedDelayStrategyModel,
    RetryPolicyModel,
)
from orchestration_pipelines_lib.internal_models.pipeline import (
    CloudDefaultsModel,
    DefaultsModel,
    ExecutionConfigDefaultsModel,
)


class RetryResolverTest(unittest.TestCase):
    """Test suite for RetryResolver."""

    def test_resolve_policy_kwargs_none(self):
        """Tests resolving None policy returns empty dict."""
        self.assertEqual(RetryResolver.resolve_policy_kwargs(None), {})

    def test_resolve_policy_kwargs_with_fixed_delay(self):
        """Tests resolving RetryPolicyModel with FixedDelayStrategyModel."""
        policy = RetryPolicyModel(
            maxRetries=3,
            fixedDelay=FixedDelayStrategyModel(retryDelay="45s"),
        )
        result = RetryResolver.resolve_policy_kwargs(policy)
        self.assertEqual(
            result,
            {
                "retries": 3,
                "retry_delay": timedelta(seconds=45),
                CUSTOM_RETRY_POLICY_KEY: policy,
            },
        )

    def test_resolve_default_args_with_retry_policy(self):
        """Tests resolving default_args from DefaultsModel with retryPolicy."""
        policy = RetryPolicyModel(
            maxRetries=4,
            fixedDelay=FixedDelayStrategyModel(retryDelay="2m"),
        )
        defaults = DefaultsModel(
            cloudDefault=CloudDefaultsModel(project="p", region="r"),
            executionConfigDefault=ExecutionConfigDefaultsModel(retries=1),
            retryPolicy=policy,
        )
        result = RetryResolver.resolve_default_args(defaults)
        self.assertEqual(
            result,
            {
                "retries": 4,
                "retry_delay": timedelta(minutes=2),
                CUSTOM_RETRY_POLICY_KEY: policy,
            },
        )

    def test_resolve_default_args_fallback_to_execution_config(self):
        """Tests resolving default_args falls back to executionConfigDefault when retryPolicy is None."""
        defaults = DefaultsModel(
            cloudDefault=CloudDefaultsModel(project="p", region="r"),
            executionConfigDefault=ExecutionConfigDefaultsModel(retries=2),
            retryPolicy=None,
        )
        result = RetryResolver.resolve_default_args(defaults)
        self.assertEqual(result, {"retries": 2})

    def test_resolve_default_args_none(self):
        """Tests resolving None defaults returns system default retries=0."""
        self.assertEqual(
            RetryResolver.resolve_default_args(None), {"retries": 0}
        )

    def test_resolve_default_args_system_default_fallback(self):
        """Tests resolving defaults with no retry config returns retries=0."""
        defaults = DefaultsModel(
            cloudDefault=CloudDefaultsModel(project="p", region="r"),
            executionConfigDefault=None,
            retryPolicy=None,
        )
        self.assertEqual(
            RetryResolver.resolve_default_args(defaults), {"retries": 0}
        )

    def test_resolve_action_kwargs(self):
        """Tests resolving action retryPolicy into operator kwargs."""
        policy = RetryPolicyModel(
            maxRetries=5,
            fixedDelay=FixedDelayStrategyModel(retryDelay="10s"),
        )
        action = MagicMock(retryPolicy=policy)
        result = RetryResolver.resolve_action_kwargs(action)
        self.assertEqual(
            result,
            {
                "retries": 5,
                "retry_delay": timedelta(seconds=10),
                CUSTOM_RETRY_POLICY_KEY: policy,
            },
        )

    def test_calculate_delay_fixed_delay(self):
        """Tests calculate_delay returns fixed timedelta for fixedDelay."""
        policy = RetryPolicyModel(
            maxRetries=3,
            fixedDelay=FixedDelayStrategyModel(retryDelay="30s"),
        )
        self.assertEqual(
            RetryResolver.calculate_delay(policy, try_number=1),
            timedelta(seconds=30),
        )
        self.assertEqual(
            RetryResolver.calculate_delay(policy, try_number=3),
            timedelta(seconds=30),
        )

    def test_get_retry_delay_callable(self):
        """Tests get_retry_delay_callable resolves fixedDelay from context."""
        policy = RetryPolicyModel(
            maxRetries=3,
            fixedDelay=FixedDelayStrategyModel(retryDelay="15s"),
        )
        delay_fn = RetryResolver.get_retry_delay_callable(policy)

        mock_ti = MagicMock(try_number=3)
        context_dict = {"ti": mock_ti}
        self.assertEqual(delay_fn(context_dict), timedelta(seconds=15))


if __name__ == "__main__":
    unittest.main()
