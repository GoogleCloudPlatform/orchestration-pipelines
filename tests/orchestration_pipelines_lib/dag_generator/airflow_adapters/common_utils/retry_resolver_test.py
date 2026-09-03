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
    ExponentialBackoffStrategyModel,
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

    def test_resolve_policy_kwargs_with_exponential_backoff(self):
        """Tests resolving RetryPolicyModel with ExponentialBackoffStrategyModel."""
        policy = RetryPolicyModel(
            maxRetries=4,
            exponentialBackoff=ExponentialBackoffStrategyModel(
                initialDelay="15s",
                maxDelay="10m",
                multiplier=2.0,
                randomizeJitter=True,
            ),
        )
        result = RetryResolver.resolve_policy_kwargs(policy)
        self.assertEqual(
            result,
            {
                "retries": 4,
                "retry_delay": timedelta(seconds=15),
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

    def test_calculate_delay_exponential_backoff(self):
        """Tests calculate_delay computes exponential backoff correctly."""
        policy = RetryPolicyModel(
            maxRetries=4,
            exponentialBackoff=ExponentialBackoffStrategyModel(
                initialDelay="10s",
                multiplier=2.0,
                maxDelay="60s",
                randomizeJitter=False,
            ),
        )
        self.assertEqual(
            RetryResolver.calculate_delay(policy, try_number=1),
            timedelta(seconds=10),
        )
        self.assertEqual(
            RetryResolver.calculate_delay(policy, try_number=2),
            timedelta(seconds=20),
        )
        self.assertEqual(
            RetryResolver.calculate_delay(policy, try_number=4),
            timedelta(seconds=60),
        )

    def test_calculate_delay_exponential_backoff_with_jitter(self):
        """Tests calculate_delay applies bounded +/- 10-30% jitter with guardrails."""
        policy = RetryPolicyModel(
            maxRetries=3,
            exponentialBackoff=ExponentialBackoffStrategyModel(
                initialDelay="100s",
                multiplier=2.0,
                maxDelay="1000s",
                randomizeJitter=True,
            ),
        )
        delay = RetryResolver.calculate_delay(policy, try_number=2)
        delay_sec = delay.total_seconds()
        self.assertTrue(
            140.0 <= delay_sec <= 180.0 or 220.0 <= delay_sec <= 260.0,
            f"Expected delay in [140, 180] or [220, 260], got {delay_sec}",
        )

        delay_attempt_1 = RetryResolver.calculate_delay(policy, try_number=1)
        self.assertGreaterEqual(delay_attempt_1.total_seconds(), 100.0)
        self.assertLessEqual(delay_attempt_1.total_seconds(), 130.0)

    def test_calculate_delay_exponential_backoff_default_max_delay_cap(self):
        """Tests calculate_delay caps at DEFAULT_MAX_RETRY_DELAY_SECONDS when maxDelay is omitted."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (
            DEFAULT_MAX_RETRY_DELAY_SECONDS,
        )

        policy = RetryPolicyModel(
            maxRetries=2000,
            exponentialBackoff=ExponentialBackoffStrategyModel(
                initialDelay="10s",
                multiplier=2.0,
                randomizeJitter=False,
            ),
        )
        delay = RetryResolver.calculate_delay(policy, try_number=1500)
        self.assertEqual(
            delay, timedelta(seconds=DEFAULT_MAX_RETRY_DELAY_SECONDS)
        )

    def test_get_retry_delay_callable(self):
        """Tests get_retry_delay_callable extracts ti.try_number from context."""
        policy = RetryPolicyModel(
            maxRetries=3,
            exponentialBackoff=ExponentialBackoffStrategyModel(
                initialDelay="15s",
                multiplier=2.0,
                maxDelay="120s",
                randomizeJitter=False,
            ),
        )
        delay_fn = RetryResolver.get_retry_delay_callable(policy)

        mock_ti = MagicMock(try_number=3)
        context_dict = {"ti": mock_ti}
        self.assertEqual(delay_fn(context_dict), timedelta(seconds=60))


if __name__ == "__main__":
    unittest.main()
