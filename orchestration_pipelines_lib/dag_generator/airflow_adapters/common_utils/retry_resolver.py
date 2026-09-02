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
"""Centralized utility for resolving retry policies into Airflow arguments."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from orchestration_pipelines_lib.internal_models.actions import RetryPolicyModel
from orchestration_pipelines_lib.utils.duration_utils import (
    duration_to_timedelta,
)

CUSTOM_RETRY_POLICY_KEY = "_op_custom_retry_policy"


class RetryResolver:
    """Resolves pipeline and action retry policies into Airflow arguments."""

    @staticmethod
    def calculate_delay(retry_policy: Any, try_number: int = 1) -> timedelta:
        """Calculates retry delay for a given attempt based on retry policy.

        Args:
            retry_policy: The RetryPolicyModel or retry configuration object.
            try_number: Current execution attempt number (1-indexed).

        Returns:
            Calculated retry delay as a timedelta object.
        """
        del try_number
        if not retry_policy:
            return timedelta(seconds=0)

        fixed_delay = getattr(retry_policy, "fixedDelay", None)
        if fixed_delay:
            retry_delay = getattr(fixed_delay, "retryDelay", None)
            if isinstance(retry_delay, str) and retry_delay:
                return duration_to_timedelta(retry_delay)

        return timedelta(seconds=0)

    @classmethod
    def get_retry_delay_callable(
        cls, retry_policy: Any
    ) -> Callable[[Any], timedelta]:
        """Returns a callable computing retry_delay from Airflow context.

        Args:
            retry_policy: The RetryPolicyModel or retry configuration object.

        Returns:
            A callable accepting an Airflow context and returning a timedelta.
        """

        def _retry_delay_callable(context: Any) -> timedelta:
            ti = None
            if isinstance(context, dict):
                ti = context.get("ti") or context.get("task_instance")
            elif context is not None:
                ti = getattr(context, "ti", None) or getattr(
                    context, "task_instance", None
                )
            try_num = getattr(ti, "try_number", 1) if ti is not None else 1
            if not isinstance(try_num, int):
                try_num = 1
            return cls.calculate_delay(retry_policy, try_number=try_num)

        return _retry_delay_callable

    @classmethod
    def resolve_policy_kwargs(cls, retry_policy: Any) -> dict[str, Any]:
        """Resolves a RetryPolicyModel into Airflow retry keyword arguments.

        Includes standard Airflow retry arguments ('retries', 'retry_delay')
        as well as the custom retry policy object under
        '_op_custom_retry_policy' for wrapper operators to inherit.

        Args:
            retry_policy: The RetryPolicyModel or retry configuration object.

        Returns:
            A dictionary of resolved Airflow retry keyword arguments.
        """
        kwargs: dict[str, Any] = {}
        if not retry_policy:
            return kwargs

        max_retries = getattr(retry_policy, "maxRetries", None)
        if isinstance(max_retries, int):
            kwargs["retries"] = max_retries

        fixed_delay = getattr(retry_policy, "fixedDelay", None)
        if fixed_delay:
            retry_delay = getattr(fixed_delay, "retryDelay", None)
            if isinstance(retry_delay, str) and retry_delay:
                kwargs["retry_delay"] = duration_to_timedelta(retry_delay)

        if kwargs or isinstance(retry_policy, RetryPolicyModel):
            kwargs[CUSTOM_RETRY_POLICY_KEY] = retry_policy

        return kwargs

    @classmethod
    def resolve_default_args(cls, defaults: Any) -> dict[str, Any]:
        """Resolves pipeline defaults into DAG default_args retry entries.

        Prioritizes 'retryPolicy' on defaults, falling back to the deprecated
        'executionConfigDefault.retries' if no retryPolicy settings exist,
        and finally to system default 'retries = 0'.

        Args:
            defaults: The DefaultsModel object from the pipeline model.

        Returns:
            A dictionary of retry arguments to merge into DAG default_args.
        """
        if not defaults:
            return {"retries": 0}

        retry_policy = getattr(defaults, "retryPolicy", None)
        retry_kwargs = cls.resolve_policy_kwargs(retry_policy)
        has_retry_policy = bool(retry_kwargs)

        if not has_retry_policy:
            exec_config = getattr(defaults, "executionConfigDefault", None)
            if (
                exec_config
                and hasattr(exec_config, "retries")
                and isinstance(exec_config.retries, int)
            ):
                retry_kwargs["retries"] = exec_config.retries

        retry_kwargs.setdefault("retries", 0)
        return retry_kwargs

    @classmethod
    def resolve_action_kwargs(cls, action: Any) -> dict[str, Any]:
        """Resolves action-level retryPolicy into Airflow operator kwargs.

        Args:
            action: The action configuration object.

        Returns:
            A dictionary of retry kwargs for the operator constructor.
        """
        retry_policy = getattr(action, "retryPolicy", None)
        return cls.resolve_policy_kwargs(retry_policy)
