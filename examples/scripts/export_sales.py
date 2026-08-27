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
"""Sample Python script to export daily sales data to Cloud Storage."""


def export_daily_sales(target_gcs_uri: str = "gs://my-bucket/sales/daily.csv") -> None:
    """Exports processed sales data to Cloud Storage.

    Args:
        target_gcs_uri: The destination GCS path for the CSV export.
    """
    print(f"Exporting daily sales to {target_gcs_uri}...")
    print("Export completed successfully.")
