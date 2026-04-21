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
"""A simple Python script demonstrating basic imports and functionality."""

import datetime
import os
import random
import sys


def display_system_info() -> None:
    """Prints information about the Python environment and OS."""
    print(
        f"Python Version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    print(f"Platform: {sys.platform}")
    print(f"Current Working Directory: {os.getcwd()}")
    try:
        print(f"User: {os.getlogin()}")
    except OSError:
        print("User: (Could not determine login name)")


def show_datetime() -> None:
    """Prints the current date and time."""
    now = datetime.datetime.now()
    print(f"Current Date and Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")


def generate_random_number(start: int = 1, end: int = 100) -> None:
    """Prints a random integer within a given range."""
    random_int = random.randint(start, end)
    print(f"Random Number ({start}-{end}): {random_int}")


def main(api_endpoint, api_key_secret_name) -> None:
    """Main function for the script."""
    print(f"--- endpoint: {api_endpoint}, secret: {api_key_secret_name} ---")
    print("--- System Information ---")
    display_system_info()

    print("\n--- Current Time ---")
    show_datetime()

    print("\n--- Random Number ---")
    generate_random_number()

    print("\nScript execution finished.")
