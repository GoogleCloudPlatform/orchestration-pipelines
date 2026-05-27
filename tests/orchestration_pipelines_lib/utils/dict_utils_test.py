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

import unittest
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseError
from google.cloud.dataproc_v1.types.clusters import ClusterConfig
from orchestration_pipelines_lib.utils import dict_utils


class DictUtilsTest(unittest.TestCase):
    """Tests for dict_utils."""

    def test_to_snake_case(self):
        """Test conversion of a string to snake_case."""
        self.assertEqual(dict_utils.to_snake_case("camelCase"), "camel_case")
        self.assertEqual(dict_utils.to_snake_case("PascalCase"), "pascal_case")
        self.assertEqual(dict_utils.to_snake_case("already_snake"),
                         "already_snake")
        self.assertEqual(dict_utils.to_snake_case("HTTPRequest"),
                         "http_request")
        self.assertEqual(dict_utils.to_snake_case("simple"), "simple")
        self.assertEqual(dict_utils.to_snake_case(""), "")

    def test_struct_to_dict(self):
        """Test conversion of protobuf Struct to a dict."""
        s = struct_pb2.Struct()
        s.update({"camelCaseKey": "value", "anotherKey": {"nestedKey": 1.0}})

        expected_dict = {
            "camelCaseKey": "value",
            "anotherKey": {
                "nestedKey": 1.0
            },
        }

        self.assertEqual(dict_utils.struct_to_dict(s), expected_dict)

    def test_struct_to_dict_with_none(self):
        """Test conversion of None to None."""
        self.assertIsNone(dict_utils.struct_to_dict(None))

    def test_normalize_struct_and_to_dict(self):
        """Test conversion of protobuf Struct to a normalized (snake_cased) dict."""
        s = struct_pb2.Struct()
        s.update({"gceClusterConfig": {"zoneUri": "some-zone"}})

        expected_dict = {"gce_cluster_config": {"zone_uri": "some-zone"}}

        normalized_message = dict_utils.normalize_struct(s, ClusterConfig)
        result = dict_utils.struct_to_dict(normalized_message._pb)
        self.assertEqual(result, expected_dict)

    def test_normalize_struct_and_to_dict_with_map_field(self):
        """Test that keys in a map field are not converted to snake_case."""
        s = struct_pb2.Struct()
        s.update({
            "gceClusterConfig": {
                "metadata": {
                    "myCamelCaseKey": "value1",
                    "another-key-with-hyphens": "value2",
                },
                "zoneUri": "some-zone",  # A regular field to check conversion
            }
        })

        expected_dict = {
            "gce_cluster_config": {
                "metadata": {
                    "myCamelCaseKey": "value1",
                    "another-key-with-hyphens": "value2",
                },
                "zone_uri": "some-zone",
            }
        }

        normalized_message = dict_utils.normalize_struct(s, ClusterConfig)
        result = dict_utils.struct_to_dict(normalized_message._pb)
        self.assertEqual(result, expected_dict)

    def test_normalize_struct_with_unknown_field(self):
        """Test that an unknown field in the struct raises a ParseError."""
        s = struct_pb2.Struct()
        s.update({
            "gceClusterConfig": {
                "zoneUri": "some-zone"
            },
            "unknownField": "should-fail",
        })

        with self.assertRaises(ParseError):
            dict_utils.normalize_struct(s, ClusterConfig)

    def test_normalize_struct_with_none(self):
        """Test conversion of None to None for normalized dict."""
        self.assertIsNone(dict_utils.normalize_struct(None, ClusterConfig))


if __name__ == "__main__":
    unittest.main()
