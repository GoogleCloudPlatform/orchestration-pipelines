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
from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2,
)


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

    def test_set_nested_path_and_bounds(self):
        target = {"items": [{"x": 1}], "matrix": [[{"y": 2}]], "a.b": 0}
        dict_utils.set_nested_path(target, "a.b", 9)
        dict_utils.set_nested_path(target, "items[0].x", 3)
        dict_utils.set_nested_path(target, "matrix[0][0]", {"y": 4})
        self.assertEqual(target["a.b"], 9)
        self.assertEqual(target["items"][0]["x"], 3)
        self.assertEqual(target["matrix"][0][0], {"y": 4})

        with self.assertRaises(IndexError):
            dict_utils.set_nested_path(target, "items[5].x", 1)
        with self.assertRaises(TypeError):
            dict_utils.set_nested_path({"items": ["s"]}, "items[0].x", 1)

    def test_set_proto_path_and_string_map_edge_cases(self):
        with self.assertRaisesRegex(TypeError, "Map entry 'k' in 'metadata'"):
            dict_utils.dict_to_struct(
                {"gceClusterConfig": {"metadata": {"k": None}}}, ClusterConfig
            )

        py_msg = orchestration_pipeline_pb2.PythonAction(name="p1")
        dict_utils.set_proto_path(py_msg, "op_kwargs.cursor", None)
        dict_utils.set_proto_path(py_msg, "op_kwargs.rows", 5)
        self.assertEqual(
            dict_utils.struct_to_dict(py_msg.op_kwargs),
            {"cursor": None, "rows": 5.0},
        )

        py_msg.depends_on.extend(["a", "b"])
        dict_utils.set_proto_path(py_msg, "depends_on[1]", "c")
        self.assertEqual(list(py_msg.depends_on), ["a", "c"])

        with self.assertRaisesRegex(TypeError, "Unsupported scalar type"):
            dict_utils.set_proto_path(py_msg, "trigger_rule", 1)

        dbt_msg = orchestration_pipeline_pb2.DbtFrameworkSpec(
            invocations=[orchestration_pipeline_pb2.DbtInvocation(command="run")]
        )
        with self.assertRaisesRegex(TypeError, "requires a subfield path"):
            dict_utils.set_proto_path(dbt_msg, "invocations[0]", "bad")

        sql_msg = orchestration_pipeline_pb2.SqlAction(name="s1")
        dict_utils.set_proto_path(sql_msg, "labels", {"a": 1, "b": True})
        dict_utils.set_proto_path(sql_msg, "labels.c", "v")
        self.assertEqual(
            dict(sql_msg.labels), {"a": "1", "b": "true", "c": "v"}
        )
        dict_utils.set_proto_path(py_msg, "op_kwargs", {"k": 2})
        dict_utils.set_proto_path(py_msg, "depends_on", ["u1", "u2"])
        self.assertEqual(list(py_msg.depends_on), ["u1", "u2"])

        retry = orchestration_pipeline_pb2.RetryPolicy()
        dict_utils.set_proto_path(retry, "max_retries", 3.0)
        dict_utils.set_proto_path(retry, "exponential_backoff.multiplier", 2.5)
        dict_utils.set_proto_path(
            retry, "exponential_backoff.randomize_jitter", True
        )
        self.assertEqual(retry.max_retries, 3)
        self.assertEqual(retry.exponential_backoff.multiplier, 2.5)
        self.assertTrue(retry.exponential_backoff.randomize_jitter)
        for path, bad in (
            ("max_retries", 1.5),
            ("max_retries", None),
            ("exponential_backoff.multiplier", "bad"),
            ("exponential_backoff.randomize_jitter", "yes"),
        ):
            with self.assertRaises(TypeError):
                dict_utils.set_proto_path(retry, path, bad)
        with self.assertRaises(TypeError):
            dict_utils.set_proto_path(sql_msg, "labels", "not-map")
        with self.assertRaises(TypeError):
            dict_utils.set_proto_path(py_msg, "depends_on", "not-list")
        self.assertIsNone(dict_utils.iso_to_timestamp_dict(""))
        self.assertEqual(
            dict_utils.iso_to_timestamp_dict("2026-01-01T00:00:10Z"),
            {"seconds": 1767225610},
        )


if __name__ == "__main__":
    unittest.main()
