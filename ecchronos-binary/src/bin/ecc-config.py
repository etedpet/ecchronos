#!/bin/sh
''''exec python -B -- "$0" ${1+"$@"} # '''
# vi: syntax=python
#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from argparse import ArgumentParser
import os
import sys
try:
    from ecchronoslib import rest, table_formatter
except ImportError:
    script_dir = os.path.dirname(__file__)
    lib_dir = os.path.join(script_dir, "..", "pylib")
    sys.path.append(lib_dir)
    from ecchronoslib import rest, table_formatter


def convert_config(config):
    entry = list()
    entry.append(config.id)
    entry.append(config.keyspace)
    entry.append(config.table)
    entry.append(config.get_repair_interval())
    entry.append(config.repair_parallelism)
    entry.append(config.repair_unwind_ratio)
    entry.append(config.get_repair_warning_time())
    entry.append(config.get_repair_error_time())

    return entry


def print_table_config(config_data):
    config_table = list()
    config_table.append(["Id", "Keyspace", "Table", "Interval", "Parallelism", "Unwind ratio", "Warning time", "Error time"])
    if type(config_data) is list:
        sorted_config_data = sorted(config_data, key=lambda config: (config.keyspace, config.table))
        for config in sorted_config_data:
            if config.is_valid():
                config_table.append(convert_config(config))
    elif config_data.is_valid():
        config_table.append(convert_config(config_data))

    table_formatter.format_table(config_table)

def parse_arguments():
    parser = ArgumentParser(description='Show repair configuration')
    parser.add_argument('keyspace', nargs='?',
                        help='show config for a specific keyspace')
    parser.add_argument('table', nargs='?',
                        help='show config for a specific table')
    parser.add_argument('-i', '--id', type=str,
                        help='show config for a specific job')
    parser.add_argument('-u', '--url', type=str,
                        help='The host to connect to with the format (http://<host>:port)',
                        default=None)
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    request = rest.RepairConfigRequest(base_url=arguments.url)

    if arguments.id:
        if arguments.keyspace or arguments.table:
            print("id must be specified alone")
            exit(1)
        else:
            result = request.get(id=arguments.id)
    else:
        result = request.list(keyspace=arguments.keyspace, table=arguments.table)

    if result.is_successful():
        print_table_config(result.data)
    else:
        print(result.format_exception())


if __name__ == "__main__":
    main()
