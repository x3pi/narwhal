# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join
import os

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        # Lấy UDS socket path từ biến môi trường (mặc định nếu không có)
        uds_socket = os.environ.get('UDS_SOCKET_PATH', '/tmp/get_validator.sock_1')
        return (
            f'./node {v} run --keys {keys} --committee {committee} '
            f'--store {store} --parameters {parameters} '
            f'--uds-socket {uds_socket} primary'
        )

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        # Lấy UDS socket path từ biến môi trường (mặc định nếu không có)
        uds_socket = os.environ.get('UDS_SOCKET_PATH', '/tmp/get_validator.sock_1')
        return (
            f'./node {v} run --keys {keys} --committee {committee} '
            f'--store {store} --parameters {parameters} '
            f'--uds-socket {uds_socket} worker --id {id}'
        )

    @staticmethod
    def run_client(address, size, rate, nodes):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return f'./benchmark_client {address} --size {size} --rate {rate} {nodes}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'