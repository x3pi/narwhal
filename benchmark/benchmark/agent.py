import math
import time
import requests
from re import search


class FakeAgent:
    def __init__(self, duration, bench_parameters, node_parameters, consensus_prometheus_addresses, workers_prometheus_addresses):
        self.duration = duration
        self.bench_parameters = bench_parameters
        self.node_parameters = node_parameters
        self.consensus_addresses = consensus_prometheus_addresses
        self.workers_addresses = workers_prometheus_addresses

        # Print out the addresses where the nodes expose the metrics and
        # accept new parameters.
        print()
        for i, address in enumerate(self.consensus_addresses):
            print(f'Metrics address of consensus {i}: {address}')
        for i, address in enumerate(self.workers_addresses):
            print(f'Metrics address of worker {i}: {address}')

    def compute_performance(self):
        # Query the state of the first consensus node.
        address = self.consensus_addresses[0]
        url = f'http://{address}/metrics'
        response = requests.get(url, ()).text

        # Compute throughput.
        start_time = int(
            search(r'first_sent_transaction (\d+)', response).group(1)
        )
        end_time = int(
            search(r'last_committed_transaction (\d+)', response).group(1)
        )
        committed_bytes = int(
            search(r'committed_bytes_total (\d+)', response).group(1)
        )

        bps = committed_bytes * 1_000 / (end_time-start_time)
        tps = bps / self.bench_parameters.tx_size

        # Compute latency.
        num_samples = int(
            search(r'committed_sample_transactions_total (\d+)', response).group(1)
        )
        total_time = int(
            search(r'latency_total (\d+)', response).group(1)
        )
        square_total_time = int(
            search(r'latency_square_total (\d+)', response).group(1)
        )

        latency_avg = total_time / num_samples
        latency_std = math.sqrt(
            square_total_time / num_samples - latency_avg**2
        )

        print()
        print(f'BPS: {round(bps):,} B/s')
        print(f'TPS: {round(tps):,} tx/s')
        print(f'Latency: {round(latency_avg):,} +/- {round(latency_std):,} ms')
        print()

    def run(self):

        # Run for 25% of the benchmark duration without changing the parameters.
        time.sleep(self.duration/4)

        # Compute performance.
        self.compute_performance()

        # Query the state of the workers and print the state of the first node.
        for i, address in enumerate(self.workers_addresses):
            if i == 0:
                url = f'http://{address}/metrics'
                x = requests.get(url, ())
                print(f'\nState of worker {i}:')
                print(x.text)

        # Update the parameters of all workers to divide the batch size by 2.
        new_batch_size = int(self.node_parameters.json['batch_size']) / 2
        new_parameters = {
            'header_size': self.node_parameters.json['header_size'],
            'max_header_delay': self.node_parameters.json['max_header_delay'],
            'sync_retry_delay': self.node_parameters.json['sync_retry_delay'],
            'sync_retry_nodes': self.node_parameters.json['sync_retry_nodes'],
            'batch_size': int(new_batch_size),
            'max_batch_delay': self.node_parameters.json['max_batch_delay']
        }

        print(f'\nSetting new parameters to: {new_parameters}')

        for i, address in enumerate(self.workers_addresses):
            url = f'http://{address}/parameters'
            x = requests.post(url, json=new_parameters)
            print(f'Worker {i} update status: {x.text}')
        print()

        # Run the rest of the benchmark. It is not important to precisely
        # respect the benchmark duration.
        time.sleep(3*self.duration/4)

        # Compute performance.
        self.compute_performance()
