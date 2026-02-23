import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from transforms import deduplicate

def run_test():
    test_data = [
        ({'transaction_id': 'A', 'amount': 100}, 'msg1', True),
        ({'transaction_id': 'B', 'amount': 200}, 'msg2', True),
        ({'transaction_id': 'A', 'amount': 100}, 'msg3', True),  # Duplicate
        ({'transaction_id': 'C', 'amount': 300}, 'msg4', True),
        ({'transaction_id': 'B', 'amount': 200}, 'msg5', True),  # Duplicate
    ]

    with beam.Pipeline(options=PipelineOptions()) as p:
        input_pcoll = p | 'CreateTestInput' >> beam.Create(test_data)
        deduped = deduplicate.deduplicate_on_transaction_id_transform(input_pcoll)
        deduped | 'PrintOutput' >> beam.Map(print)

if __name__ == '__main__':
    run_test()