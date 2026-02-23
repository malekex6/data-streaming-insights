import apache_beam as beam
from typing import Dict, Any
from datetime import datetime

def prepare_for_gold_write(record: Dict[str, Any]) -> Dict[str, Any]:
    
    date = record.get('event_time', '')
    if date:
        date = date.split(' ')[0]
    return {
        'date': date,
        'region': record.get('region', ''),
        'product_id': record.get('product_id', ''),
        'amount': record.get('amount', 0.0),
    }

class GoldAggregationFn(beam.CombineFn):
    def create_accumulator(self):
        return {'total_sales': 0.0, 'transaction_count': 0, 'amounts': []}

    def add_input(self, accumulator, input):
        accumulator['total_sales'] += input['amount']
        accumulator['transaction_count'] += 1
        accumulator['amounts'].append(input['amount'])
        return accumulator

    def merge_accumulators(self, accumulators):
        total_sales = sum(acc['total_sales'] for acc in accumulators)
        transaction_count = sum(acc['transaction_count'] for acc in accumulators)
        amounts = [amt for acc in accumulators for amt in acc['amounts']]
        return {'total_sales': total_sales, 'transaction_count': transaction_count, 'amounts': amounts}

    def extract_output(self, accumulator):
        avg_amount = (
            sum(accumulator['amounts']) / accumulator['transaction_count']
            if accumulator['transaction_count'] > 0 else 0.0
        )
        return {
            'total_sales': accumulator['total_sales'],
            'transaction_count': accumulator['transaction_count'],
            'avg_amount': avg_amount,
        }
