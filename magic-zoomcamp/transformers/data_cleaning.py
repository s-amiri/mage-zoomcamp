import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
  name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


@transformer
def transform(data, *args, **kwargs):
    
    print(f"Preprocessing: rows with zero passengers = { data['passenger_count'].isin([0]).sum() }")
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    data.columns = data.columns.map(camel_to_snake)
    data = data[data['trip_distance'] > 0]

    return data[(data['passenger_count'] > 0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0
    assert output['trip_distance'].isin([0]).sum() == 0
    assert (~output['vendor_id'].isin([1,2])).sum() == 0
    
