from app.kafka.raw_event_consumer import _defect_probability, _create_defect_transfer_detector, _predict_defect_transfer
from app.data_generation.manufacturing_event_json_builder import ManufacturingEventJsonBuilder

class Req:
    car_id_map = {1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7, 8:8, 9:9, 10:10}
builder = ManufacturingEventJsonBuilder()
events = list(builder.iter_rows(Req()))
assembly_events = [e for e in events if e['process_code'] == 'ASSEMBLY']

detector = _create_defect_transfer_detector()

for row in assembly_events[:10]:
    e = row['event_json']
    fallback_prob = _defect_probability(e, 'ASSEMBLY')
    
    pred = _predict_defect_transfer(detector, row)
    ml_prob = pred.defect_probability if pred else None
    
    print(f"Fallback: {fallback_prob}, ML: {ml_prob}")
