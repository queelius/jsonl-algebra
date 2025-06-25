def get_json_type(value):
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "boolean"
    if isinstance(value, int): return "integer"
    if isinstance(value, float): return "number"
    if value is None: return "null"
    if isinstance(value, list): return "array"
    if isinstance(value, dict): return "object"
    return "unknown"

def merge_schemas(s1, s2):
    if s1 is None: return s2
    if s2 is None: return s1
    if s1 == s2: return s1

    # Merge types
    type1 = s1.get('type', [])
    if not isinstance(type1, list): type1 = [type1]
    type2 = s2.get('type', [])
    if not isinstance(type2, list): type2 = [type2]
    
    merged_types = sorted(list(set(type1) | set(type2)))
    if 'integer' in merged_types and 'number' in merged_types:
        merged_types.remove('integer')

    merged_schema = {}
    if len(merged_types) == 1:
        merged_schema['type'] = merged_types[0]
    else:
        merged_schema['type'] = merged_types

    # If both schemas could be objects, merge properties
    if 'object' in type1 and 'object' in type2:
        props1 = s1.get('properties', {})
        props2 = s2.get('properties', {})
        all_keys = set(props1.keys()) | set(props2.keys())
        merged_props = {key: merge_schemas(props1.get(key), props2.get(key)) for key in all_keys}
        if merged_props:
            merged_schema['properties'] = merged_props

    # If both schemas could be arrays, merge items
    if 'array' in type1 and 'array' in type2:
        items1 = s1.get('items')
        items2 = s2.get('items')
        merged_items = merge_schemas(items1, items2)
        if merged_items:
            merged_schema['items'] = merged_items
            
    return merged_schema

def infer_value_schema(value):
    type_name = get_json_type(value)
    schema = {'type': type_name}
    if type_name == 'object':
        schema['properties'] = {k: infer_value_schema(v) for k, v in value.items()}
    elif type_name == 'array':
        if value:
            item_schema = None
            for item in value:
                item_schema = merge_schemas(item_schema, infer_value_schema(item))
            if item_schema:
                schema['items'] = item_schema
    return schema

def add_required_fields(schema, data_samples):
    """Add required fields to a schema based on data samples"""
    if schema.get('type') == 'object' and 'properties' in schema:
        # For object schemas, find fields present in all samples
        dict_samples = [s for s in data_samples if isinstance(s, dict)]
        if dict_samples:
            required_keys = set(dict_samples[0].keys())
            for sample in dict_samples[1:]:
                required_keys.intersection_update(sample.keys())
            if required_keys:
                schema['required'] = sorted(list(required_keys))
        
        # Recursively add required fields to nested object properties
        for prop_name, prop_schema in schema['properties'].items():
            prop_samples = [s.get(prop_name) for s in dict_samples if prop_name in s]
            if prop_samples:
                add_required_fields(prop_schema, prop_samples)
    
    elif schema.get('type') == 'array' and 'items' in schema:
        # For array schemas, collect all array items and add required fields
        array_items = []
        for sample in data_samples:
            if isinstance(sample, list):
                array_items.extend(sample)
        if array_items:
            add_required_fields(schema['items'], array_items)

def infer_schema(data):
    records = list(data)
    if not records:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
        }

    # Infer schema for each record
    inferred_schemas = [infer_value_schema(rec) for rec in records]

    # Merge all inferred schemas
    merged_schema = None
    for s in inferred_schemas:
        merged_schema = merge_schemas(merged_schema, s)

    # Add required fields recursively
    if merged_schema:
        add_required_fields(merged_schema, records)

    # Add the meta-schema URL
    final_schema = {"$schema": "http://json-schema.org/draft-07/schema#"}
    if merged_schema:
        final_schema.update(merged_schema)
    
    return final_schema