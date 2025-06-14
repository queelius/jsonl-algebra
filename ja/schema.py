def _get_type_name(value):
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "boolean"
    if isinstance(value, int): return "integer"
    if isinstance(value, float): return "float"
    if value is None: return "null"
    if isinstance(value, list): return "array" 
    if isinstance(value, dict): return "object"
    return type(value).__name__

def _schema_to_string_for_union(s_dict):
    if not isinstance(s_dict, dict): return str(s_dict)
    
    s_type = s_dict.get("type")
    if s_type == "object":
        return "object"
    elif s_type == "array":
        items = s_dict.get("items")
        if isinstance(items, dict):
            item_str = _schema_to_string_for_union(items)
            return f"array[{item_str}]"
        else:
            return f"array[{items or 'unknown'}]"
    return "complex_schema_dict"

def _merge_schemas(s1, s2):
    if s1 is None: return s2
    if s2 is None: return s1
    if s1 == s2: return s1

    s1_is_dict = isinstance(s1, dict)
    s2_is_dict = isinstance(s2, dict)

    if s1_is_dict and not s2_is_dict: # s2 is str
        s1_str = _schema_to_string_for_union(s1)
        types = sorted(list(set(s1_str.split(" | ")).union(str(s2).split(" | "))))
        return " | ".join(types)
    if not s1_is_dict and s2_is_dict: # s1 is str
        s2_str = _schema_to_string_for_union(s2)
        types = sorted(list(set(str(s1).split(" | ")).union(s2_str.split(" | "))))
        return " | ".join(types)

    if not s1_is_dict and not s2_is_dict: # Both are type strings
        types = sorted(list(set(str(s1).split(" | ")).union(str(s2).split(" | "))))
        return " | ".join(types)

    # Both are schema dicts
    if s1.get("type") == "object" and s2.get("type") == "object":
        merged_props = {}
        all_keys = set(s1.get("properties", {}).keys()).union(s2.get("properties", {}).keys())
        for key in all_keys:
            prop1 = s1.get("properties", {}).get(key)
            prop2 = s2.get("properties", {}).get(key)
            merged_props[key] = _merge_schemas(prop1, prop2)
        return {"type": "object", "properties": merged_props}
    
    elif s1.get("type") == "array" and s2.get("type") == "array":
        merged_items = _merge_schemas(s1.get("items"), s2.get("items"))
        return {"type": "array", "items": merged_items}
    else: # Mixed dict types (e.g. object and array), or other unhandled dicts
        s1_str = _schema_to_string_for_union(s1)
        s2_str = _schema_to_string_for_union(s2)
        types = sorted(list(set(s1_str.split(" | ")).union(s2_str.split(" | "))))
        return " | ".join(types)

def _infer_value_schema(value):
    type_name = _get_type_name(value)
    if type_name == "object":
        return _infer_relation_schema_from_records([value]) 
    elif type_name == "array":
        if not value:
            return {"type": "array", "items": "unknown"}
        item_schema = None
        for item in value:
            current_item_schema = _infer_value_schema(item)
            item_schema = _merge_schemas(item_schema, current_item_schema)
        return {"type": "array", "items": item_schema or "unknown"}
    else:
        return type_name

def _infer_relation_schema_from_records(relation_records):
    properties = {}
    if not relation_records: # Handles empty list or list with non-dict if we filter them
        return {"type": "object", "properties": {}}

    for record in relation_records:
        if not isinstance(record, dict):
            continue 
        for key, value in record.items():
            value_schema = _infer_value_schema(value)
            properties[key] = _merge_schemas(properties.get(key), value_schema)
    
    return {"type": "object", "properties": properties}

def infer_schema(relation_data):
    if not isinstance(relation_data, list):
        if isinstance(relation_data, dict):
            relation_data = [relation_data]
        else:
            return {"error": "Input must be a list of objects or a single object."}
            
    if not relation_data:
        return {}

    master_properties = {}
    for record in relation_data:
        if not isinstance(record, dict):
            # Log or handle non-dictionary items at the top level of the relation
            continue 
        
        # Infer schema for the current record's properties
        # _infer_relation_schema_from_records expects a list of records
        record_object_schema = _infer_relation_schema_from_records([record])
        current_record_properties = record_object_schema.get("properties", {})
        
        for key, prop_schema_for_key_in_record in current_record_properties.items():
            master_properties[key] = _merge_schemas(master_properties.get(key), prop_schema_for_key_in_record)
            
    return master_properties