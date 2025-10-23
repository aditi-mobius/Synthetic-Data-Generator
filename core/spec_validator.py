import json
from jsonschema import validate, ValidationError
from typing import Dict, Any, Tuple

def _validate_spec_dicts(spec: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Core validation logic that works on Python dictionaries.
    Returns (True, "") on success, (False, error_message) on failure.
    """
    try:
        validate(instance=spec, schema=schema)
        return True, ""
    except ValidationError as e:
        error_path = " -> ".join(map(str, e.path))
        return False, f"Validation error at '{error_path}': {e.message}"

def validate_spec(spec_path: str, schema_path: str) -> Tuple[bool, Any]:
    """
    Validate the JSON spec file at spec_path using the JSON schema at schema_path.
    Returns (True, spec_dict) on success, (False, error_message) on failure.
    """
    try:
        with open(spec_path, 'r') as f:
            spec = json.load(f)
        with open(schema_path, 'r') as f:
            schema = json.load(f)
    except FileNotFoundError as e:
        return False, f"File not found: {e.filename}"
    except json.JSONDecodeError as e:
        return False, f"Error decoding JSON in {e.doc.name if hasattr(e, 'doc') and hasattr(e.doc, 'name') else 'file'}: {e.msg} at line {e.lineno}, column {e.colno}"

    ok, error_message = _validate_spec_dicts(spec, schema)
    if ok:
        return True, spec
    else:
        return False, error_message
