from aims_soap_client import get_aims_client

client = get_aims_client()
client._init_client()

print("--- Searching for ExemptSBY in all types ---")
for type_obj in client._client.wsdl.types.types:
    if hasattr(type_obj, 'elements'):
        for name, element in type_obj.elements:
            if 'ExemptSBY' in name:
                print(f"Found ExemptSBY in type: {type_obj.name}")
                print(f"  Element: {name} (Type: {element.type})")
    if hasattr(type_obj, 'attributes'):
        for name, attr in type_obj.attributes:
            if 'ExemptSBY' in name:
                print(f"Found ExemptSBY in attribute: {type_obj.name}")
                print(f"  Attribute: {name} (Type: {attr.type})")
