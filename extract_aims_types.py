from aims_soap_client import get_aims_client
import zeep

client = get_aims_client()
client._init_client()

print("--- Types in WSDL ---")
# Use the publicized way to list types
for type_obj in client._client.wsdl.types.types:
    t_name = str(type_obj)
    if 'TAIMSCrewInfo2' in t_name or 'TAIMSCrewQual' in t_name or 'TAIMSGetCrewItm' in t_name:
        print(f"\nType: {t_name}")
        try:
            # Inspection depends on the type of node
            if hasattr(type_obj, 'elements'):
                for name, element in type_obj.elements:
                     print(f"  Element: {name} ({element.type})")
            if hasattr(type_obj, 'attributes'):
                for name, attr in type_obj.attributes:
                     print(f"  Attribute: {name} ({attr.type})")
        except Exception as e:
            print(f"  Error inspecting: {e}")

# Alternative: list all available services and their types
print("\n--- Services & Operations ---")
for service in client._client.wsdl.services.values():
    for port in service.ports.values():
        for operation in port.binding._operations.values():
            print(f"Op: {operation.name}")
            # print(f"  Input: {operation.input.body.type}")
            # print(f"  Output: {operation.output.body.type}")
