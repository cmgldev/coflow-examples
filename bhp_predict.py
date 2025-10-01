import configparser
import os

import coflow

# A local file called bhp_predict.ini contains the
# bootstrapping information need to run the model
config = configparser.ConfigParser()
config.read("bhp_predict.txt")
service = config["service"]
tenant_url = service["tenant_url"].strip('"\'')
project = service["project"].strip('"\'')
release = service["release"].strip('"\'')
model_path = service["model_path"].strip('"\'')

# Obtain a CoFlow API key from your tenant landing page
# Login here and select click the "Create Token" button. By default, tokens
# are time limited to 60 minutes. Set the COFLOW_API_KEY
# environment variable using the generated token. Examples
# export COFLOW_API_KEY=<generated-token> Linux / bash
# $env:COFLOW_API_KEY="<generated-token>" Windows / Powershell
api_key = os.getenv("COFLOW_API_KEY")
if not api_key:
    print("COFLOW_API_KEY not set. Exiting")
    exit(1)

# The following URL is associated with your tenant, project and given release.
# Note that no compute resources are provisioned here - this happens later.
client = coflow.CoFlow(
    f"{tenant_url}/api/projects/{project}/releases/{release}",
    api_key)

# Values in CoFlow can be get/set using a URL like
# path (called soft references).
network_prefix = "Networks/Well Model/NetworkObjects"
input_defs = {
    "gas_rate": {
        "path": f"{network_prefix}/Gas/Outlet/Gas Rate SC Inst",
        "unit": "MMSCF/day"
    },
    "oil_rate": {
        "path": f"{network_prefix}/Oil/Outlet/Oil Rate SC Inst",
        "unit": "STB/day",
    },
    "water_rate": {
        "path": f"{network_prefix}/Water/Outlet/Water Rate SC Inst",
        "unit": "STB/day",
    },
    "outlet_pressure": {
        "path": f"{network_prefix}/Pipe/Outlet/Pressure",
        "unit": "psi",
    }
}

output_defs = {
    "bhp": {
        "path": f"{network_prefix}/bhp/Outlet/Pressure",
        "unit": "psi",
    },
}

model = coflow.SteadyStateModel(
    client,
    model_path,
    input_defs,
    output_defs,
)

# We can now set some actual input values based on the tags and using
# the units of measure that we defined above.
input = {
    "oil_rate": 435.394,
    "gas_rate": 2.59,
    "water_rate": 3418.68,
    "outlet_pressure": 2667.761,
}

output = model.run(input)
print(f"Predicted BHP: {output['bhp']:.2f} psi")
