from dataclasses import dataclass
from enum import Enum
from typing import Any
import json
import requests
import time
import urllib.parse

# Toggle to print debug output
debug_output: bool = False


# Print some thing if debug output is enabled
def debug_print(thing: Any) -> None:
    if debug_output:
        print(thing)


# Print the requests module status codes in a readable format
def dump_status_codes() -> None:
    for code_name, code_value in requests.codes.__dict__.items():
        print(f"{code_name}: {code_value}")


# Find the message corresponding to the given requests module status code
def lookup_status_code(code: int) -> str:
    for key, value in requests.codes.__dict__.items():
        if value == code:
            return f"Status: {key}, Code: {code}"
    return f"Error: Unrecognized status code {code}."


# Define some CoFlow session status codes found within response.json()
# ["statusCode"]
class CoFlowSessionStatus(Enum):
    Unknown = 0
    Pending = 100
    Running = 200
    Failed = 300

    def __eq__(self, other):
        if isinstance(other, int):
            return self.value == other
        return super().__eq__(other)


# Find the message corresponding to the given CoFlow service status code
def lookup_coflow_status_code(code: int) -> str:
    try:
        status = CoFlowSessionStatus(code)
        return f"Status: {status.name}, Code: {status.value}"
    except ValueError:
        return f"Error: Unrecognized status code {code}."


@dataclass
class AuthError(Exception):
    message: str


@dataclass
class SessionError(Exception):
    message: str


@dataclass
class CoFlowError(Exception):
    message: str


@dataclass
class Scalar:
    value: float
    unit: str

    def __str__(self):
        return f"{self.value} {self.unit}"

    def __repr__(self):
        return self.__str__()


@dataclass
class SteadyState:
    steady_state_url: str
    api_key: str

    def run(self, values) -> None:
        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.post(
            self.steady_state_url + "/run", json=values, headers=headers
        )
        if response.status_code != requests.codes.ACCEPTED:  # 202
            raise SessionError(
                f"{response.text}: {lookup_status_code(response.status_code)}"
            )

        print(response.text)
        done: bool = False
        while not done:
            response: requests.Response = requests.get(
                self.steady_state_url + "/run", headers=headers
            )
            if response.status_code != requests.codes.OK:  # 200
                debug_print(response.status_code)
                raise SessionError(
                    f"{response.text}: {lookup_status_code(response.status_code)}"
                )
            status: dict = response.json()
            status_code: str = status["status"]
            status_msg: str = status["message"]
            print(status_code)
            if status_code == "Succeeded" or status_code == "Failed":
                done = True
                print(status_msg)
            else:
                time.sleep(1)

    def query(self, request) -> dict:
        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.post(
            self.steady_state_url + "/query", json=request, headers=headers
        )
        if response.status_code != requests.codes.OK:  # 200
            error_text = f"{response.text}: {lookup_status_code(response.status_code)}"
            debug_print(error_text)
            raise SessionError(error_text)
        return response.json()


@dataclass
class CoFlow:
    url: str
    api_key: str

    def open_steady_state(
        self, path: str, cores: int = 1, mem: int = 1024
    ) -> SteadyState:
        print(self.url + "/sessions")
        session_request: dict = {"resources": {
            "coreCount": cores, "memoryMiB": mem}}

        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.post(
            self.url + "/sessions", json=session_request, headers=headers
        )
        if response.status_code == requests.codes.UNAUTHORIZED or response.status_code == 405:
            raise AuthError(
                "401 UNAUTHORIZED error: try updating your API Key")
        elif response.status_code != requests.codes.ACCEPTED:  # 202
            error_text = f"{response.text}: {lookup_status_code(response.status_code)}"
            debug_print(error_text)
            raise SessionError(
                f"Failed to create steady state session: {error_text}")
        # location contains /sessions/{session_id}/status
        location: str = response.headers.get("Location")
        print(f"Location: {location}")

        # poll for status
        while True:
            response: requests.Response = requests.get(
                self.url + location, headers=headers
            )
            if response.status_code != requests.codes.OK:  # 200
                raise SessionError(
                    f"Cannot get session status: {lookup_status_code(response.status_code)}"
                )

            coflow_status: int = response.json()["statusCode"]
            if coflow_status == CoFlowSessionStatus.Pending:  # 100
                print("Provisioning compute...")
                time.sleep(1)
                continue
            elif coflow_status == CoFlowSessionStatus.Running:  # 200
                break
            elif coflow_status == CoFlowSessionStatus.Failed:  # 300
                raise CoFlowError(
                    f"Failed to create session: {lookup_coflow_status_code(coflow_status)}"
                )
            else:
                raise CoFlowError(
                    f"Unknown error: {lookup_coflow_status_code(coflow_status)}"
                )

        # url/sessions/{session}/current/v1/steady-state
        ss_url: str = (
            self.url + location.replace("/status", "") +
            "/current/api/v1/steady-state"
        )

        # create CoFlow session init request
        print("Initializing steady state model...")
        # We wait for short period of time to ensure CoFlow engine
        # is ready
        time.sleep(3)
        init_request: dict = self.create_init_request(path)
        session_url: str = ss_url + "/session"
        debug_print(session_url)
        init_json: str = json.dumps(init_request, indent=4)
        debug_print(init_json)
        debug_print(init_request)
        response: requests.Response = requests.post(
            session_url, json=init_request, headers=headers
        )
        debug_print(response.status_code)
        if response.status_code != requests.codes.CREATED:
            self._close_state_state_session(session_url)  # 201
            debug_print(response)
            raise SessionError(
                f"Failed to initialize steady state model: {lookup_status_code(response.status_code)}"
            )
        print("Initialization complete")

        # return steady-state client
        return SteadyState(ss_url, self.api_key)

    def create_init_request(self, path: str) -> dict:
        try:
            pathAsUrl: urllib.parse.SplitResult = urllib.parse.urlsplit(path)
            components: list = pathAsUrl.path.split("/")
            repo: str = pathAsUrl.netloc
            project: str = components[1]
            study: str = components[2]
            target_case: str = components[3]
        except Exception as ex:
            print(
                f"Exception while extracting repo/project/study: {ex.__class__.__name__} - {ex}"
            )
            raise

        init_request: dict = {
            "initializationInfo": {
                "repositoryName": repo,
                "projectName": project,
                "studyName": study,
            },
            "editableInfo": {"systemOfUnits": "SI", "caseName": target_case},
        }
        return init_request

    def open_steady_state_local(self, path):
        init_request = self.create_init_request(path)
        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.post(
            "http://localhost:12500/api/v1/steady-state/session",
            json=init_request,
            headers=headers,
        )
        if response.status_code != requests.codes.CREATED:  # 201
            raise SessionError(
                f"{response.text}: {lookup_status_code(response.status_code)}"
            )
        return SteadyState("http://localhost:12500/api/v1/steady-state")

    def close_steady_state(self, ss):
        url = ss.steady_state_url + "/session"
        debug_print(url)
        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.delete(url, headers=headers)
        if response.status_code != requests.codes.ACCEPTED:  # 202
            debug_print(response.status_code)
            raise SessionError(
                f"{response.text}: {lookup_status_code(response.status_code)}"
            )
        print("Steady State session closed.")

    def _close_state_state_session(self, session_url):
        headers: dict = {"Authorization": f"Bearer {self.api_key}"}
        response: requests.Response = requests.delete(
            session_url,
            headers=headers
        )
        if response.status_code != response.codes.OK:
            print(response)


class SteadyStateModel:
    def __init__(self, client, model_path, input_defs, output_defs):
        self.client = client
        self.model_path = model_path
        self.input_defs = input_defs
        self.output_defs = output_defs

    def run(self, input_values):
        input_prop_payload = []
        for alias, prop in self.input_defs.items():
            input_value = input_values[alias]
            payload = {
                "propertyPath": prop["path"],
                "unit": prop["unit"],
                "value": input_value,
            }
            input_prop_payload.append(payload)

        output_prop_payload = []
        output_lookup = {}
        for key, prop in self.output_defs.items():
            payload = {
                "propertyPath": prop["path"],
                "unit": prop["unit"]
            }
            output_prop_payload.append(payload)
            output_lookup[prop["path"]] = (key, prop)

        try:
            model = self.client.open_steady_state(path=self.model_path)
            model.run({"properties": input_prop_payload})
            model_output = model.query({"properties": output_prop_payload})
            output = {}
            for output_value in model_output["properties"]:
                path = output_value["propertyPath"]
                (alias, prop) = output_lookup[path]
                # We assume CoFlow will return data in expected units
                # no check is performed here to make sure that this matches
                value = output_value["value"]
                output[alias] = value
            return output

        finally:
            self.client.close_steady_state(model)
