"""Stream class for tap-workday."""

import base64
import json
from typing import cast, Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator

import logging
import requests
import xmltodict

logging.basicConfig(level=logging.INFO)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class TapWorkdayStream(RESTStream):
    """Workday stream class."""
    
    _LOG_REQUEST_METRIC_URLS: bool = True
    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://impl-services1.wd12.myworkday.com"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/xml"
        return headers

    @property
    def authenticator(self):
        http_headers = {}
        return SimpleAuthenticator(stream=self, auth_headers=http_headers)

class HumanResources(TapWorkdayStream):
    name = "humanresources" # Stream name 
    path = "/ccx/service/degreed_dpt1/Human_Resources/v39.2" # API endpoint after base_url 
    #primary_keys = ["id"]
    replication_key = None
    primary_keys = ["wd_Worker_ID"]
    records_jsonpath = "$[*].wd_Worker_Data"

    # Optional: If using schema_filepath, remove the propertyList schema method below
    schema = th.PropertiesList(

                th.Property("wd_Worker_ID", th.StringType),
                th.Property("wd_User_ID", th.StringType),
   

    ).to_dict()
    # Overwrite GET here by updating rest_method
    rest_method = "POST"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Define request parameters to return"""

        body = """
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:bsvc="urn:com.workday/bsvc">
                       <soapenv:Header>
                          <bsvc:Workday_Common_Header>
                             <bsvc:Include_Reference_Descriptors_In_Response>?</bsvc:Include_Reference_Descriptors_In_Response>
                          </bsvc:Workday_Common_Header>
                            <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                                <wsse:UsernameToken>
                                    <wsse:Username>{username}</wsse:Username>
                                    <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{password}</wsse:Password>
                                </wsse:UsernameToken>
                            </wsse:Security>
                       </soapenv:Header>
                       <soapenv:Body>
                          <bsvc:Get_Workers_Request bsvc:version="v39.2">
                             <bsvc:Request_Criteria>
                                <bsvc:Exclude_Inactive_Workers>true</bsvc:Exclude_Inactive_Workers>
                                <bsvc:Exclude_Employees>false</bsvc:Exclude_Employees>
                                <bsvc:Exclude_Contingent_Workers>false</bsvc:Exclude_Contingent_Workers>
                             </bsvc:Request_Criteria>
                                  <bsvc:Response_Filter>
                                    <bsvc:Page>1</bsvc:Page>
                                    <bsvc:Count>2</bsvc:Count>
                             </bsvc:Response_Filter>
                             <bsvc:Response_Group>
                                <bsvc:Include_Personal_Information>true</bsvc:Include_Personal_Information>
                                <bsvc:Include_Employment_Information>true</bsvc:Include_Employment_Information>
                                <bsvc:Include_User_Account>true</bsvc:Include_User_Account>
                             </bsvc:Response_Group>
                          </bsvc:Get_Workers_Request>
                       </soapenv:Body>
                    </soapenv:Envelope>

        """.format(username=self.config.get("username"),
                   password=self.config.get("password"))
        logging.info("##PR##: body")
        logging.info(body)

        
        return body

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    data=request_data,
                )
            ),
        )
        return request
    
    def replace_key_names(self, obj):
        if isinstance(obj, dict):
            return {key.replace(':', '_'): self.replace_key_names(val) for key, val in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_key_names(elem) for elem in obj]
        else:
            return obj
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        logging.info("##PR## response:")
        

        xml_str = response.text
        # XML to dict
        xml_dict = xmltodict.parse(xml_str)
        # dict to JSON
        json_response = json.dumps(xml_dict)
        json_obj = json.loads(json_response)
        new_json_obj = self.replace_key_names(json_obj)
        new_json_str = json.dumps(new_json_obj['env_Envelope']["env_Body"]["wd_Get_Workers_Response"]["wd_Response_Data"]["wd_Worker"])
        json_dict = json.loads(new_json_str)

        logging.info(json_dict)
        yield from extract_jsonpath(self.records_jsonpath, input=json_dict)
        #yield from extract_jsonpath(self.records_jsonpath, input=new_json_str)
    
