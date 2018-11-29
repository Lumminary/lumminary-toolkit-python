import os
import base64
import binascii
import re
import json
from util import get_export_handler_class
from six import string_types

class Config:
    def __init__(self, path):
        self._path = path
        self._configExplicit = self.extract()
        self._clsCustomHandler = get_export_handler_class(self._configExplicit["export_handler"])
    
    def extract(self):
        if not os.path.isfile(self._path):
            raise IOError("No file found at {0}. Expecting a configuration json".format(self._path))
        
        with open(self._path) as fConfig:
            configExplicit = json.load(fConfig)
        
        return configExplicit

    def parse(self):
        configExplicit = self.fill_defaults(self._configExplicit)
        self.assert_config_valid(configExplicit.copy())

        return configExplicit

    def fill_defaults(self, configExplicit, configSchema = None):
        if configSchema is None:
            configSchema = self.get_config_schema()

        configUpdated = configExplicit.copy()
        for attributeName, attributeDetails in configSchema.items():
            if attributeName not in configUpdated:
                if "required" not in attributeDetails or attributeDetails["required"] != False:
                    raise ValueError("Expecting attribute {0} in {1}".format(attributeName, self._path))

                if "default" not in attributeDetails:
                    raise ValueError("Attribute {0} does not have a default value and is missing from {1}".format(attributeName, self._path))

                configUpdated[attributeName] = attributeDetails["default"]

        if "optional" in configSchema:
            configUpdated["optional"] = self.fill_defaults(
                configExplicit["optional"],
                configSchema["optional"]
            )

        return configUpdated
    
    def assert_config_valid(self, configWithDefaults):
        configSchema = self.get_config_schema()
        configOptional = configWithDefaults.pop("optional")
        configOptionalSchema = configSchema.pop("optional")

        for attributeName, schemaDetails in configSchema.items():
            configValue = configWithDefaults[attributeName]
            schemaDetails["validator"](configValue)
        
        for attributeName, schemaDetails in configOptionalSchema.items():
            configValue = configOptional[attributeName]
            schemaDetails["validator"](configValue)
    
    def get_config_schema(self):
        return {
            "api_key": {
                "validator": Config._validate_api_key,
                "required": True
            },
            "product_uuid": {
                "validator": Config._validate_product_uuid,
                "required": True
            },
            "api_host": {
                "validator": Config._validate_api_host,
                "required": False,
                "default": "https://api.lumminary.com/v1"
            },
            "output_root": {
                "validator": Config._validate_output_root,
                "required": True
            },
            "export_handler": {
                "validator": Config._validate_export_handler,
                "required": True
            },
            "optional": self._clsCustomHandler.get_config_optional_schema()
        }
    
    @classmethod
    def _validate_api_key(cls, apiKey):
        if not isinstance(apiKey, string_types):
            raise Exception("Expecting api_key attribute to be a string, {0} found for value {1}".format(
                type(apiKey), apiKey
            ))

        try:
            base64.decodestring(apiKey)
        except binascii.Error:
            raise Exception("Expecting a base64-encoded value in the api_key attribute, found {1}".format(
                apiKey
            ))

    @classmethod
    def _validate_product_uuid(cls, productUuid):
        if not re.match("^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$", productUuid):
            raise Exception("Expecting a product UUID with format '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$' got {0}".format(
                productUuid
            ))

    @classmethod
    def _validate_api_host(cls, apiBaseUrl):
        if not re.match("^https://(.)*\.lumminary\.com/v[0-9]+$", apiBaseUrl):
            raise Exception("Expecting a lumminary api endpoint at in api_host, got {0}".format(
                apiBaseUrl
            ))

    @classmethod
    def _validate_output_root(cls, outputRootDirPath):
        if not os.path.isdir(outputRootDirPath):
            raise Exception("Config attribute `output_root` invalid, expecting a directory at {0}".format(
                outputRootDirPath
            ))
    
    @classmethod
    def _validate_export_handler(cls, exportHandlerImport):
        scriptPath = os.path.dirname(os.path.realpath(__file__))
        handlersPath = os.path.join(scriptPath, "export_handler")

        exportHandlers = [f for f in os.listdir(handlersPath) if os.path.isfile(os.path.join(handlersPath, f))]
        exportHandlers = ['.'.join(handler.split(".")[:-1]) for handler in exportHandlers if not handler.endswith(".pyc")]

        exportHandlers.remove("__init__")
        exportHandlers.remove("export_handler_base")

        if exportHandlerImport not in exportHandlers:
            raise Exception("Invalid export handler `{0}`, possible values : {1}".format(exportHandlerImport, ",".join(exportHandlers)))

