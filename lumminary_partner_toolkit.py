from lumminary_sdk import LumminaryApi
from lumminary_sdk import Credentials
from config import Config
from util import get_export_handler_class
import os
import json
import argparse
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This tool pulls Lumminary authorizations data for an product")
    parser.add_argument(
        "-config-path",
        dest = "config_path",
        type = str,
        help = "Path to the json config for the Lumminary toolkit"
    )
    args = parser.parse_args()
    config = Config(args.config_path)
    objConfig = config.parse()

    logging.info("Connecting to the Lumminary API...")
    apiCredentials = Credentials(
        uuid = objConfig["product_uuid"],
        api_key = objConfig["api_key"],
        host = objConfig["api_host"]
    )
    api = LumminaryApi(apiCredentials)

    logging.info("Authenticated to the Lumminary api")

    authorizationsPending = api.get_authorizations_queue(objConfig["product_uuid"])
    logging.info("Fetched {0} authorizations".format(len(authorizationsPending)))

    exportHandlerClass = get_export_handler_class(objConfig["export_handler"])
    product = api.get_product(objConfig["product_uuid"])
    for authorization in authorizationsPending:
        logging.info("Processing authorization {0}".format(authorization.authorization_uuid))

        exportHandler = exportHandlerClass(objConfig["output_root"], authorization, product, api, objConfig["optional"])

        exportHandler.pull_authorization_data()
        exportHandler.update_authorization_processed()
