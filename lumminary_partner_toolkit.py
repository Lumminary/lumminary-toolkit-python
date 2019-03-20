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

# Overwrite default encoding globally for the current python2.7 (only) process, to allow non-ascii reports
reload(sys)
if sys.getdefaultencoding() == "ascii":
    sys.setdefaultencoding('latin-1')

def rrm(dirPath):
    if not os.path.isdir(dirPath):
        raise Exception("{0} is not a directory".format(dirPath))
    
    for dirChild in  os.listdir(dirPath):
        childPath = os.path.join(dirPath, dirChild)

        if os.path.isdir(childPath):
            rrm(childPath)
        elif os.path.isfile(childPath):
            os.unlink(childPath)
        else:
            raise Exception("Unexpected file type for {0}".format(childPath))
    
    os.rmdir(dirPath)

def post_reports(authorizationUuid, productUuid, authorizationReportsBasePath, logging, api):
    reportsCreated = []

    if os.path.isdir(authorizationReportsBasePath):
        authorizationReportFiles = os.listdir(authorizationReportsBasePath)
        logging.info("Uploading {0} report files for authorization {1}".format(
            len(authorizationReportFiles),
            authorizationUuid
        ))

        for reportFilename in authorizationReportFiles:
            reportPath = os.path.join(authorizationReportsBasePath, reportFilename)

            reportCreated = api.post_authorization_result_file(
                objConfig["product_uuid"],
                authorizationUuid,
                file_report=reportPath,
                original_filename=reportFilename
            )
            reportsCreated.append(reportCreated)

            logging.info("Done uploading reports for authorization {0}".format(authorizationUuid))

        try:
            rrm(authorizationBasePath)
        except Exception as authorizationCleanupException:
            raise Exception("Unable to cleanup authorization {0}. {1}".format(
                authorizationUuid,
                str(authorizationCleanupException)
            ))
    else:
        logging.info("No reports directory found for authorization {0}".format(authorizationUuid))

    return reportsCreated

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
        login = objConfig["product_uuid"],
        api_key = objConfig["api_key"],
        host = objConfig["api_host"]
    )
    api = LumminaryApi(apiCredentials)

    logging.info("Authenticated to the Lumminary api")

    exportHandlerClass = get_export_handler_class(objConfig["export_handler"])
    product = api.get_product(objConfig["product_uuid"])

    if "push_reports" in objConfig["operations"]:
        try:
            for authorizationUuid in os.listdir(objConfig["output_root"]) :
                authorizationBasePath = os.path.join(objConfig["output_root"], authorizationUuid)
                authorizationReportsBasePath = os.path.join(authorizationBasePath, "reports")

                arrReportsCreated = post_reports(authorizationUuid, objConfig["product_uuid"], authorizationReportsBasePath, logging, api)
        except Exception as pushReportException:
            logging.error(pushReportException)

    if "pull_datasets" in objConfig["operations"]:
        authorizationsPending = api.get_authorizations_queue(objConfig["product_uuid"])
        logging.info("Fetched {0} authorizations".format(len(authorizationsPending)))

        for authorization in authorizationsPending:
            try:
                logging.info("Processing authorization {0}".format(authorization.authorization_uuid))

                exportHandler = exportHandlerClass(objConfig["output_root"], authorization, product, api, objConfig["optional"])
                if not exportHandler.should_pull_atuhorization():
                    logging.info("Skipping authorization {0} because Authorization data directory already exists".format(authorization.authorization_uuid))
                    continue

                exportHandler.pull_authorization_data()
                exportHandler.update_authorization_processed()
            except Exception as pullDatasetException:
                logging.error("Unable to pull data for authorization {0} : {1}".format(authorization["authorization_uuid"], str(pullDatasetException)))
