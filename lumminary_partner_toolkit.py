from lumminary_sdk import LumminaryApi
from lumminary_sdk import Credentials
from config import Config
from util import get_export_handler_class
import os
import json
import argparse
import logging
import sys

if sys.version_info.major == 3:
    from importlib import reload

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Overwrite default encoding globally for the current python2.7 (only) process, to allow non-ascii reports
if sys.version_info.major == 2:
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
    if not os.path.isdir(authorizationReportsBasePath):
        raise Exception("Expecting reports directory at {0}".format(authorizationReportsBasePath))

    reportsCreated = []
    resultFilePath = os.path.join(authorizationReportsBasePath, "result.json")

    if os.path.isfile(resultFilePath):
        with open(resultFilePath, "r") as f_result:
            objResult = json.load(f_result)

        if "credentials" in objResult:
            logging.info("Uploading credentials for authorization {0}".format(authorizationUuid));

            if "url"in objResult["credentials"]:
                reportsCreated.append(api.post_authorization_result_credentials(
                    productUuid,
                    authorizationUuid,
                    report_url = objResult["credentials"]["url"],
                    credentials_username = objResult["credentials"]["username"],
                    credentials_password = objResult["credentials"]["password"]
                ))
            else:
                raise Exception("Expected required 'url' attribute in the 'credentials' object at {0}".format(resultFilePath))
        elif "physical_product" in objResult:
            logging.info("Uploading order dispatched report for authorization {0}".format(authorizationUuid))

            if "physical_product_completed" in objResult["physical_product"] and objResult["physical_product"]["physical_product_completed"]:
                reportsCreated.append(api.post_product_authorization(
                    productUuid,
                    authorizationUuid
                ))
            else:
                raise Exception("Expecting 'physical_product_completed' attribute under 'physical_product' in {0}".format(resultFilePath))
        elif "unfulfillable" in objResult:
            logging.info("Uploading error report for authorization {0}".format(authorizationUuid))

            if "error" in objResult["unfulfillable"]:
                reportsCreated.append(api.post_product_authorization_unfulfillable(productUuid, authorizationUuid))
            else:
                raise Exception("Expecting 'error' attribute under 'unfulfillable' in {0}".format(resultFilePath))
        else:
            raise Exception("Unexpected reports object format {0} in {1}".format(json.dumps(objResult), resultFilePath))    
    else:
        authorizationReportFiles = os.listdir(authorizationReportsBasePath)
        logging.info("Uploading {0} report files for authorization...{1}".format(
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

    return reportsCreated

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This tool pulls Lumminary authorizations data for an product")
    parser.add_argument(
        "--config-path",
        dest = "config_path",
        type = str,
        help = "Path to the json config for the Lumminary toolkit"
    )
    args = parser.parse_args()
    config = Config(args.config_path)
    objConfig = config.parse()

    logging.info("Connecting to the Lumminary api on {0} as product {1}".format(objConfig["api_host"], objConfig["product_uuid"]))
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
                logging.error("Unable to pull data for authorization {0} : {1}".format(authorization.authorization_uuid, str(pullDatasetException)))
