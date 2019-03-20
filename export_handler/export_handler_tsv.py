import os
import json
from export_handler.export_handler_base import ExportHandlerBase

class ExportHandlerTsv(ExportHandlerBase):
    FILENAME_DNA_DATA = "dna-data.tsv"
    FILENAME_AUTHORIZATION_METADATA = "authorization.json"

    def save_authorization(self):
        authorizationMetadataPath = self.authorization_metadata_path()
        authorizationMetadata = self._api.authorization_metadata(self._authorization.authorization_uuid)

        with open(authorizationMetadataPath, "w") as authorizationMetadataFile:
            json.dump(authorizationMetadata, authorizationMetadataFile, sort_keys=True, indent=4, separators=(',', ': '))
        
        return authorizationMetadataPath

    def save_dataset(self):
        datasetPath = self.dna_data_path()

        authorizationDnaData = self._api.authorization_dna_data(self._authorization.authorization_uuid)

        tmpDatasetPath = datasetPath + "_tmp"
        with open(tmpDatasetPath, "w") as datasetFile:
            datasetFile.write("\n".join(authorizationDnaData))

        os.rename(tmpDatasetPath, datasetPath)

        return datasetPath

    def update_authorization_processed(self):
        return

    def authorization_metadata_path(self):
        return os.path.join(self._path, ExportHandlerTsv.FILENAME_AUTHORIZATION_METADATA)
    
    def dna_data_path(self):
        return os.path.join(self._path, ExportHandlerTsv.FILENAME_DNA_DATA)
