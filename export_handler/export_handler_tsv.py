import os
import json
from export_handler.export_handler_base import ExportHandlerBase

class ExportHandlerTsv(ExportHandlerBase):
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

    @classmethod
    def get_config_optional_schema(cls):
        return {
            "dna_data_filename": {
                "validator": lambda datasetFilename: cls._validate_dna_data_filename(datasetFilename),
                "required": False,
                "default": None
            },
            "authorization_metadata_filename": {
                "validator": lambda authorizationMetadata: cls._validate_authorization_metadata_filename(authorizationMetadata),
                "required": False,
                "default": "authorization-metadata.json"
            }
        }

    def update_authorization_processed(self):
        # Do nothing for plain TSV export, to allow for time to process the authorized dataset
        return

    def authorization_metadata_path(self):
        return os.path.join(self._path, self._optional["authorization_metadata_filename"])
    
    def dna_data_path(self):
        datasetFilename = None
        if self._optional["dna_data_filename"] is not None:
            datasetFilename = self._optional["dna_data_filename"]
        else:
            datasetFilename = self._authorization.scopes.dataset

        return os.path.join(self._path, datasetFilename)
    
    @classmethod
    def _validate_dna_data_filename(cls, datasetFilename):
        pass
    
    @classmethod
    def _validate_authorization_metadata_filename(cls, authorizationMetadata):
        pass
