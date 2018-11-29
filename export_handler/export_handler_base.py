import os
import abc

class ExportHandlerBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, outputRoot, authorization, product, api, optional = None):
        self._outputRoot = outputRoot
        self._authorization = authorization
        self._product = product
        self._api = api
        self._optional = optional

        self._path = os.path.join(self._outputRoot, authorization.authorization_uuid)

    def pull_authorization_data(self):
        os.mkdir(self._path)

        authorizationPath = self.save_authorization()

        dnaDataPath = self.save_dataset()

        objAuthorizationData = {
            "authorization_metadata_path": authorizationPath,
            "authorization_dna_data_path": dnaDataPath
        }

        return objAuthorizationData

    @classmethod
    def get_config_optional_schema(cls):
        return {}

    @abc.abstractmethod
    def save_dataset(self):
        return

    @abc.abstractmethod
    def save_authorization(self):
        return

    @abc.abstractmethod
    def authorization_metadata_path(self):
        return
    
    @abc.abstractmethod
    def dna_data_path(self):
        return
    
    @abc.abstractmethod
    def update_authorization_processed(self):
        return
