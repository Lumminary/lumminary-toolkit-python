import importlib

def get_export_handler_class(exportHandler):
    """
    @param  str exportHandler   export_handler_tsv
    """
    module = importlib.import_module("export_handler.{0}".format(exportHandler))
    
    exportHandlerClassname = "".join([
        exportHandlerPart[:1].upper() + exportHandlerPart[1:]
        for exportHandlerPart in exportHandler.split("_")
    ])

    module = vars(module)[exportHandlerClassname]
    return module
