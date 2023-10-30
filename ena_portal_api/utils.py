def get_default_connection_headers():
    return {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }
    }

def get_default_params():
    return {"format": "json", "includeMetagenomes": True, "dataPortal": "metagenome"}

def run_filter(d):
    return d["library_strategy"] != "AMPLICON"
