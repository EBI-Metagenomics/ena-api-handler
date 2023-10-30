import logging

def check_fields(list_of_fields, result):
    for field in list_of_fields:
        if field not in result:
            logging.info("Missing in result:", field)
            return False
    context = False
    for field in result:
        if field not in list_of_fields:
            logging.info("Missing in list", field)
            if result[field] != "":
                context = True
    if not context:
        logging.info("All returned fields are empty")
    return True
