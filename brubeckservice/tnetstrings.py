# functions for creating/parsing tnetstrings fields
def t(text):
    """create a tnetstring field given the text"""
    return "%d:%s" % (len(str(text)), str(text))

def t_parse(field_text):
    """ parse a tnetstring field, and return any remainder
        field_value - a value in n:data format where n is the data length
            and data is the text to get the first n chars from
        returns the a tuple containing the value and whatever remains
    """
    #logging.debug("t_parse: %s" % field_text)
    field_data = field_text.split(':', 1)
    expected_len = int(field_data[0])
    #logging.debug("expected_len: %s" % expected_len)
    if expected_len > 0:
        field_value = field_data[1]
        value = field_value[0:expected_len]
    else:
        value = ''
    rest = field_value[expected_len:] if len(field_value) > expected_len else ''
    return (value, rest)
