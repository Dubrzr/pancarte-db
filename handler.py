import re


def handle_query(query):
    if re.match(pattern='SELECT \* WHERE \(([\w|-]+(?:==|>=|<=)[\w|-]+,?)*\)', string=query, flags=re.IGNORECASE):

    elif re.match(pattern='INSERT ')
    return query.upper()
