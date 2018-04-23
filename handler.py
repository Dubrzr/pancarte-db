import re


def handle_query(query):
    if re.match(pattern=' *SELECT +( *[\w|-]+ *, *)* *[\w|-]+ * +WHERE +\(( *( *[\w|-]+(?:==|>=|<=)[\w|-]+ *,)* *[\w|-]+(?:==|>=|<=)[\w|-]+)*\) *', string=query, flags=re.IGNORECASE):
        pass