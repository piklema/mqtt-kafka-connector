import datetime
import re
from json import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class Template:
    MASK_REGEXP = r"{(?P<tpl_name>\w+)}"

    def __init__(self, src_tpl: str):
        self.src_tpl = src_tpl  # Исходный шаблон

    def tpl_to_regex(self, tpl: str) -> str:
        """Заменить шаблоны вида {маска} на именованные регулярные выражения"""
        return re.sub(self.MASK_REGEXP, r"(?P<\g<tpl_name>>.+)", tpl)

    def to_topic(self, wildcard='+') -> str:
        """Заменить шаблоны вида {маска} на `wildcard`"""
        return re.sub(self.MASK_REGEXP, wildcard, self.src_tpl)

    def to_dict(self, topic: str) -> dict:
        """Получить из шаблона словарь со значениями из строки"""
        regex_str = self.tpl_to_regex(self.src_tpl)
        matches = re.match(regex_str, topic)
        return matches.groupdict() if matches else {}


def clean_none_fields(input_dict: dict | list) -> any:
    if isinstance(input_dict, dict):
        return {k: v for k, v in ((kk, clean_none_fields(vv)) for kk, vv in input_dict.items()) if v is not None}
    if isinstance(input_dict, list):
        return [v for v in map(clean_none_fields, input_dict) if v is not None]

    return input_dict
