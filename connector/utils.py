import re
from typing import Dict, Optional


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

    def to_dict(self, topic: str) -> Optional[Dict[str, str]]:
        """Получить из шаблона словарь с значениями из строки"""
        regex_str = self.tpl_to_regex(self.src_tpl)
        matches = re.match(regex_str, topic)
        return matches.groupdict() if matches else None
