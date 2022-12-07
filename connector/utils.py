import re

def prepare_topic_mqtt_to_kafka(mqtt_topic: str) -> str:
    try:
        customer_id = mqtt_topic.split('/')[1]
    except IndexError:
        return ''
    return f'customer_{customer_id}'


class Template:
    MASK_REGEXP = r"{(?P<tpl_name>\w+)}"
    NOT_USED_SYM = '~'

    def __init__(self, src_tpl: str, dst_tpl: str):
        self.src_tpl = src_tpl  # Исходный шаблон
        self.dst_tpl = dst_tpl  # Шаблон к которому нужно привести

    def tpl_to_regex(self, tpl: str) -> str:
        """Заменить шаблоны вида {маска} на именованные регулярные выражения"""
        return re.sub(self.MASK_REGEXP, "(?P<\g<tpl_name>>.+)", tpl)


    def tpl_to_sub(self, tpl: str) -> str:
        """Заменить шаблоны вида {ШАБЛОН} на подстановочные выражения"""
        # HACK: не получилось сразу заменить backslash. Возможно это
        # изменится в будущих версиях питона.
        s = re.sub(self.MASK_REGEXP, rf"{self.NOT_USED_SYM}g<\g<tpl_name>>", tpl)
        return s.replace(self.NOT_USED_SYM, "\\")


    def transform(self, topic: str) -> str | None:
        """Получить топик путём подстановки в шаблон"""
        regex_str = self.tpl_to_regex(self.src_tpl)
        is_match = re.match(regex_str, topic)
        if not is_match:
            return None

        sub_mask = self.tpl_to_sub(self.dst_tpl)
        return re.sub(regex_str, sub_mask, topic)
