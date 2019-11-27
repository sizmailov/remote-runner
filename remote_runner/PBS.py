import re

class QStatParser:
    def __init__(self):
        job_attribute_line_regex = r" +(?P<attr>[\w._]+) = (?P<value>[^\n]*)\n"
        job_attributes_regex = fr"(?P<attrs>({job_attribute_line_regex})*)"
        job_title_regex = "Job Id: (?P<id>[^\n]+)\n"
        job_regex = rf"{job_title_regex}{job_attributes_regex}"

        self.job = re.compile(job_regex)
        self.attr = re.compile(job_attribute_line_regex)

    def parse(self, inp):
        fix_continuation = inp.replace("\n\t", "")
        result = {}
        for job_id, attrs, _, _, _ in self.job.findall(fix_continuation):
            result[job_id] = {}
            for attr, value in self.attr.findall(attrs):
                result[job_id][attr] = value
        return result
