import re
import sys

rule = """HARDCODE VALUE  = 'SIMAH'"""


print(rule.find("HARDCODE VALUE  = '"))
print(rule[rule.find("'"):])