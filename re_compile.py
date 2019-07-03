import re

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def normalize(text, pat_obj):
    words = re.findall(pat_obj, text.lower())
    return words

st = 'Flowchart: Should I Even Consider Self-Employment?'
print(normalizeWords(st))
pat_obj = re.compile(r'\W+')
print(normalize(st, pat_obj))
