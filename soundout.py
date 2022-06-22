#!/usr/bin/python3

import sys
import re
from pincelate import Pincelate

# get Pincelate object
pin = Pincelate()

def get_stress(word):
    '''Takes a string and returns a string of numbers representing the stress pattern'''
    res = None
    try:
        # use pincelate's soundout method to guess the phonemes
        # if pincelate cannot make a guess, return null
        phones = "".join(pin.soundout(word))
        stress = re.sub(r"[^012]", "", phones)
    except:
        pass
    return stress

for line in sys.stdin:
    if len(line.strip()) > 0:
        try:
            words = line[14:-3].split(", ")
            for word in words:
                if len(word) > 0 and len(word.strip()) > 0:
                    stress = get_stress(word)
                    if stress is not None:
                        print(f"{word},{stress}")
        except:
            pass
        