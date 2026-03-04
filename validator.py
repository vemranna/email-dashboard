#!/usr/bin/env python3
import sys
import random
SUCCESS=0.2
if random.random() < SUCCESS:
    print("Validation sucessful")
    print("Summary")
    sys.exit(0)
else:
    print("Validation failed")
    print("Errors")
    sys.exit(1)
