import sys
import os

print("--- Python Interpreter ---")
print(sys.executable)
print("\n--- Python Path ---")
for path in sys.path:
    print(path)

print("\n--- PyMuPDF (fitz) Location ---")
try:
    import fitz
    print(fitz.__file__)
except ImportError as e:
    print(e)
