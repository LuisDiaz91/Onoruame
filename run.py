#!/usr/bin/env python
# run.py - Punto de entrada principal de Onoruame

import os
import sys
import logging

# Asegurar que podemos importar los módulos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gui.main_window import main

if __name__ == '__main__':
    main()
