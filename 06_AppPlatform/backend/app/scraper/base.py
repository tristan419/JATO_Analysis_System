from importlib import import_module
import sys

from app.scraper import enable_external_scraper_package


enable_external_scraper_package()
sys.modules[__name__] = import_module("jato_scraper.base")
