#pylint: disable=missing-docstring,line-too-long,trailing-whitespace, wrong-import-order, unused-import
from . import queue, manager
import app
from backend.sql import base


if __name__ == '__main__':
    base.init()
    queue.start()
