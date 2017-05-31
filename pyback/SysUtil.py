#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import time
import logging
import sys
import os
import multiprocessing
import datetime
import re
import traceback


def get_local_address():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    if ip:
        return ip
    else:
        return hostname


def get_today_date():
    return time.strftime('%Y%m%d', time.localtime(time.time()))


def get_now_time():
    return datetime.datetime.now()


def get_logger(log_file=None, level='info', console=False):
    """初始化logger"""
    logger = logging.getLogger()

    level = getattr(logging, level.upper(), logging.INFO)
    set_logging_level(level)

    if console:
        add_logging_console()
    if log_file:
        add_logging_file(log_file)

    return logger


def set_logging_level(level):
    logger = logging.getLogger()
    if level < logger.level:
        logger.setLevel(level)
        formatter = get_logging_formater()
        for handler in logger.handlers:
            handler.setFormatter(formatter)


def add_logging_console():
    logger = logging.getLogger()
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            return False

    formatter = get_logging_formater()
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def add_logging_file(log_file, force=False):
    logger = logging.getLogger()
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and not force:
            return False

    formatter = get_logging_formater()
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return fh


def remove_logging_file(fh):
    logger = logging.getLogger()
    logger.removeHandler(fh)


def get_logging_formater():
    logger = logging.getLogger()
    if logger.level == logging.DEBUG:
        formatter = logging.Formatter('%(asctime)s - %(module)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')

    return formatter


def get_file_size(filename, store_type, pyback=None):
    if store_type == 'hdfs' and pyback:
        return pyback.hdfs.get_size(filename)
    elif store_type == 'local' and os.path.isfile(filename):
        return os.path.getsize(filename)
    else:
        return None


def check_positive_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def add_unit(value, value_type='decimal'):
    if value_type == 'bytes':
        base = 1024
    else:
        base = 1000

    unit = ['', 'K', 'M', 'G', 'T', 'P']
    unit_level = 0

    if check_positive_int(value):
        value = int(value)
        while value > base:
            value /= base
            unit_level += 1
        res_value = '%s%s' % (round(value, 0), unit[unit_level])
    else:
        res_value = value

    return res_value


def tv_interval(start_time, end_time=None):
    if not end_time:
        end_time = datetime.datetime.now()
    elapsed = end_time - start_time
    return elapsed.total_seconds()


def begin_process(func, args):
    try:
        res = func(*args)
        if res is True:
            exitcode = 0
        else:
            exitcode = 255
    except Exception, e:
        get_logger().error("Got unexcept error: %s" % e)
        exitcode = 255

    sys.exit(exitcode)


def start_child_process(func, args):
    process = multiprocessing.Process(target=begin_process, args=(func, args))
    process.start()
    return process


def croak(msg=None):
    if msg:
        print msg
    print traceback.format_exc()
    sys.exit(255)


def put_stderr(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()


def get_permission_sign(permission_num):
    permission_str = ''
    for num in str(permission_num)[-3:]:
        bin_num = str(bin(int(num))).replace('0b', '').zfill(3)
        permission_str += 'r' if int(bin_num[0]) else '-'
        permission_str += 'w' if int(bin_num[1]) else '-'
        permission_str += 'x' if int(bin_num[2]) else '-'
    return permission_str
