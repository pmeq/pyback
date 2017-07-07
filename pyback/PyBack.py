#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import ConfigParser

import SysUtil
import HdfsUtil

log = logging.getLogger(__name__)


class PyBack:
    def __init__(self, config_file=None):
        self.config_file = config_file

        self.hdfs_conf = None
        self.home_dir = None
        self.address = None

        self.address = SysUtil.get_local_address()
        self.read_config()
        self.hdfs = self.get_hdfs()

        self.err_msg = ''
        self.last_put_file = ''

    def du(self, path):
        size = self.hdfs.get_size(path)
        if size is None:
            log.error(self.hdfs.err_msg)

        return size

    def list(self, path):
        info = self.hdfs.get_path_status(path)
        if info is None:
            log.error(self.hdfs.err_msg)

        return info

    def get(self, source, dest=None):
        if not dest:
            dest = os.getcwd()
        res = self.hdfs.get_to_local(source, dest)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def put(self, source, dest=None, date=None, sub_dir=None, store_type='online', stream=False):
        if source == '-':
            stream = True

        if not dest and stream:
            log.error("A destination file should be given when using streaming mode")
            return False

        dest = self.get_format_dest_file(source, dest, date, sub_dir, store_type)
        if stream:
            res = self.hdfs.put_from_stream(dest)
        else:
            res = self.hdfs.put_from_local(source, dest)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def move(self, source, dest):
        res = self.hdfs.move(source, dest)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def mkdir(self, path):
        res = self.hdfs.mkdir(path)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def delete(self, path):
        res = self.hdfs.delete(path)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def cat(self, path):
        res = self.hdfs.cat(path)
        if not res:
            log.error(self.hdfs.err_msg)

        return res

    def get_format_dest_file(self, source, dest='', date=None, sub_dir=None, store_type='online'):
        """
        返回hdfs上传目录(不加参数时会自动补全)
        """
        if not dest:
            dest = os.path.basename(source)

        # 绝对路径不补全目录
        if dest.startswith('/'):
            return dest

        address = self.address

        # 存储类型, 在线或归档
        if store_type == 'archive':
            base_dir = os.path.join(self.home_dir, store_type)
        elif store_type == 'online':
            base_dir = os.path.join(self.home_dir, store_type)
        else:
            base_dir = self.home_dir

        # 日期目录
        if not date:
            date = SysUtil.get_today_date()
        base_dir = os.path.join(base_dir, date, address)

        # 子目录
        if sub_dir:
            base_dir = os.path.join(base_dir, sub_dir)

        dest_file = os.path.join(base_dir, os.path.basename(dest))
        return dest_file

    def get_hdfs(self):
        conf = self.hdfs_conf
        h = HdfsUtil.HDFS(**conf)
        h.connect()
        return h

    def read_config(self):
        config_file = self.config_file
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        try:
            self.hdfs_conf = {
                'hdfs_hosts': config.get('hdfs', 'hosts'),
                'hdfs_user': config.get('hdfs', 'user_name'),
                'connect_timeout': config.getint('hdfs', 'timeout'),
                'connect_max_tries': config.getint('hdfs', 'max_tries'),
                'connect_retry_delay': config.getint('hdfs', 'retry_delay'),
            }
            self.home_dir = config.get('hdfs', 'home_dir')

        except ConfigParser.Error, err:
            log.error("Got error when read config file %s: %s " % (config_file, err.message))
