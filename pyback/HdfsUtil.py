#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyhdfs
import sys
import os
import shutil
import requests
import logging

import SysUtil

BLOCK_SIZE = 128*1024*1024

log = logging.getLogger(__name__)


class HDFS:
    def __init__(self, **kwargs):
        self.hdfs_hosts = kwargs.get('hdfs_hosts')
        self.hdfs_user = kwargs.get('hdfs_user')
        self.connect_timeout = kwargs.get('connect_timeout', 5)
        self.connect_max_tries = kwargs.get('connect_max_tries', 1)
        self.connect_retry_delay = kwargs.get('connect_retry_delay', 3)

        self.err_msg = None
        self.hdfs_client = None

    def connect(self):
        """初始化hdfs client, 测试连接"""
        self.__clear_err_msg()
        try:
            self.hdfs_client = pyhdfs.HdfsClient(hosts=self.hdfs_hosts, user_name=self.hdfs_user,
                                                 timeout=self.connect_timeout, max_tries=self.connect_max_tries,
                                                 retry_delay=self.connect_retry_delay)
            self.hdfs_client.list_status('/')
            return True
        except pyhdfs.HdfsException, err:
            self.err_msg = "Init hdfs failed: %s" % err.message
            return False
        except Exception, e:
            self.err_msg = "Init hdfs failed: %s" % e
            return False

    def __create_file(self, source, dest_file):
        """ 创建文件 """
        hdfs_client = self.hdfs_client
        self.__clear_err_msg()
        try:
            if not self.exists(dest_file):
                hdfs_client.create(dest_file, source, buffersize=BLOCK_SIZE)
                return True
            else:
                self.err_msg = "File %s is exists" % dest_file
                return False
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when put file: %s" % err.message
            return False
        except Exception, e:
            SysUtil.croak(e)
            self.err_msg = "Got error when put file: %s" % e
            return False

    def put_from_stream(self, dest_file):
        """从标准输入上传文件"""
        return self.__create_file(sys.stdin, dest_file)

    def put_from_local(self, source_file, dest):
        """从本地文件上传"""
        self.__clear_err_msg()
        try:
            if self.exists(dest) and self.is_dir(dest):
                dest_dir = dest
                dest_file = os.path.join(dest_dir, os.path.basename(source_file))
            else:
                dest_file = dest
            return self.__create_file(open(source_file, 'rb'), dest_file)
        except Exception, e:
            self.err_msg = "Got error when put file: %s" % e
            return False

    def get_to_local(self, source_file, dest, offset=0):
        """下载文件到本地"""
        hdfs_client = self.hdfs_client

        self.__clear_err_msg()
        if os.path.isdir(dest):
            dest_dir = dest
            dest_file = os.path.join(dest_dir, os.path.basename(source_file))
        else:
            dest_file = dest

        try:
            if os.path.exists(dest_file) and offset == 0:
                self.err_msg = "Destination file is exists!"
                return False

            fsrc = hdfs_client.open(source_file, buffersize=BLOCK_SIZE, offset=offset)
            fdst = open(dest_file, 'ab', buffering=BLOCK_SIZE)
            shutil.copyfileobj(fsrc, fdst, BLOCK_SIZE)
            fdst.close()
            return True
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when get file: %s" % err.message
            return False
        except requests.packages.urllib3.exceptions.ProtocolError, e:
            if "IncompleteRead" in str(e):
                fdst.close()
                local_size = SysUtil.get_file_size(dest_file, 'local')
                log.warning("Got IncompleteRead when get file: %s, retry from offset %s" % (source_file, local_size))
                return self.get_to_local(source_file, dest, local_size)
            self.err_msg = "Got error when get file: %s" % e
            return False
        except Exception, e:
            self.err_msg = "Got error when get file: %s" % e
            return False

    def cat(self, path, offset=0):
        """返回文件句柄"""
        hdfs_client = self.hdfs_client

        self.__clear_err_msg()
        total_size = 0
        push_size = offset if offset else 0
        try:
            if self.is_file(path):
                total_size = self.get_size(path)

            fsrc = hdfs_client.open(path, buffersize=BLOCK_SIZE, offset=offset)
            while push_size < total_size:
                read_data = fsrc.read(BLOCK_SIZE)
                sys.stdout.write(read_data)
                push_size += len(read_data)
            return True
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when cat file: %s" % err.message
            return False
        except requests.packages.urllib3.exceptions.ProtocolError, e:
            if "IncompleteRead" in str(e):
                log.warning("Got IncompleteRead when get file: %s, retry from offset %s" % (path, push_size))
                return self.get_to_local(path, push_size)
            self.err_msg = "Got error when cat file: %s" % e
            return False
        except Exception, e:
            self.err_msg = "Got error when cat file: %s" % e
            return False

    def move(self, source, dest):
        """移动文件"""
        hdfs_client = self.hdfs_client
        if self.exists(dest) and self.is_dir(dest):
            dest = os.path.join(dest, os.path.basename(source))

        self.__clear_err_msg()
        try:
            return hdfs_client.rename(source, dest)
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when move file : %s" % err.message
            return False
        except Exception, e:
            self.err_msg = "Got error when get file: %s" % e
            return False

    def mkdir(self, path):
        """创建目录"""
        hdfs_client = self.hdfs_client
        self.__clear_err_msg()

        try:
            if self.exists(path):
                if self.is_dir(path):
                    return True
                elif self.is_file(path):
                    self.err_msg = "Destination path is file: %s" % path
                    return False
                else:
                    return hdfs_client.mkdirs(path)
            else:
                return hdfs_client.mkdirs(path)
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when mkdir dir: %s" % err.message
            return False
        except Exception, e:
            self.err_msg = "Got error when mkdir dir: %s" % e
            return False

    def delete(self, path):
        """删除文件"""
        self.__clear_err_msg()

        try:
            if not self.exists(path):
                self.err_msg = "Destination path is not exist: %s" % path
                return False

            trash_dir = os.path.join('/user', self.hdfs_user, '.Trash/Current') + os.path.dirname(path)
            trash_file = os.path.join(trash_dir, os.path.basename(path))
            self.mkdir(trash_dir)
            if self.exists(trash_file):
                suffix = 1
                while True:
                    tmp_file = "%s.%s" % (trash_file, suffix)
                    if self.exists(tmp_file):
                        suffix += 1
                        continue
                    else:
                        trash_file = tmp_file
                        break
            return self.move(path, trash_file)
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got hdfs error when delete path: %s" % err.message
            return False
        except Exception, e:
            self.err_msg = "Got error when delete path: %s" % e
            return False

    def get_size(self, path):
        """获取文件或目录大小"""
        self.__clear_err_msg()

        size = 0
        if not self.exists(path):
            self.err_msg = "Destination file is not exists!"
            return None

        list_status = self.__get_list_status(path)
        if list_status is not None:
            for file_status in list_status:
                if file_status.type.upper() == "FILE":
                    size += file_status.length
                elif file_status.type.upper() == "DIRECTORY":
                    size += self.get_size(os.path.join(path, file_status.pathSuffix))
            return size
        else:
            return None

    def get_path_status(self, path):
        """获取hdfs状态, list格式, 如果path是目录返回目录内文件状态"""
        self.__clear_err_msg()
        if self.exists(path):
            return self.__get_list_status(path)
        else:
            self.err_msg = "Destination file is not exists!"
            return None

    def __get_file_status(self, path):
        """获取单个hdfs文件状态"""
        self.__clear_err_msg()

        try:
            file_status = self.hdfs_client.get_file_status(path)
            return file_status
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got error when get file status: %s" % err.message
            return None

    def __get_list_status(self, path):
        """获取hdfs状态, list格式, 如果path是目录返回目录内文件状态"""
        self.__clear_err_msg()

        try:
            file_status = self.hdfs_client.list_status(path)
            return file_status
        except pyhdfs.HdfsException, err:
            self.err_msg = "Got error when get file status: %s" % err.message
            return None

    def __get_file_type(self, path):
        """获取文件类型"""
        file_type = 'unknown'
        file_status = self.hdfs_client.get_file_status(path)
        if file_status:
            file_type = file_status.type

        return file_type

    def is_dir(self, path):
        """判断path是否是目录"""
        if self.__get_file_type(path).upper() == "DIRECTORY":
            return True
        else:
            return False

    def is_file(self, path):
        """判断path是否是文件"""
        if self.__get_file_type(path).upper() == "FILE":
            return True
        else:
            return False

    def exists(self, path):
        """判断path是否存在"""
        return self.hdfs_client.exists(path)

    def __clear_err_msg(self):
        """清空错误信息"""
        self.err_msg = ''


if __name__ == "__main__":
    hdfs = HDFS(hdfs_hosts="172.25.179.16:50070,172.25.179.46:50070", hdfs_user="xdba")
    hdfs.connect()
    # hdfs.put_from_local('abc.txt', '/database/mysql/test')
