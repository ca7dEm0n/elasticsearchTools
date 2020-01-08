# coding: utf-8
'''
@Author: cA7dEm0n
@Blog: http://www.a-cat.cn
@Since: 2019-09-23 18:00:23
@Motto: 欲目千里，更上一层
@message: ES操作工具
'''
import logging.config
import argparse
import yaml
import json
import os

from ast import literal_eval
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, TransportError


# config log level
def _log_config(level):
    return {
        'version': 1,
        'formatters': {
            'simple': {
                'format': '%(asctime)s %(name)s [%(levelname)s] - %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'simple'
            },
            'file': {
                'class': 'logging.FileHandler',
                'filename': '{}.log'.format(__file__),
                'level': 'DEBUG',
                'formatter': 'simple'
            },
        },
        'loggers': {
            __file__: {
                'handlers': ['console', 'file'],
                'level': level,
            },
        }
    }


# nothing to do


class QuietLOG:
    @classmethod
    def debug(self, message):
        pass

    @classmethod
    def info(self, message):
        pass

    @classmethod
    def error(self, message):
        pass


# default
logging.config.dictConfig(_log_config("INFO"))

parser = argparse.ArgumentParser()
parser.add_argument("mode",
                    choices=["playbook", "shell"],
                    help="选择运行模式: playbook/shell")
parser.add_argument("-v", default=0, action="count", help="-vv开启DEBUG模式,默认-v")
parser.add_argument("-c", default="config.yaml", help="指定配置文件,默认config.yaml")
parser.add_argument("-q", action="store_true", help="安静模式")
parser.add_argument("--playbook", help="playbook")
parser.add_argument("--force", action="store_true", help="强制模式")

args = parser.parse_args()

if args.v == 2:
    logging.config.dictConfig(_log_config("DEBUG"))

logger = logging.getLogger(__file__)

if args.q:
    logger = QuietLOG


class Base:
    pass


class Config:
    def __init__(self, path):
        self.path = path
        self.env = {}

        # 读取
        self.data = self.read(self.path)

        # env
        self.__format_env()

    # 用变量渲染self.data
    def __format_env(self):
        '''
        @description: 处理变量
        '''
        if self.data and self.data.get("env", ""):
            env_dict = self.data["env"]
            for k, v in env_dict.items():
                if isinstance(v, dict):
                    for kt, command in v.items():
                        if kt == "python":
                            self.env[k] = self._run_python(command)
                        elif kt == "shell":
                            self.env[k] = self._run_shell(command)
                else:
                    self.env[k] = [v]
            self.data.pop("env")
            self.data = self.format_data(self.data, self.env)
        logger.debug("config env: {}".format(str(self.env)))

    @classmethod
    def read(self, filePath):
        '''
        @description: 读取yaml配置
        @param {string}  filePath 文件路径 
        @return: dict
        '''
        assert os.path.exists(filePath), "路径[{}]不存在".format(filePath)
        with open(filePath, 'r') as f:
            return yaml.safe_load(f.read())
        return None

    @classmethod
    def format_data(self, source_data, mapping_dict):
        '''
        @description: 返回渲染后的data
        @param {dict}    source_data未经渲染数据
        @param {dict}    mapping_dict渲染数据
        @return: 
        '''
        filter_mapping_key = dict(
            filter(lambda x: x[0] in str(source_data), mapping_dict.items()))
        if filter_mapping_key:
            source_data = str(source_data)

            # 增加双括号
            source_data = source_data.replace("{", "{{").replace("}", "}}")

            # 取消mapping双括号
            for _ in filter_mapping_key.keys():
                source_data = source_data.replace("{{%s}}" % (_), "{%s}" % (_))

            # format mapping
            source_data = source_data.format(**filter_mapping_key)
            return literal_eval(source_data)
        else:
            return source_data

    @classmethod
    def _run_python(self, command):
        '''
        @description: 执行python代码
        '''
        try:
            code = compile(command, "script", "exec")
            result = {}
            exec code in result
            return result.get("result", "")
        except Exception as e:
            logger.error("run_python error : {}".format(e))
            return command

    @classmethod
    def _run_shell(self, command):
        '''
        @description: 执行shell脚本
        '''
        try:
            from os import popen
            from string import rstrip
            run_shell = popen(command)
            return ''.join(map(rstrip, run_shell.readlines()))
        except Exception as e:
            logger.error("run_shell error: {}".format(e))
            return command


class PlayBook:
    def __init__(self, config, settings=None, es=None, force=False):
        self.es = es
        self.config = config
        self.force = force
        self.settings = settings

    def run(self):
        for _ in self.config:
            job_type = _["job"]
            job_function = getattr(self, "job_{}".format(job_type), None)
            if job_function:
                job_function(_)

    def get_snapshot(self, repository_name, snapshot_name):
        '''
        @description: 获取快照
        @param {string}  repository_name   仓库名
        @param {string}  snapshot_name     快照名
        @return: dict
        '''
        try:
            result = self.es.snapshot.get(repository=repository_name,
                                          snapshot=snapshot_name)
            logger.debug("获取快照仓库,返回:{}".format(str(result)))
        except NotFoundError:
            result = {}
            logger.debug("获取快照仓库,返回为空")
        return result

    def get_snapshot_repository(self, repository_name=None):
        '''
        @description: 获取快照仓库
        @param {string}  repository_name   仓库名，默认为None
        @return: dict
        '''
        try:
            result = self.es.snapshot.get_repository(repository_name)
            logger.debug("获取快照仓库,返回:{}".format(str(result)))
        except NotFoundError:
            result = {}
            logger.debug("获取快照仓库,返回为空")
        return result

    def create_snapshot(self, repository_name, snapshot_name, body):
        '''
        @description: 创建快照
        @param {string}   repository_name  仓库名
        @param {string}   snapshot_name    快照名
        @param {string}   body             post_body
        @return: True/False
        '''
        _base_num = 0
        from time import sleep
        from random import random
        while True:
            random_num = "%2f" % (random() + 600 * _base_num)
            try:
                result = self.es.snapshot.create(repository_name,
                                                 snapshot_name, body)
                if result.get("acknowledged", ""):
                    logger.debug("创建快照[{}]成功".format(snapshot_name))
                    return True
            except NotFoundError as e:
                logger.error("创建快照[{}]失败,错误信息:{}".format(snapshot_name, e))
                return False
            except TransportError as e:
                is_exits = self.get_snapshot(repository_name, snapshot_name)
                if is_exits:
                    logger.debug("快照[{}]已经存在".format(snapshot_name))
                    return True
                _base_num += 1
                logger.warning("创建快照[{}]失败(第{}次尝试,休息{}秒),错误信息:{}".format(
                    snapshot_name, _base_num + 1, random_num, e))
                sleep(float(random_num))
            if _base_num >= 10:
                logger.error("创建快照[{}]失败!".format(snapshot_name))
                return False

    def create_snapshot_repository(self, repository_name, body):
        '''
        @description: 创建快照仓库
        @param {string}  repository_name   仓库名
        @param {dict}    body              参数
        @return: True/False
        '''
        result = self.es.snapshot.create_repository(repository_name, body)
        if result.get("acknowledged", ""):
            logger.debug("成功创建仓库名为[{}]的快照, POST参数:{}".format(
                repository_name, str(body)))
            return True
        logger.debug("创建仓库名为[{}]的快照失败! POST参数:{}".format(
            repository_name, str(body)))
        return False

    def job_backup(self, config):
        '''
        @description: 备份任务  
        '''
        snapshot_settings = self.settings["snapshot"]
        snapshot_repository_name = snapshot_settings["repository"]
        snapshot_repository_post_body = snapshot_settings["body"]

        index_list = config["index"]
        index_body = config["body"]

        self._exe_create_s3_snapshot_job(snapshot_repository_name,
                                         snapshot_repository_post_body,
                                         index_list, index_body)

    def job_delete(self, config):
        '''
        @description: 删除任务
        '''
        index_name = config.get("index", "")
        save_day = config.get("save", "")

        if isinstance(index_name, list):
            # 遍历index列表
            for i in index_name:
                self._exe_delete_index_job(i, save_day)
        elif isinstance(index_name, str):
            self._exe_delete_index_job(index_name, save_day)

    def _exe_create_s3_snapshot_job(self, snapshot_repository_name,
                                    snapshot_repository_post_body, index_list,
                                    index_body):
        '''
        @description:  执行创建快照任务
        @param {string}   snapshot_repository_name 快照仓库名
        @param {dict}     snapshot_repository_post_body 快照仓库创建body
        @param {list/string}     index_list  需要快照的index列表
        @param {dict}     index_body  index创建body
        '''
        # 判断快照仓库
        if not self.get_snapshot_repository(snapshot_repository_name):
            if not self.force:
                yORn = raw_input(
                    "确定需要创建[{}]快照仓库?  (y/n)".format(snapshot_repository_name))
                if yORn == "y" or yORn == "Y":
                    create_result = self.create_snapshot_repository(
                        snapshot_repository_name,
                        snapshot_repository_post_body)
                    if not create_result:
                        logger.error(
                            "创建[{}]快照仓库失败!".format(snapshot_repository_name))
            else:
                logger.debug(
                    "没找到名为[{}]的快照仓库,需要创建".format(snapshot_repository_name))
                self.create_snapshot_repository(snapshot_repository_name,
                                                snapshot_repository_post_body)

        # 强制转index列表
        index_list = [index_list] if isinstance(index_list,
                                                str) else index_list

        for _ in index_list:
            # 渲染
            post_body = Config.format_data(index_body, {"index": _})
            result = self.create_snapshot(snapshot_repository_name, _,
                                          post_body)

        logger.info("S3快照任务执行完成")

    def _exe_delete_index_job(self, index, save_day):
        '''
        @description:  执行删除index任务
        @param {string}  index      索引名 
        @param {int}     save_day   保存时间
        @return: None
        '''
        from time import localtime, strftime
        index_create_date_list = self._get_index_create_data(index)
        result = self._filter_index(index_create_date_list, int(save_day))

        # 发现需要删除列表
        if result:
            if self.force:
                for index_name, create_time in result.items():
                    delete_index = self.es.delete_index(index_name)
                    if delete_index:
                        strTime = strftime("%Y年%m月%d日",
                                           localtime(int(create_time[:10])))
                        logger.debug("[*]index[{}]被删除,因该index创建于{}".format(
                            index_name, strTime))
                    else:
                        logger.debug("[*]index[{}]删除失败".format(index_name))
            else:
                for index_name, create_time in result.items():
                    strTime = strftime("%Y年%m月%d日",
                                       localtime(int(create_time[:10])))
                    logger.debug(
                        "index[{}]需要被删除,因配置文件保留{}天,而该index创建于{}".format(
                            index_name, save_day, strTime))
                    yORn = raw_input("确定删除[{}]?  (y/n)".format(index_name))
                    if yORn == "y" or yORn == "Y":
                        delete_index = self.es.delete_index(index_name)
                        if delete_index:
                            logger.debug("[*]index[{}]已被删除".format(index_name))
                        else:
                            logger.debug("[*]index[{}]删除失败".format(index_name))
                    else:
                        logger.debug("取消删除[{}]任务.".format(index))
        else:
            logger.info("[{}]的删除任务执行完成.没发现需要删除的index.".format(index))

    def _filter_index(self, data, day):
        '''
        @description: 过滤需要删除的index
        @param {dict}  data   index与创建日期的字典
        @param {int}   day    保留天数
        @return: dict
        '''
        from time import time
        now_time = int(round(time() * 1000))
        day_delta = int(day) * 86400000
        last_time = now_time - day_delta
        return dict(filter(lambda x: int(x[1]) < last_time, data.items()))

    def _get_index_create_data(self, index):
        '''
        @description: 获取index配置
        @param {string} name  index前缀
        '''
        settings_list = self.es.get_index_settings("{}*".format(index))
        return {
            k: v["settings"]["index"]["creation_date"]
            for k, v in settings_list.items() if settings_list
        }


class Shell:
    def __init__(self):
        pass


class Es(Elasticsearch):
    '''
    @description: 继承Elasticsearch 自定义封装
    '''

    # 新增获取index属性
    def get_index_settings(self, index=""):
        return self.transport.perform_request("GET",
                                              "/{}*/_settings".format(index))

    # 删除index
    def delete_index(self, index):
        '''
        @description: 删除index
        @param {string}  index index名
        @return: True/False
        '''
        try:
            result = self.indices.delete(index=index, ignore=[400, 404])
            if result.get("acknowledged", ""):
                logger.debug("删除[{}]成功".format(index))
                return True
            else:
                logger.debug("删除[{}]失败".format(index))
                return False
        except Exception as e:
            logger.error("function (delete_index)尝试删除[{}]失败,原因:{}".format(
                index, e))
            return False


def main(args):
    '''
    @description: 运行主程
    @param {dict} args 传入的参数 
    '''
    # 实例化Config
    c = Config(args.c)

    # ES配置
    ES_URL = c.data["elasticsearch"]["url"]
    ES = Es(ES_URL, request_timeout=30)

    # 强制模式
    force = args.force

    if args.mode == "playbook":
        if not args.playbook:
            logger.error("[playbook]模式, [--playbook]不能为空")
            return

        # 源数据
        book_config_source = Config.read(args.playbook)

        # mapping格式化后
        book_config = Config.format_data(book_config_source, c.env)

        # 加载playbook
        playbook = PlayBook(book_config, c.data, ES, force)
        playbook.run()


if __name__ == "__main__":
    main(args)