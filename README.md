# esTools 

[![Python2.7](https://img.shields.io/badge/Python-2.7-green.svg?style=plastic)](https://www.python.org/)


`Elasticsearch`操作工具


## 参数说明

#### 支持两种运行模式

- playbook
	读取配置文件执行模式

- shell
	命令行执行模式

#### 可选参数
- `-c` 指定配置文件.
- `-q` 开启后没有任何输出结果.
- `-v` 日志等级，`-v`为INFO级别，`-vv`为DEBUG级别.
- `--playbook` Playbook运行模式下可使用，指定`playbook`文件.
- `--force` 强制模式，开启后不会再询问，全为`y`.


```
usage: esTools.py [-h] [-v] [-c C] [-q] [--playbook PLAYBOOK] [--force]
                  {playbook,cmd}

positional arguments:
  {playbook,cmd}     选择运行模式: playbook/shell

optional arguments:
  -h, --help           show this help message and exit
  -v                   -vv开启DEBUG模式，默认-v
  -s                   指定es主机
  -cmd                 执行cmd的方法名
  -c C                 指定配置文件，默认config.yaml
  -q                   安静模式
  --playbook PLAYBOOK  playbook
  --force              强制模式
```

## 配置文件说明

- `elasticsearch` ES基础配置
	- `url`  ES的访问链接

示例：
```yaml
elasticsearch:
	url: 192.168.1.1:9200
```

- `env` 设置变量，可用于配置文件

> 变量设置可支持三种方式： 默认、shell、 python

示例：
```yaml
env:
	# 返回: {"today": "20190101"}
    today: "20190101"    				# 默认方式
	
	# 运行shell获取结果
	# 返回: {"today_one": "2019.09.26"}
  today_one: 
    shell: "date +%Y.%m.%d"		
    
  # 运行python获取结果,取result值
  # 返回: {"today_two": "Python生成"}
  today_two:
    python: "result='Python生成'"
```

- `snapshot`  快照配置
	- `repository` 指定快照仓库，推荐每天一个快照仓库
	- `body` 仓库的配置，如果没有，将自动创建快照仓库

示例：
```yaml
snapshot:
  # 用变量方式定义快照仓库名
  repository: "{today_one}" 
  body:
  	# 仓库的POST信息
  	# 当前采用S3快照备份
    type: "s3"
    settings: 
      bucket: "log_backup"
      base_path: "elasticsearch/{today_two}"
      endpoint: "ap-northeast-1"
      compress: True
```

## Playbook

**delete**

> 按创建时间删除！ 

- `index` 需要删除的`index`，例: `access*`.
- `save`  保留天数，按`创建时间`删除过期index.

**backup**
> 推荐一天为一个快照仓库

- `type`  备份类型，当前只有`s3`类型.
- `index` 需要快照的`index`.
- `body`  创建快照参数，`{index}`为特殊变量，可自动循环替换

## Cmd

**readOnly**

> 获取当前只读索引

示例:

```shell
python esTools.py cmd -cmd getReadOnly -s http://192.168.1.1:9200
```