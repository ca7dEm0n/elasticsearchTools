#!/bin/bash
### 
# @Author: cA7dEm0n
 # @Blog: http://www.a-cat.cn
 # @Since: 2019-09-23 18:00:14
 # @Motto: 欲目千里，更上一层
 # @message:  esTools执行脚本
 ###

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

playbook() {
    python ${SCRIPT_DIR}/esTools.py playbook  -c "${SCRIPT_DIR}/config.yaml" --playbook "${SCRIPT_DIR}/playbook.yaml" -vv --force
}

$1