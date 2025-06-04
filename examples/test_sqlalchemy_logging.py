#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLAlchemy日志控制测试脚本

此脚本用于测试message模块对SQLAlchemy日志的控制功能
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from tk_db_utils.message import message, set_sqlalchemy_log_level
from tk_db_utils.datebase import engine, get_session
from sqlalchemy import text

def test_sqlalchemy_logging_control():
    """测试SQLAlchemy日志控制功能"""
    
    print("=== SQLAlchemy日志控制测试 ===")
    
    # 测试1: 默认情况下SQLAlchemy日志应该被抑制
    print("\n1. 测试默认日志级别 (应该不显示SQLAlchemy内部日志):")
    if engine:
        try:
            with get_session() as session:
                result = session.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                print(f"查询结果: {row[0]}")
        except Exception as e:
            print(f"查询失败: {e}")
    else:
        print("数据库引擎未初始化，跳过测试")
    
    # 测试2: 启用SQLAlchemy DEBUG日志
    print("\n2. 测试启用SQLAlchemy DEBUG日志 (应该显示详细的SQL日志):")
    set_sqlalchemy_log_level("DEBUG")
    
    if engine:
        try:
            with get_session() as session:
                result = session.execute(text("SELECT 2 as test_value"))
                row = result.fetchone()
                print(f"查询结果: {row[0]}")
        except Exception as e:
            print(f"查询失败: {e}")
    else:
        print("数据库引擎未初始化，跳过测试")
    
    # 测试3: 重新禁用SQLAlchemy日志
    print("\n3. 测试重新禁用SQLAlchemy日志 (应该不显示SQLAlchemy内部日志):")
    set_sqlalchemy_log_level("WARNING")
    
    if engine:
        try:
            with get_session() as session:
                result = session.execute(text("SELECT 3 as test_value"))
                row = result.fetchone()
                print(f"查询结果: {row[0]}")
        except Exception as e:
            print(f"查询失败: {e}")
    else:
        print("数据库引擎未初始化，跳过测试")
    
    # 测试4: 测试无效日志级别
    print("\n4. 测试无效日志级别处理:")
    try:
        set_sqlalchemy_log_level("INVALID")
        print("错误: 应该抛出异常")
    except ValueError as e:
        print(f"正确捕获异常: {e}")
    
    print("\n=== 测试完成 ===")

def test_message_instance_method():
    """测试Message实例方法"""
    
    print("\n=== Message实例方法测试 ===")
    
    # 测试实例方法
    print("\n测试Message实例的set_sqlalchemy_log_level方法:")
    try:
        message.set_sqlalchemy_log_level("INFO")
        print("成功设置SQLAlchemy日志级别为INFO")
        
        message.set_sqlalchemy_log_level("WARNING")
        print("成功设置SQLAlchemy日志级别为WARNING")
        
    except Exception as e:
        print(f"测试失败: {e}")
    
    print("\n=== Message实例方法测试完成 ===")

if __name__ == "__main__":
    # 设置基本日志配置
    message.set_logger_level("info")
    
    print("开始SQLAlchemy日志控制功能测试...")
    
    # 运行测试
    test_sqlalchemy_logging_control()
    test_message_instance_method()
    
    print("\n所有测试完成！")
