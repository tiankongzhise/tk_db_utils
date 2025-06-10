#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试_types_compatible方法对DATETIME和TIMESTAMP类型的区分能力
"""

import unittest
from unittest.mock import Mock
from src.tk_db_utils.schema_validator import SchemaValidator


class TestTypesCompatible(unittest.TestCase):
    """测试类型兼容性检查方法"""
    
    def setUp(self):
        """设置测试环境"""
        # 创建模拟的engine和session
        mock_engine = Mock()
        mock_session = Mock()
        self.validator = SchemaValidator(mock_engine, mock_session)
    
    def test_datetime_timestamp_distinction(self):
        """测试DATETIME和TIMESTAMP类型的区分"""
        # 测试DATETIME类型匹配
        self.assertTrue(
            self.validator._types_compatible('DATETIME', 'DATETIME'),
            "DATETIME应该与DATETIME兼容"
        )
        
        # 测试TIMESTAMP类型匹配
        self.assertTrue(
            self.validator._types_compatible('TIMESTAMP', 'TIMESTAMP'),
            "TIMESTAMP应该与TIMESTAMP兼容"
        )
        
        # 测试DATETIME和TIMESTAMP不应该互相兼容
        self.assertFalse(
            self.validator._types_compatible('DATETIME', 'TIMESTAMP'),
            "DATETIME不应该与TIMESTAMP兼容"
        )
        
        self.assertFalse(
            self.validator._types_compatible('TIMESTAMP', 'DATETIME'),
            "TIMESTAMP不应该与DATETIME兼容"
        )
    
    def test_case_insensitive_matching(self):
        """测试大小写不敏感的匹配"""
        # 测试小写
        self.assertTrue(
            self.validator._types_compatible('datetime', 'datetime'),
            "小写datetime应该匹配"
        )
        
        self.assertTrue(
            self.validator._types_compatible('timestamp', 'timestamp'),
            "小写timestamp应该匹配"
        )
        
        # 测试混合大小写
        self.assertTrue(
            self.validator._types_compatible('DateTime', 'DATETIME'),
            "混合大小写应该匹配"
        )
        
        # 测试不同类型的大小写组合不匹配
        self.assertFalse(
            self.validator._types_compatible('datetime', 'TIMESTAMP'),
            "不同类型即使大小写不同也不应该匹配"
        )
    
    def test_other_type_compatibility(self):
        """测试其他类型的兼容性"""
        # 测试INTEGER类型组
        self.assertTrue(
            self.validator._types_compatible('INTEGER', 'INT'),
            "INTEGER应该与INT兼容"
        )
        
        self.assertTrue(
            self.validator._types_compatible('INT', 'BIGINT'),
            "INT应该与BIGINT兼容"
        )
        
        # 测试VARCHAR和TEXT的兼容性
        self.assertTrue(
            self.validator._types_compatible('VARCHAR', 'TEXT'),
            "VARCHAR应该与TEXT兼容"
        )
        
        self.assertTrue(
            self.validator._types_compatible('TEXT', 'STRING'),
            "TEXT应该与STRING兼容"
        )
        
        # 测试BOOLEAN类型组
        self.assertTrue(
            self.validator._types_compatible('BOOLEAN', 'BOOL'),
            "BOOLEAN应该与BOOL兼容"
        )
        
        self.assertTrue(
            self.validator._types_compatible('BOOL', 'TINYINT(1)'),
            "BOOL应该与TINYINT(1)兼容"
        )
    
    def test_exact_string_matching(self):
        """测试精确字符串匹配"""
        # 测试完全相同的自定义类型
        self.assertTrue(
            self.validator._types_compatible('CUSTOM_TYPE', 'CUSTOM_TYPE'),
            "相同的自定义类型应该匹配"
        )
        
        # 测试不同的自定义类型
        self.assertFalse(
            self.validator._types_compatible('CUSTOM_TYPE_A', 'CUSTOM_TYPE_B'),
            "不同的自定义类型不应该匹配"
        )
    
    def test_whitespace_handling(self):
        """测试空格处理"""
        # 测试带空格的类型名称
        self.assertTrue(
            self.validator._types_compatible('DATE TIME', 'DATETIME'),
            "带空格的类型名称应该被正确处理"
        )
        
        self.assertTrue(
            self.validator._types_compatible('TIME STAMP', 'TIMESTAMP'),
            "带空格的类型名称应该被正确处理"
        )
        
        # 测试不同类型即使去除空格后也不匹配
        self.assertFalse(
            self.validator._types_compatible('DATE TIME', 'TIMESTAMP'),
            "不同类型即使去除空格后也不应该匹配"
        )


if __name__ == '__main__':
    # 运行测试
    unittest.main(verbosity=2)