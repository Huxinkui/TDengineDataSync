# TDengineDataSync

个人开发，仅供学习参考，请勿用于商业用途,基于 GCC8.3.1。

## 使用前提
-TDengin 需要提前创建好数据库

## 功能

- 同步TDengine数据到新的TDengine实例
- 支持多线程同步
- 支持断点续传
- 支持2.6数据迁移到3.0
- 配置文件 stable_define.json 根据超表定义进行同步
[
    {
        "_comment":"TDengine stableName必填 columns必填 并且作为主键的列必须在columns中第一个,并且总列数必须大于等于2，tags可选",
        "stableName":"stb_name",  //TDengine超表名 ，必填
        "columns":[ //列名称和列类型，必填且长度大于2
            {
                "columnName":"updatetime",
                "columnType":"TIMESTAMP"
            },
            {
                "columnName":"value",
                "columnType":"double"
            },
            {
                "columnName":"status",
                "columnType":"BINARY(128)"
            }
        ],
        "tags":[ // tag名称和tag类型，可选
            {
                "tagName":"tag1",
                "tagType":"BINARY(128)"
            },
            {
                "tagName":"tag2",
                "tagType":"BINARY(128)"
            }
        ]

    }
]
