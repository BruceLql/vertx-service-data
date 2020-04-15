### 个人信息表 表名：carrier_baseinfo

| 移动         | 备注           | 联通        | 备注        |电信        | 备注       | Mysql       |备注       |
| ------------ | -----------   | ----        | ---------- |------------| --------  | --------    | -------- |
| name         |  用户名      |custName     |   用户名     |            |           | name        | 用户名    |
| status       |  用户状态    |subscrbstat  |   用户状态   |            |           | state       | 账户状态  |            
| level        |  用户级别    |custlvl      |   用户级别   |            |           |             |           |
| brand        |  所属品牌   |brand_name   |   所属品牌   |            |            | brand       | 所属品牌   |
|              |  当前套餐   |packageName  |   当前套餐   |            |            | package_name|  套餐名称  |
| inNetDate    |  入网时间   |opendate     |   入网时间   |            |           |  in_net_date |  入网时间  |
| netAge       |  网龄      |根据opendate计算|   网龄       |            |           |   net_age   |  网龄  |
| realNameInfo |  实名认证   |             |   实名认证   |            |           | reliability| 实名认证  |
| starLevel    |  星级      |             |   星级       |            |           |  level    | 星级       |
| starScore    |  星级得分   |             |   星级得分   |            |           | star_score | 星级得分   |
| starTime     |  星级有效期 |             |  星级有效期  |            |           |           |           |
| mobile       |  联系电话   | usernumber  |   联系电话   |            |           | mobile   | 联系电话   |
| email        |  电子邮箱   |             |   电子邮箱   |            |           | user_email| 电子邮箱  |
| zipCode      |  邮政编码   |             |   邮政编码   |            |           | zip_code | 邮政编码  |
| address      |  联系地址   |  certaddr   |   联系地址   |            |           | user_address| 联系地址  |
|              |  归属地     | certaddr    |   归属地     |            |           |           |           |
|              |  通话级别   | landlvl     |   通话级别   |            |           |           |           |
|              |  证件号码   | certnum     |   证件号码   |            |           |  idcard   |           |
|              |  账单户名   | managername |   账单户名   |            |           |           |           |
|              |  付费方式   | paytype     |   付费方式   |            |           |           |           |
|              |  寄存地址   | sendaddr    |   寄存地址   |            |           |           |           |
|              |  服务状态   | sendpost    |   服务状态   |            |           |           |           |
|              |            |             |             |            |           |  carrier  | 运营商（CMCC 移动、CUCC联通、CTCC电信） |
|              |            |             |             |            |           |  province | 省份      |
|              |            |             |             |            |           |  city     | 城市      |
|              |            |             |             |            |           |  delete_at| 删除状态 （0：正常;有时间表示删除）|
|              |            |             |             |            |           |  create_at | 创建时间 |
|              |            |             |             |            |           |  update_at | 更新时间   |
|              |            |             |             |            |           |  id       |  主键      |

- CUCC
```CUCC
{
    "data": [
        {
            "name":"孙乾翔",
            "realNameInfo":"开通",
            "level":"二星忠诚用户",
            "brand":"沃4G后付费",
            "packageName":"4G主副卡业务-语音副卡基本套餐",
            "opendate":"20080623175916",
            "certaddr":"河南**********",
            "landlvl":"港澳台通话",
            "certnum":"3303****0038",
            "managername":"",
            "paytype ":"现金",
            "sendaddr":"",
            "sendpost":""
        }
    ]
}
url = "https://iservice.10010.com/e3/static/query/searchPerInfo/?_=1586432154491"
referer = "https://iservice.10010.com/e4/query/bill/call_dan-iframe.html?_=1586432092049"
```
- CMCC
```CMCC
{
    "data":{
        "remark":null,
        "name":"x*",
        "brand":"01",
        "level":"100",
        "status":"00",
        "inNetDate":"20200106170002",
        "netAge":"3个月",
        "email":"",
        "address":"",
        "zipCode":"650000",
        "contactNum":"",
        "starLevel":"0",
        "starScore":"0",
        "starTime":null,
        "realNameInfo":"2",
        "vipInfo":null,
        "inNetDay":null,
        "gsmState":"7",
        "birthday":"19971012"
    },
    "retCode":"000000",
    "retMsg":"ok",
    "sOperTime":"20200325144800"
}

```


### 通话详单

MYSQL>>通话详单

| 移动字段名    | 联通字段(返回数据中的名字)        | 电信字段 | 注释                      |
| ------------- | --------------------------------- | -------- | ------------------------- |
| remark        |                                   |          |                           |
| startTime     | calldate(日期)/calltime(精确到秒) |          | 开始时间                  |
| commPlac      | homeareaName                      |          | 通信地点                  |
| commMode      | calltypeName                      |          | 通信方式(主叫/被叫)       |
| anotherNm     | othernum                          |          | 对方号码                  |
| commTime      | calllonghour                      |          | 通话时长                  |
| commType      | landtype                          |          | 通信类型(国内异地主叫/--) |
|               | calledhome                        |          | 对方归属地                |
|               | thtypeName                        |          | 业务类型                  |
| mealFavorable |                                   |          | 套餐优惠                  |
| commFee       | totalfee                          |          | 通话费用                  |
| totalNum      |                                   |          |                           |
| startDate     |                                   |          | 起始时间                  |
| endDate       |                                   |          | 结束时间                  |
| curCuror      |                                   |          |                           |
| retMsg        | isSuccess                         |          | 返回状态(成功/失败)       |
|               | callTotaltime                     |          | 总通话时长                |

```python
a = {
    "data":[
        {
            "startTime":"2020-04-09 17:59:14",
            "commPlac":"上海",
            "commMode":"被叫",
            "anotherNm":"15517116999",
            "commTime":"56秒",
            "commType":"国内通话",
            "calledhome":"河南郑州",
            "thtypeName":"语音电话",
            "totalfee":"0.00",
            "commTime":"56秒",
        }
    ],
    "retMsg":"true",
    "callTotaltime":"1小时59分钟41秒"
}

url = "https://iservice.10010.com/e3/static/query/callDetail?_=1586432156748&accessURL=https://iservice.10010.com/e4/query/bill/call_dan-iframe.html?_=1586432092049&menuid=000100030001"
referer = "https://iservice.10010.com/e4/query/bill/call_dan-iframe.html?_=1586432092049"
FormData = {
    "pageNo":"1",
    "pageSize":"20",
    "beginDate":"20200401",
    "endDate":"20200409"
}
```



### 短信表

MYSQL>>短彩信详单

| 移动      | 联通            | 电信 | 注释                                             |
| --------- | --------------- | ---- | ------------------------------------------------ |
| startTime | smsdate/smstime |      | 起始时间                                         |
| commPlac  |                 |      | 通信地点                                         |
| anotherNm | othernum        |      | 对方号码                                         |
| infoType  | smstype(1接收)  |      | 通信方式/传送方式                                |
| busiName  |                 |      |                                                  |
| meal      |                 |      | 业务名称                                         |
| commFee   | fee             |      | 费用                                             |
| commMode  |                 |      | 信息类型                                         |
|           | businesstype    |      | 业务类型(01国内短信/02国际短信/03国内彩信)(联通) |

```python
"""

"""
```






### 充值记录

| 移动         | 联通       | 备注               |
| ------------ | ---------- | ------------------ |
| payDate      | paydate    | 日期               |
| payFee       | payfee     | 金额               |
| payTypeName  | payment    | 交费渠道、储值方式 |
| payStaffCode | paychannel | 交费方式           |

```python
a = {
    "data": [
        {
            "paydata": "2020-04-04 05:34:37",
            "payfee": "10.00",
            "payment": "总部电子渠道缴费财付通",
            "paychannel": "ECS-WAP"
        }
    ],
    "success": "true",
    "respCode": "0000",
    "respDesc": "成功"
}
url = "https://iservice.10010.com/e3/static/query/paymentRecord?_=1586485602013&accessURL=https://iservice.10010.com/e4/query/calls/paid_record-iframe.html"
Referer = "https://iservice.10010.com/e4/query/calls/paid_record.html?menuId=000100010003"
formdata = {
    "pageNo":"1",
    "pageSize":"20",
    "beginDate":"20200401",
    "endDate":"20200410"
}
```



### 上网详情表

| 移动        | 联通                          | 备注     |
| ----------- | ----------------------------- | -------- |
| startTime   | begindate+begintime           | 起始时间 |
| commPlac    | homearea                      | 通信地点 |
| netPlayType | roamstatformat(国际漫游/国内) | 上网方式 |
| netType     | nettypeformat                 | 网络类型 |
| commTime    | longhour                      | 总时长   |
| sumFlow     | pertotalsm                    | 总流量   |
| meal        | deratefee                     | 套餐优惠 |
| commFee     | totalfee                      | 总费用   |

```python
a = {
    "data": [
        {
            "startTime":"20200408081801",
            "commPlac":"上海",
            "netPlayType":"国际漫游",
            "netType":"4G网络",
            "commTime":"2176",
            "sumFlow":"307.30",
            "meal":"0.09",
            "commFee":"0.00"
        }
    ],
    "querytime":"2020年04月10日 10:44:36",
    "rspcode":"0000",
    "rspdesc":"OK"
}
url = "https://iservice.10010.com/e3/static/query/callFlow?_=1586486676622&accessURL=https://iservice.10010.com/e4/query/basic/call_flow_iframe1.html&menuid=000100030004"
Referer = "https://iservice.10010.com/e4/query/basic/call_flow_iframe1.html"
formdata = {
    "pageNo":"1",
    "pageSize":"20",
    "beginDate":"2020-04-08",
    "endDate":"2020-04-08"
}
```

### 账户余额(当前号码)

| 移动        | 联通        | 备注     |
| ----------- | ----------- | -------- |
| curFee      | realbalance | 可用余额 |
| curFeeTotal | acctBalance | 总余额   |
| myIntegral  |             | 我的积分 |
| realFee     | realFee     | 当月话费 |

```python
a = {
    "data": [
        {
            "curFee":"222.73",
            "curFeeTotal":"232.73",
            "realFee":"10.00"
        }
    ],
    "success":"true"
}
url = "https://iservice.10010.com/e3/static/realtimewo/accountbalancewo?_=1586488077806"
Referer = "https://iservice.10010.com/e4/skip.html?menuCode=000100010002&1586487884100"
formdta = {"chargetype":"1"}
```

### 消费记录

| 移动          | 联通    | 备注     |
| ------------- | ------- | -------- |
| billMonth     | cycleId | 时间     |
| billStartDate |         | 开始时间 |
| billEndDate   |         | 结束时间 |
| billFee       | allFee  | 金额     |

```python
a = {
    "data":[
        {
            "billMonth":"202003",
            "billStartDate":"需要根据billMonth自己做判断",
            "billEndDate":"需要根据billMonth自己做判断",
            "billFee":"139.00"
        }
    ],
    "success":"true"
}
url = "https://iservice.10010.com/e3/static/wohistory/bill?dat=&_=1586488775019"
Referer = "https://iservice.10010.com/e4/skip.html?menuCode=000100020001"
formdata = {
    "chargetype":""
}
```




### 通知记录表  表名：carrier_notice_records

| 字段名          | 名称    | 备注     |
| ------------- | ------- | -------- |
| id            | 主键    | 自增id      |
| mobile        | 手机号  | 开始时间  |
| task_id       | 任务ID | mongo 会话表id  |
| back_url      | 回调地址  | 数据推送地址     |
| create_at     | 创建时间  |       |
| update_at     | 更新时间  |      |
| delete_at     | 删除时间  | 0：正常； 时间：删除时间     |



### 查询记录流水表  表名：carrier_query_records

| 字段名          | 名称    | 备注     |
| ------------- | ------- | -------- |
| id            | 主键    | 自增id      |
| mobile        | 手机号  | 开始时间  |
| task_id       | 任务ID | mongo 会话表id  |
| type          | 获取数据的方式 | 1:推送 2：主动查询   |
| status      | 状态  | 0：success  1:error |
| message      | 说明  | "success";"异常信息" |
| result      | 查询结果  | 完整数据 |
| create_at     | 创建时间  |       |
| update_at     | 更新时间  |      |
| delete_at     | 删除时间  | 0：正常； 时间：删除时间     |


### 分析结果数据表  表名：carrier_result_data

| 字段名          | 名称    | 备注     |
| ------------- | ------- | -------- |
| id            | 主键    | 自增id    |
| mobile        | 手机号  | 开始时间  |
| task_id       | 任务ID | mongo 会话表id  |
| item      | 数据项  | 基本信息："user_basic";<br> 朋友圈分析：friend_circle ;<br>短信联系详情统计分析：sms_contact_detail;<br>电话风险分析：call_risk_analysis;<br>电话行为分析：cell_behavior;<br> 电话联系详情统计分析：call_contact_detail;<br>基本检查项分析：basic_check_items;<br>联系区域分析：contact_region;<br>分析报告：report;<br> 行为核查分析：behavior_check;<br>手机号账户信息分析：cell_phone;<br>|
| result      | 单项数据  | 单项数据结果 |
| create_at     | 创建时间  |       |
| update_at     | 更新时间  |      |
| delete_at     | 删除时间  | 0：正常； 时间：删除时间 |


