# iFinD 可转债字段映射表

来源：iFinD 基础函数 THS_BD 接口文档，2026-04-23 确认。

## 关键字段（本次新增需求用）

### 强赎
| 中文名 | iFinD 字段 | 说明 |
|---|---|---|
| 赎回条款 | `ths_redemp_clause_cbond` | 全文 |
| 赎回触发比例 | `ths_redemp_trigger_ratio_cbond` | 如 130% |
| 赎回触发价 | `ths_redemp_trigger_price_cbond` | 2026-04-23 |
| 不强赎提示公告日 | `ths_not_compulsory_redemp_indicative_date_bond` | 2026-04-23 |
| 不强赎提示起始日 | `ths_not_compulsory_redemp_startdate_cbond` | 2026-04-23 |
| 不强赎提示截止日 | `ths_not_compulsory_redemp_enddate_cbond_bond` | 2026-04-23 |
| 条件赎回累计触发天数 | `ths_conditionalredemption_triggercumulativedays_cbond` | 2026-04-23 |
| 是否有时点赎回条款 | `ths_is_redemp_clause_of_time_cbond` | |

### 下修
| 中文名 | iFinD 字段 | 说明 |
|---|---|---|
| 是否有特别向下修正条款 | `ths_is_special_down_correct_clause_cbond` | |
| 特别向下修正条款全文 | `ths_special_modified_clause_cbond` | |
| 特别修正起始时间 | `ths_special_correction_start_time_cbond` | |
| 特别修正结束时间 | `ths_special_correction_end_time_cbond` | |
| 触发比例 | `ths_trigger_ratio_cbond` | 下修触发比例 |
| 特别修正幅度 | `ths_special_correction_range_cbond` | |
| 修正价格底线说明 | `ths_correct_pricedl_explain_cbond` | |
| 修正次数限制 | `ths_correct_times_limit_cbond` | |

### 正股估值
| 中文名 | iFinD 字段 | 说明 |
|---|---|---|
| 正股市盈率 | `ths_stock_pe_new_cbond` | 2026-04-23 |
| 正股市净率 | `ths_stock_pb_cbond` | 2026-04-23 |
| 转股市盈率 | `ths_conver_pe_cbond` | 2026-04-23 |
| 转股市净率 | `ths_conver_pb_cbond` | 2026-04-23 |

### 行业
| 中文名 | iFinD 字段 | 说明 |
|---|---|---|
| 所属同花顺行业 | `ths_the_ths_industry_cbond` | |
| 所属同花顺行业代码 | `ths_ssthshydm_cbond` | |

### 转股相关
| 中文名 | iFinD 字段 | 说明 |
|---|---|---|
| 转股价 | `ths_conversion_price_cbond` | 2026-04-23 |
| 转股比例 | `ths_conversion_ratio_cbond` | |
| 转换价值 | `ths_transfer_value_cbond` | 2026-04-23 |

## 已在使用的字段

| 中文名 | iFinD 字段 |
|---|---|
| 转债代码 | `ths_convertible_debt_code_cbond` |
| 转债简称 | `ths_convertible_debt_short_name_cbond` |
| 正股代码 | `ths_stock_code_cbond` |
| 正股简称 | `ths_stock_short_name_cbond` |
| 上市日期 | `ths_listed_date_cbond` |
| 终止上市日期 | `ths_stop_listing_date_bond` |
| 债券余额 | `ths_bond_balance_cbond` |
| 到期日期 | `ths_maturity_date_cbond` |
| 发行信用评级 | `ths_issue_credit_rating_cbond` |
| 转股溢价率 | `ths_conversion_premium_rate_cbond` |
| 纯债溢价率 | `ths_pure_bond_premium_rate_cbond` |
| 纯债价值 | `ths_pure_bond_value_cbond` |
| 隐含波动率 | `ths_implied_volatility_cbond` |
| 双低 | `ths_convertible_debt_doublelow_cbond` |

## 完整字段列表

```
转债代码;转债简称;正股代码;正股简称;上市日期;终止上市日期;上市地点;发行人中文名称;发行人英文名称;所属同花顺行业;所属同花顺行业代码;转债面值;发行期限;发行价格;发行总额;债券余额;起息日期;到期日期;利率类型;发行利率;年付息次数;发行信用评级;信用评估机构;担保人;担保方式;担保期限;担保范围;反担保情况;主承销商;副主承销商;分销商;上市推荐人;资产评估机构;会计师事务所;律师事务所;债权代理人;转股代码;转股简称;转股条款;转股价格;转股价格调整日期;转换比例;转股起始日期;转股终止日期;相对转股期;是否强制转股;强制转股日;强制转股价格;未转股余额;未转股比例;转股价随派息调整;票面利率说明;付息说明;补偿利率;补偿利率(公布);补偿利率说明;是否随存款利率调整;是否有利息补偿;利息补偿是否包含当期利息;条件回售条款全文;回售价格说明;有条件回售价;条件回售起始日期;条件回售截止日期;相对回售期;回售触发计算最大时间区间;回售触发计算时间区间;回售触发比例;回售触发价;回售价格;回售公告日;回售登记日;每年回售次数;利息处理;无条件回售期;无条件回售起始日期;无条件回售结束日期;无条件回售价;无条件回售条款;时点回售数;时点回售条款全文;附加回售价格说明;附加回售条件;赎回条款;赎回价格说明;有条件赎回价;到期赎回价;条件赎回起始日期;条件赎回截止日期;赎回停止交易日;赎回停止转股日;发行人资金到账日;相对赎回期;每年可赎回次数;赎回触发计算最大时间区间;赎回触发计算时间区间;赎回触发比例;赎回触发价;赎回价格;赎回公告日;赎回登记日;不强赎提示公告日;不强赎提示起始日;不强赎提示截止日;利息处理;是否有时点赎回条款;时点赎回数;时点赎回条款全文;是否有特别向下修正条款;特别向下修正条款全文;特别修正起始时间;特别修正结束时间;向下修正触发计算最大时间区间;向下修正触发计算时间区间;触发比例;是否为算术平均价;特别修正幅度;修正价格底线说明;修正次数限制;时点修正条款全文;发行公告日;发行方式;发行费用;...
```
