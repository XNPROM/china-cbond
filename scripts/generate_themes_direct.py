"""Generate a first-pass theme JSONL directly from cbond_dataset.json.

This path does not call external LLMs. It infers themes and a broad industry
from the underlying profile text using deterministic keyword rules.
"""
import argparse
import json
import re
from datetime import datetime
from pathlib import Path

from _db import connect, init_schema, upsert as db_upsert


THEME_RULES = [
    ("银行", ["银行", "商业银行"]),
    ("证券", ["证券", "经纪业务", "投行业务", "自营业务", "信用业务", "期货业务"]),
    ("保险", ["保险", "财险", "寿险"]),
    ("电力-火电", ["火力发电", "火电", "热电联产"]),
    ("电力-水电", ["水力发电", "水电"]),
    ("电力-新能源运营", ["风力发电", "光伏发电", "新能源发电", "电站运营", "可再生能源发电", "新能源电力供应商", "电力、热力生产和供应"]),
    ("燃气-水务", ["燃气", "供水", "水务", "供排水"]),
    ("环保-固废水处理", ["垃圾焚烧", "固废", "污水处理", "环卫", "废物处理", "再生资源"]),
    ("航空机场", ["航空运输", "机场", "客运航线", "货运航线", "航空客、货、邮", "客运及客运相关服务", "航空配餐"]),
    ("公路铁路", ["高速公路", "铁路运输", "轨道交通运营"]),
    ("航运-港口", ["港口", "航运", "码头", "集装箱运输"]),
    ("物流-快递", ["物流", "快递", "仓储", "供应链服务"]),
    ("特种纸-造纸", ["造纸", "纸基", "特种纸", "原纸", "包装纸", "纸浆"]),
    ("食品饮料-白酒", ["白酒"]),
    ("食品饮料-大众品", ["乳制品", "饮料", "调味品", "烘焙", "休闲食品", "屠宰", "肉制品", "大众食品", "食品加工", "糖醇", "淀粉糖"]),
    ("农业-种业", ["种子", "制种", "育种"]),
    ("农业-畜禽养殖", ["生猪养殖", "畜禽养殖", "养猪", "养鸡", "养殖业务", "肉猪", "肉鸡", "仔猪", "种猪", "白羽鸡苗", "生猪的养殖销售"]),
    ("农业-饲料-动保", ["饲料", "动保", "兽药"]),
    ("美妆个护", ["化妆品", "护肤品", "个护", "口腔护理"]),
    ("商贸零售", ["商超", "百货", "电商平台", "连锁门店", "零售业务"]),
    ("家电-白电", ["空调", "冰箱", "洗衣机", "白色家电"]),
    ("家电-小家电", ["小家电", "厨电", "清洁电器"]),
    ("轻工-家具家居", ["家具", "家居", "定制家居", "地板", "沙发"]),
    ("纺织服装-家纺", ["家纺", "纺织", "服饰", "服装面料"]),
    ("建材-消费建材", ["防水材料", "涂料", "管材", "五金", "石膏板", "门类产品"]),
    ("建材-水泥玻璃", ["水泥", "玻璃", "浮法玻璃"]),
    ("玻璃基材", ["光伏玻璃", "电子玻璃", "药用玻璃", "药玻"]),
    ("建筑装饰-设计施工", ["建筑设计", "装饰装修", "工程设计", "园林", "钢结构工程", "钢结构", "房屋建筑工程施工"]),
    ("基建-工程", ["基础设施建设", "工程承包", "路桥施工", "公路、市政"]),
    ("地产-开发", ["房地产开发", "地产开发"]),
    ("地产-物管", ["物业管理", "物业服务"]),
    ("基础化工", ["化工产品", "化工原料", "磷化工", "氯碱", "纯碱", "尿素", "己二酸"]),
    ("化工-农药", ["农药", "除草剂", "杀虫剂", "杀菌剂"]),
    ("化工-染料颜料", ["染料", "颜料"]),
    ("新材料-高分子", ["PEEK", "POE", "工程塑料", "高分子材料", "功能膜材料", "PET", "聚酯材料", "尼龙66", "尼龙6", "尼龙切片", "锦纶"]),
    ("新材料-碳纤维复材", ["碳纤维", "碳纤维复合材料"]),
    ("新材料-磁材稀土永磁", ["稀土永磁", "磁材", "钕铁硼"]),
    ("新材料-电子化学品", ["电子化学品", "湿电子化学品", "光刻胶", "电子气体"]),
    ("有色-铜铝铅锌", ["电解铜", "铜冶炼", "铜加工", "铜材", "铜箔", "电解铝", "铝冶炼", "铝加工", "铝材", "铝箔", "铅锌矿", "铅酸电池", "锌冶炼", "锌矿"]),
    ("有色-锂钴镍", ["碳酸锂", "氢氧化锂", "锂矿", "锂辉石", "锂盐", "钴矿", "钴酸锂", "镍矿", "硫酸镍", "三元前驱体"]),
    ("有色-黄金", ["黄金", "金矿"]),
    ("钢铁-特钢", ["特钢", "轴承钢", "不锈钢", "钢材"]),
    ("半导体-前道设备", ["刻蚀", "薄膜沉积", "CMP", "量测", "清洗设备", "光刻"]),
    ("半导体-后道封测", ["封装", "测试设备", "先进封装", "封测"]),
    ("半导体-材料", ["硅片", "光刻胶", "电子气体", "掩模", "抛光液"]),
    ("半导体-零部件", ["石英", "陶瓷部件", "射频电源", "阀门", "真空腔体", "半导体零部件"]),
    ("半导体-设计", ["芯片设计", "IC设计", "集成电路设计"]),
    ("半导体-制造", ["晶圆代工", "IDM", "晶圆制造"]),
    ("功率半导体", ["IGBT", "SiC", "MOSFET", "功率半导体", "功率器件"]),
    ("AI算力-服务器", ["服务器", "算力设备", "整机柜"]),
    ("AI算力-光模块", ["光模块", "硅光", "CPO", "AOC"]),
    ("AI算力-PCB", ["PCB", "印制电路板", "HDI", "载板"]),
    ("AI算力-连接器线缆", ["连接器", "高速线缆", "铜缆", "线束"]),
    ("AI算力-散热液冷", ["液冷", "散热模组", "液冷散热"]),
    ("AI应用", ["人工智能", "AI应用", "大模型", "智能客服"]),
    ("消费电子-苹果链", ["苹果", "Apple"]),
    ("消费电子-安卓链", ["安卓", "智能手机"]),
    ("XR-VR-AR", ["VR", "AR", "MR", "XR"]),
    ("折叠屏", ["折叠屏"]),
    ("面板-OLED-LCD", ["OLED", "LCD", "面板"]),
    ("信创-国产替代", ["国产替代", "操作系统", "数据库", "信创"]),
    ("通信-5G/6G", ["5G", "6G", "通信设备", "基站"]),
    ("卫星互联网", ["卫星互联网"]),
    ("商业航天", ["火箭", "卫星制造", "航天器"]),
    ("低空经济", ["无人机", "eVTOL", "低空经济"]),
    ("光伏-硅料硅片", ["硅料", "硅片"]),
    ("光伏-电池片", ["电池片", "TOPCon", "HJT", "BC电池"]),
    ("光伏-组件", ["光伏组件", "组件制造"]),
    ("光伏-逆变器", ["逆变器"]),
    ("光伏-辅材", ["胶膜", "EVA", "POE胶膜", "焊带", "边框", "光伏玻璃", "银浆"]),
    ("光伏-设备", ["光伏设备", "拉晶设备", "切片设备"]),
    ("储能-系统集成", ["储能系统", "储能集成"]),
    ("储能-电芯", ["储能电芯"]),
    ("动力电池", ["动力电池"]),
    ("电池材料-正极", ["正极材料", "磷酸铁锂", "三元材料"]),
    ("电池材料-负极", ["负极材料", "石墨负极", "硅基负极"]),
    ("电池材料-电解液", ["电解液"]),
    ("电池材料-隔膜", ["隔膜"]),
    ("电池结构件-铜箔铝箔", ["铜箔", "铝箔", "结构件"]),
    ("风电-整机", ["风电整机"]),
    ("风电-零部件", ["风电叶片", "塔筒", "主轴", "风电铸件", "风电轴承"]),
    ("氢能-电解槽-燃料电池", ["燃料电池", "电解槽", "氢能"]),
    ("核电-核能", ["核电", "核能"]),
    ("电网-特高压", ["特高压"]),
    ("电网-配网智能化", ["配网", "电网自动化", "智能电网"]),
    ("电网-变压器", ["变压器"]),
    ("电网-开关电气", ["开关柜", "断路器", "电气设备"]),
    ("虚拟电厂-需求响应", ["虚拟电厂", "需求响应"]),
    ("新能源车-整车", ["整车制造", "汽车整车"]),
    ("汽车零部件-传统", ["汽车零部件", "内饰", "底盘", "制动系统"]),
    ("智能驾驶-ADAS", ["ADAS", "智能驾驶", "辅助驾驶"]),
    ("车载传感器", ["激光雷达", "毫米波雷达", "摄像头模组"]),
    ("汽车电子", ["车载电子", "域控制器", "智能座舱"]),
    ("一体化压铸", ["一体化压铸"]),
    ("军工-信息化", ["雷达", "电子对抗", "军用通信"]),
    ("军工-航空", ["航空发动机", "军机", "机载"]),
    ("军工-航天", ["导弹", "航天"]),
    ("军工-材料", ["高温合金", "钛合金", "军工材料"]),
    ("创新药", ["创新药", "新药研发"]),
    ("CXO-CRO-CDMO", ["CRO", "CDMO", "CXO"]),
    ("原料药", ["原料药", "API"]),
    ("仿制药-特色原料", ["仿制药"]),
    ("医疗器械-高值耗材", ["高值耗材", "植介入"]),
    ("医疗器械-体外诊断IVD", ["体外诊断", "IVD", "诊断试剂", "POCT", "快速诊断", "血糖监测", "血糖仪", "糖化血红蛋白", "传染病检测"]),
    ("医疗器械-影像设备", ["医学影像", "超声设备", "影像设备"]),
    ("中药-中成药", ["中成药", "中药"]),
    ("疫苗", ["疫苗"]),
    ("医美", ["医美", "玻尿酸"]),
    ("血制品", ["血制品"]),
    ("工控自动化", ["工业自动化", "工控", "自动化控制"]),
    ("机器人-本体-人形机器人", ["机器人本体", "人形机器人"]),
    ("机器人-减速器丝杠", ["减速器", "丝杠"]),
    ("工程机械", ["工程机械", "挖掘机", "起重机"]),
    ("机床刀具", ["机床", "刀具"]),
    ("激光设备", ["激光切割", "激光设备", "激光器"]),
    ("3D打印", ["3D打印", "增材制造"]),
    ("出口链", ["出口占比", "外销占比", "海外业务占比", "境外收入占比", "境外销售占比"]),
]

THEME_OVERRIDES = {
    "113610.SH": ["原料药"],
    "113615.SH": ["基建-工程"],
    "113624.SH": ["玻璃基材"],  # 正川股份: 药用玻璃, not 有色/水泥
    "113644.SH": ["工程机械"],
    "113646.SH": ["轻工-家具家居"],  # 永吉股份: 烟标印刷, not 有色
    "113666.SH": ["新能源车-整车"],
    "113677.SH": ["汽车零部件-传统"],
    "113686.SH": ["工控自动化"],
    "113694.SH": ["光伏-辅材", "电力-新能源运营"],
    "113699.SH": ["基建-工程"],
    "113652.SH": ["环保-固废水处理", "电池材料-正极"],  # 伟明环保: 垃圾焚烧+新能源材料, not 有色/证券
    "113661.SH": ["光伏-辅材", "新材料-高分子"],  # 福斯特: 光伏胶膜, not 有色
    "113670.SH": ["轻工-家具家居"],  # 金牌家居: 定制家居, not 有色
    "110090.SH": ["汽车零部件-传统", "电池结构件-铜箔铝箔"],  # 爱柯迪: 汽车铝合金压铸件, not 有色
    "110095.SH": ["光伏-硅料硅片", "光伏-组件"],  # 双良节能: 硅片+组件+节能, drop 电池片/半导体
    "110074.SH": ["新材料-高分子"],  # 精达股份: 电磁线/特种导体, not 有色
    "113042.SH": ["银行"],  # 上海银行: 银行, not 有色-黄金
    "113692.SH": ["汽车零部件-传统", "车载传感器"],  # 保隆科技: TPMS+传感器, not AI散热/XR/军工
    "113697.SH": ["军工-材料", "核电-核能"],  # 应流股份: 高温合金+核电铸件, not 半导体/工程机械
    "118005.SH": ["动力电池"],
    "118007.SH": ["信创-国产替代"],
    "118010.SH": ["医疗器械-高值耗材"],
    "118012.SH": ["创新药"],
    "118015.SH": ["半导体-设计"],
    "118018.SH": ["新材料-高分子"],
    "118020.SH": ["电池材料-正极"],  # 芳源股份: 三元正极前驱体, primary is 电池材料 not 有色
    "118022.SH": ["电池材料-正极"],  # 五矿新能: 正极材料, primary is 电池材料 not 有色
    "118024.SH": ["动力电池"],  # 珠海冠宇: 锂离子电池, not 有色
    "118025.SH": ["医疗器械-影像设备"],
    "118035.SH": ["电网-开关电气"],
    "118037.SH": ["汽车电子"],
    "118038.SH": ["半导体-材料"],
    "118032.SH": ["新材料-高分子"],  # 建龙微纳: 分子筛, not 有色
    "118034.SH": ["光伏-组件", "光伏-硅料硅片"],  # 晶科能源: 组件为主, not 半导体
    "118042.SH": ["光伏-设备"],  # 奥特维: 光伏/锂电设备, not 有色
    "118039.SH": ["电网-配网智能化"],
    "111010.SH": ["半导体-材料", "功率半导体"],  # 立昂微: 半导体硅片+功率器件, drop 光伏/激光
    "113053.SH": ["光伏-硅料硅片", "光伏-组件"],  # 隆基绿能: 硅片+组件, drop 电池片/半导体/氢能
    "113059.SH": ["光伏-辅材", "玻璃基材"],  # 福莱特: 光伏玻璃, not 建材/家具
    "118041.SH": ["基础化工"],
    "118044.SH": ["家电-白电"],
    "118060.SH": ["AI算力-连接器线缆", "汽车零部件-传统"],  # 瑞可达: 连接器, not 散热/半导体
    "118052.SH": ["信创-国产替代"],
    "118056.SH": ["半导体-材料"],
    "118061.SH": ["车载传感器"],
    "118064.SH": ["半导体-材料"],
    "118067.SH": ["汽车电子"],
    "123059.SZ": ["AI算力-服务器"],
    "123065.SZ": ["医疗器械-高值耗材"],
    "123076.SZ": ["新材料-电子化学品"],  # 强力新材: 光刻胶化学品, not AI-PCB/半导体设备
    "123144.SZ": ["新材料-高分子"],  # 裕兴股份: 聚酯薄膜, not 家电/AI/动力电池
    "123085.SZ": ["有色-铜铝铅锌", "电池结构件-铜箔铝箔"],  # 万顺新材: 铝加工 is primary, correct
    "123117.SZ": ["医疗器械-高值耗材"],
    "123124.SZ": ["新材料-电子化学品"],  # 晶瑞电材: 电子化学品/光刻胶, not 有色
    "123131.SZ": ["AI算力-服务器"],
    "123133.SZ": ["食品饮料-大众品"],
    "123142.SZ": ["电网-配网智能化", "机器人-本体-人形机器人"],
    "123149.SZ": ["风电-零部件"],  # 通裕重工: 风电主轴/铸件, not 军工航天/建材
    "123151.SZ": ["医疗器械-影像设备"],
    "123155.SZ": ["风电-零部件"],
    "123154.SZ": ["家电-小家电"],
    "123165.SZ": ["光伏-辅材", "新材料-高分子"],
    "123168.SZ": ["基础化工"],
    "123171.SZ": ["原料药"],
    "123173.SZ": ["信创-国产替代"],
    "123178.SZ": ["原料药"],
    "123180.SZ": ["工程机械"],
    "123183.SZ": ["其他综合"],  # 海顺新材: 药包材, no matching theme in vocabulary
    "123192.SZ": ["美妆个护"],
    "123194.SZ": ["商贸零售"],
    "123195.SZ": ["新材料-高分子"],
    "123196.SZ": ["信创-国产替代"],
    "123211.SZ": ["基础化工"],
    "123214.SZ": ["医疗器械-高值耗材"],
    "123220.SZ": ["医疗器械-体外诊断IVD"],
    "123225.SZ": ["电池材料-负极"],  # 翔丰华: 负极材料, not 有色
    "123236.SZ": ["新材料-高分子"],
    "123241.SZ": ["AI算力-服务器"],
    "123243.SZ": ["环保-固废水处理"],
    "123245.SZ": ["工控自动化"],
    "123176.SZ": ["半导体-前道设备", "面板-OLED-LCD"],  # 精测电子: 半导体+显示检测, not 光伏
    "123251.SZ": ["建筑装饰-设计施工"],
    "123252.SZ": ["有色-铜铝铅锌"],  # 银邦股份: 铝合金复合材料, correct 有色
    "123254.SZ": ["动力电池", "储能-系统集成"],  # 亿纬锂能: 电池制造商, not 有色
    "123255.SZ": ["半导体-材料", "新材料-电子化学品"],  # 鼎龙股份: CMP材料+光刻胶, not 前道/后道设备
    "123256.SZ": ["汽车零部件-传统"],
    "123257.SZ": ["消费电子-安卓链", "出口链"],
    "123260.SZ": ["汽车零部件-传统"],  # 星源卓镁: 镁合金压铸件→汽车, not 有色/半导体
    "123262.SZ": ["AI算力-连接器线缆"],  # 神宇股份: 射频同轴电缆, not 有色-黄金
    "123263.SZ": ["AI应用", "信创-国产替代"],
    "123265.SZ": ["工程机械"],
    "123266.SZ": ["商贸零售"],
    "127026.SZ": ["AI算力-PCB"],  # 超声电子: PCB/覆铜板, not 有色
    "127031.SZ": ["基础化工"],
    "127046.SZ": ["食品饮料-大众品"],
    "127047.SZ": ["建材-消费建材"],
    "127050.SZ": ["汽车零部件-传统"],
    "127053.SZ": ["有色-铜铝铅锌", "建材-消费建材"],  # 豪美新材: 铝型材, both 有色 and 建材 correct
    "127054.SZ": ["工程机械"],
    "127055.SZ": ["建筑装饰-设计施工"],
    "127059.SZ": ["基础化工"],
    "127062.SZ": ["建材-消费建材"],
    "127066.SZ": ["动力电池"],  # 科达利: 电池结构件, not 有色/半导体
    "127082.SZ": ["汽车零部件-传统"],  # 亚太科技: 汽车铝材, primary is 汽车零部件 not 有色
    "127088.SZ": ["新材料-高分子"],
    "127092.SZ": ["工程机械"],
    "127104.SZ": ["消费电子-苹果链"],  # 姚记科技: 扑克/游戏, not 有色-黄金
    "127108.SZ": ["电力-新能源运营"],
    "127111.SZ": ["食品饮料-大众品"],
    "127112.SZ": ["电池材料-负极"],  # 尚太科技: 负极材料, primary is 电池材料 not 有色
    "128108.SZ": ["医疗器械-高值耗材"],
    "128128.SZ": ["基础化工"],
    "128136.SZ": ["消费电子-苹果链", "汽车电子"],
    "128138.SZ": ["环保-固废水处理"],
}

THEME_TO_INDUSTRY = {
    "银行": "银行",
    "证券": "非银金融",
    "保险": "非银金融",
    "电力-火电": "公用事业",
    "电力-水电": "公用事业",
    "电力-新能源运营": "公用事业",
    "燃气-水务": "公用事业",
    "环保-固废水处理": "环保",
    "航空机场": "交通运输",
    "公路铁路": "交通运输",
    "航运-港口": "交通运输",
    "物流-快递": "交通运输",
    "特种纸-造纸": "轻工制造",
    "食品饮料-白酒": "食品饮料",
    "食品饮料-大众品": "食品饮料",
    "农业-种业": "农林牧渔",
    "农业-畜禽养殖": "农林牧渔",
    "农业-饲料-动保": "农林牧渔",
    "美妆个护": "美容护理",
    "商贸零售": "商贸零售",
    "家电-白电": "家用电器",
    "家电-小家电": "家用电器",
    "轻工-家具家居": "轻工制造",
    "纺织服装-家纺": "纺织服饰",
    "建材-消费建材": "建筑材料",
    "建材-水泥玻璃": "建筑材料",
    "玻璃基材": "建筑材料",
    "建筑装饰-设计施工": "建筑装饰",
    "基建-工程": "建筑装饰",
    "地产-开发": "房地产",
    "地产-物管": "房地产",
    "基础化工": "基础化工",
    "化工-农药": "基础化工",
    "化工-染料颜料": "基础化工",
    "新材料-高分子": "基础化工",
    "新材料-碳纤维复材": "基础化工",
    "新材料-磁材稀土永磁": "有色金属",
    "新材料-电子化学品": "电子",
    "有色-铜铝铅锌": "有色金属",
    "有色-锂钴镍": "有色金属",
    "有色-黄金": "有色金属",
    "钢铁-特钢": "钢铁",
    "半导体-前道设备": "电子",
    "半导体-后道封测": "电子",
    "半导体-材料": "电子",
    "半导体-零部件": "电子",
    "半导体-设计": "电子",
    "半导体-制造": "电子",
    "功率半导体": "电子",
    "AI算力-服务器": "电子",
    "AI算力-光模块": "通信",
    "AI算力-PCB": "电子",
    "AI算力-连接器线缆": "电子",
    "AI算力-散热液冷": "机械设备",
    "AI应用": "计算机",
    "消费电子-苹果链": "电子",
    "消费电子-安卓链": "电子",
    "XR-VR-AR": "电子",
    "折叠屏": "电子",
    "面板-OLED-LCD": "电子",
    "信创-国产替代": "计算机",
    "通信-5G/6G": "通信",
    "卫星互联网": "通信",
    "商业航天": "国防军工",
    "低空经济": "机械设备",
    "光伏-硅料硅片": "电力设备",
    "光伏-电池片": "电力设备",
    "光伏-组件": "电力设备",
    "光伏-逆变器": "电力设备",
    "光伏-辅材": "电力设备",
    "光伏-设备": "电力设备",
    "储能-系统集成": "电力设备",
    "储能-电芯": "电力设备",
    "动力电池": "电力设备",
    "电池材料-正极": "电力设备",
    "电池材料-负极": "电力设备",
    "电池材料-电解液": "电力设备",
    "电池材料-隔膜": "电力设备",
    "电池结构件-铜箔铝箔": "电力设备",
    "风电-整机": "电力设备",
    "风电-零部件": "电力设备",
    "氢能-电解槽-燃料电池": "电力设备",
    "核电-核能": "电力设备",
    "电网-特高压": "电力设备",
    "电网-配网智能化": "电力设备",
    "电网-变压器": "电力设备",
    "电网-开关电气": "电力设备",
    "虚拟电厂-需求响应": "电力设备",
    "新能源车-整车": "汽车",
    "汽车零部件-传统": "汽车",
    "智能驾驶-ADAS": "汽车",
    "车载传感器": "汽车",
    "汽车电子": "汽车",
    "一体化压铸": "汽车",
    "军工-信息化": "国防军工",
    "军工-航空": "国防军工",
    "军工-航天": "国防军工",
    "军工-材料": "国防军工",
    "创新药": "医药生物",
    "CXO-CRO-CDMO": "医药生物",
    "原料药": "医药生物",
    "仿制药-特色原料": "医药生物",
    "医疗器械-高值耗材": "医药生物",
    "医疗器械-体外诊断IVD": "医药生物",
    "医疗器械-影像设备": "医药生物",
    "中药-中成药": "医药生物",
    "疫苗": "医药生物",
    "医美": "美容护理",
    "血制品": "医药生物",
    "工控自动化": "机械设备",
    "机器人-本体-人形机器人": "机械设备",
    "机器人-减速器丝杠": "机械设备",
    "工程机械": "机械设备",
    "机床刀具": "机械设备",
    "激光设备": "机械设备",
    "3D打印": "机械设备",
    "出口链": "综合",
}

INDUSTRY_RULES = [
    ("非银金融", ["证券", "基金", "信托", "期货", "保险"]),
    ("银行", ["银行"]),
    ("公用事业", ["发电", "燃气", "水务", "供热"]),
    ("环保", ["固废", "污水处理", "垃圾焚烧", "环卫"]),
    ("交通运输", ["航空运输", "航运", "港口", "物流", "高速公路"]),
    ("轻工制造", ["造纸", "纸基", "家居", "包装"]),
    ("食品饮料", ["乳制品", "食品", "饮料", "白酒"]),
    ("农林牧渔", ["养殖", "饲料", "种子"]),
    ("美容护理", ["化妆品", "护肤品", "医美"]),
    ("家用电器", ["空调", "冰箱", "洗衣机", "小家电"]),
    ("建筑材料", ["玻璃", "水泥", "防水材料"]),
    ("建筑装饰", ["工程设计", "工程承包", "建筑装饰"]),
    ("房地产", ["房地产开发", "物业服务"]),
    ("基础化工", ["化工", "农药", "染料", "工程塑料"]),
    ("有色金属", ["电解铜", "铜加工", "电解铝", "铝加工", "碳酸锂", "锂矿", "钴矿", "镍矿", "金矿"]),
    ("钢铁", ["钢材", "特钢"]),
    ("电子", ["半导体", "芯片", "PCB", "显示面板", "电子元件"]),
    ("计算机", ["软件", "数据库", "操作系统", "人工智能"]),
    ("通信", ["光模块", "通信设备", "卫星互联网", "5G"]),
    ("电力设备", ["光伏", "储能", "电池", "逆变器", "变压器", "电网设备"]),
    ("汽车", ["汽车零部件", "整车", "智能驾驶"]),
    ("国防军工", ["导弹", "航天", "军机", "雷达"]),
    ("医药生物", ["药", "器械", "IVD", "疫苗", "血制品"]),
    ("机械设备", ["机器人", "机床", "工程机械", "自动化设备"]),
]


def _clean_profile(text: str) -> str:
    text = text.replace("\u3000", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", "", text)
    text = text.replace("公司的主营业务是", "主营业务是")
    return text.strip("。；;，, ") + "。"


def _split_sentences(text: str):
    return [s for s in re.split(r"[。；!?]", text) if s]


def _normalize_clause(text: str) -> str:
    text = text.strip("：:，,；;。 ")
    text = re.sub(r"^(公司的|公司|主营业务|主要产品或服务|主要产品)(是|为)?", "", text)
    return text.strip("：:，,；;。 ")


def _extract_clause(text: str, patterns):
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return _normalize_clause(m.group(1))
    return ""


def _extract_business(text: str) -> str:
    clause = _extract_clause(text, [
        r"主营业务(?:是|为)?(.*?)[。；]",
        r"主要从事(.*?)[。；]",
    ])
    if clause:
        return clause
    sents = _split_sentences(text)
    return _normalize_clause(sents[0]) if sents else ""


def _extract_products(text: str) -> str:
    clause = _extract_clause(text, [
        r"主要产品或服务(?:是|为|包括|有)?(.*?)[。；]",
        r"主要产品(?:是|为|包括|有)?(.*?)[。；]",
    ])
    if clause:
        return clause
    sents = _split_sentences(text)
    for sent in sents[1:3]:
        if "产品" in sent or "服务" in sent:
            return _normalize_clause(sent)
    return ""


def _split_items(text: str):
    if not text:
        return []
    normalized = text
    for old, new in [("以及", "、"), ("及", "、"), ("和", "、"), ("或", "、"), ("/", "、"), ("；", "、"), ("，", "、"), ("(", "（"), (")", "）")]:
        normalized = normalized.replace(old, new)
    parts = [p.strip("、,，；; ") for p in normalized.split("、")]
    return [p for p in parts if p]


def _summarize_products(text: str, limit: int = 5) -> str:
    items = _split_items(text)
    if not items:
        return ""
    deduped = []
    for item in items:
        if item not in deduped:
            deduped.append(item)
    brief = []
    for item in deduped:
        if len(brief) >= limit:
            break
        brief.append(item)
    return "、".join(brief)


def _normalize_text_snippet(text: str) -> str:
    text = text.strip("：:，,；;。 ")
    text = re.sub(r"^(公司|公司的|产品|业务|服务)(在|已|于|曾)?", "", text)
    return text.strip("：:，,；;。 ")


def _extract_by_patterns(text: str, patterns):
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            clause = _normalize_text_snippet(m.group(1))
            if clause:
                return clause
    return ""


def _extract_application(text: str) -> str:
    return _extract_by_patterns(text, [
        r"(?:主要|产品)?(?:广泛)?应用于(.*?)[。；]",
        r"下游(?:应用|覆盖)(.*?)[。；]",
        r"主要用于(.*?)[。；]",
        r"用于(.*?)[。；]",
        r"服务于(.*?)[。；]",
    ])


def _extract_customers(text: str) -> str:
    return _extract_by_patterns(text, [
        r"客户(?:群体|类型|覆盖|包括|主要为)?(.*?)[。；]",
        r"下游客户(?:包括|覆盖|主要为)?(.*?)[。；]",
        r"面向(.*?客户.*?)[。；]",
    ])


def _extract_position_evidence(text: str) -> str:
    checks = [
        ("龙头", "公司在细分领域具备龙头地位。"),
        ("制造业单项冠军", "公司获评制造业单项冠军。"),
        ("国家级专精特新小巨人", "公司为国家级专精特新小巨人企业。"),
        ("专精特新小巨人", "公司为专精特新小巨人企业。"),
        ("隐形冠军", "公司在细分赛道具备隐形冠军特征。"),
        ("单项冠军", "公司在细分产品领域具备较强竞争力。"),
        ("国家企业技术中心", "公司拥有国家企业技术中心等研发平台。"),
        ("工程研究中心", "公司拥有较强研发与工程化平台。"),
        ("鲁班奖", "公司在工程交付与项目经验方面积累较深。"),
        ("詹天佑奖", "公司在工程交付与项目经验方面积累较深。"),
        ("红点设计奖", "公司产品设计与品牌化能力较强。"),
        ("iF）设计奖", "公司产品设计与品牌化能力较强。"),
        ("iF设计奖", "公司产品设计与品牌化能力较强。"),
        ("领先", "公司在细分赛道处于行业较前列位置。"),
        ("第一名", "公司在细分赛道处于行业较前列位置。"),
        ("第一", "公司在细分赛道处于行业较前列位置。"),
    ]
    for needle, sentence in checks:
        if needle in text:
            return sentence
    return ""


def _infer_downstream(primary_theme: str, business: str, products: str) -> str:
    return ""


def _infer_position(text: str, primary_theme: str) -> str:
    evidence = _extract_position_evidence(text)
    return evidence if evidence else ""


def _build_business_rewrite(uname: str, profile: str, themes):
    primary_theme = themes[0] if themes else "其他综合"
    business = _extract_business(profile)
    products = _extract_products(profile)
    product_summary = _summarize_products(products)
    application = _extract_application(profile)
    customers = _extract_customers(profile)

    sentences = []
    if business and business != uname:
        sentences.append(f"{uname}主营{business}。")

    if product_summary:
        sentences.append(f"核心产品包括{product_summary}等。")
    elif products:
        sentences.append(f"核心产品和服务覆盖{products}。")

    if application:
        sentences.append(f"产品主要应用于{application}。")
    elif customers:
        sentences.append(f"客户主要覆盖{customers}。")

    position = _infer_position(profile, primary_theme)
    if position:
        sentences.append(position)
    if not sentences and business:
        sentences.append(f"{uname}主营{business}。")
    return "".join(sentence for sentence in sentences if sentence)


def _score_themes(text: str):
    scores = []
    for theme, keywords in THEME_RULES:
        hits = sum(1 for kw in keywords if kw in text)
        if hits > 0:
            scores.append((theme, hits))
    scores.sort(key=lambda x: (-x[1], x[0]))
    ordered = []
    for theme, _ in scores:
        if theme not in ordered:
            ordered.append(theme)
    if not ordered:
        ordered = ["其他综合"]
    return ordered[:4]


def _resolve_themes(code: str, text: str):
    if code in THEME_OVERRIDES:
        return THEME_OVERRIDES[code]
    return _score_themes(text)


def _infer_industry(text: str, themes):
    primary = themes[0] if themes else ""
    if primary in THEME_TO_INDUSTRY:
        return THEME_TO_INDUSTRY[primary]
    for industry, keywords in INDUSTRY_RULES:
        if any(kw in text for kw in keywords):
            return industry
    return "综合"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--trade-date", default="", help="YYYY-MM-DD; when set, save to DuckDB incrementally")
    ap.add_argument("--save-every", type=int, default=40, help="rows per incremental DB save")
    ap.add_argument("--progress-log", default="", help="optional progress log path")
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = Path(args.progress_log) if args.progress_log else out_path.with_name(out_path.stem + "_rewrite_progress.jsonl")
    progress_path.parent.mkdir(parents=True, exist_ok=True)

    existing_meta = {}
    con = None
    if args.trade_date:
        con = connect()
        init_schema(con)
        rows_raw = con.execute(
            "SELECT code, all_themes_json, industry FROM themes WHERE trade_date = ?",
            [args.trade_date]
        ).fetchall()
        existing_meta = {
            code: {
                "themes": json.loads(all_themes_json or "[]"),
                "industry": industry or "",
            }
            for code, all_themes_json, industry in rows_raw
        }

    batch_rows = []
    processed = 0
    with out_path.open("w", encoding="utf-8") as out, progress_path.open("a", encoding="utf-8") as progress:
        for item in dataset["items"]:
            profile = _clean_profile(item.get("profile", "") or item["uname"])
            meta = existing_meta.get(item["code"], {})
            # Priority: DB themes (from Shenwan via fetch_cb_universe.py) > keyword rules
            themes = meta.get("themes")
            if not themes or themes == ["其他综合"]:
                themes = _resolve_themes(item["code"], profile)
            # Priority: DB industry (from Shenwan) > inferred
            industry = meta.get("industry") or (item.get("industry") or "").strip() or _infer_industry(profile, themes)
            row = {
                "code": item["code"],
                "industry": industry,
                "business_rewrite": _build_business_rewrite(item["uname"], profile, themes),
                "themes": themes,
            }
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            out.flush()
            batch_rows.append({
                "trade_date": args.trade_date,
                "code": row["code"],
                "theme_l1": themes[0] if themes else "",
                "all_themes_json": json.dumps(themes, ensure_ascii=False),
                "business_rewrite": row["business_rewrite"],
                "industry": industry,
            })
            processed += 1

            if con and len(batch_rows) >= args.save_every:
                n = db_upsert(con, "themes", batch_rows, ["trade_date", "code"])
                progress.write(json.dumps({
                    "ts": datetime.utcnow().isoformat(),
                    "trade_date": args.trade_date,
                    "processed": processed,
                    "saved_rows": n,
                    "status": "partial_saved",
                }, ensure_ascii=False) + "\n")
                progress.flush()
                print(f"[save] {processed}/{dataset['count']} rows saved to DB")
                batch_rows = []

        if con and batch_rows:
            n = db_upsert(con, "themes", batch_rows, ["trade_date", "code"])
            progress.write(json.dumps({
                "ts": datetime.utcnow().isoformat(),
                "trade_date": args.trade_date,
                "processed": processed,
                "saved_rows": n,
                "status": "final_saved",
            }, ensure_ascii=False) + "\n")
            progress.flush()
            print(f"[save] {processed}/{dataset['count']} rows saved to DB")

    if con:
        con.close()
    print(f"[done] {dataset['count']} rows → {args.out}")


if __name__ == "__main__":
    main()
