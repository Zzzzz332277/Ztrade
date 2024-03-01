shape_text = ['十字星', '两只乌鸦', '三只乌鸦', '三内部上涨和下跌', '三线打击',
              '三外部上涨和下跌', '南方三星', '三个白兵', '弃婴', '大敌当前',
              '捉腰带线', '脱离', '收盘缺影线', '藏婴吞没', '反击线'
    , '乌云压顶', '蜻蜓十字/T形十字', '吞噬模式', '十字暮星', '暮星',
              '向上/下跳空并列阳线', '墓碑十字/倒T十字', '锤头', '上吊线', '母子线',
              '十字孕线', '风高浪大线', '陷阱', '修正陷阱', '家鸽',
              '三胞胎乌鸦', '颈内线', '倒锤头', '反冲形态', '由较长缺影线决定的反冲形态',
              '停顿形态', '条形三明治', '探水竿', '跳空并列阴阳线', '插入',
              '三星', '奇特三河床', '向上跳空的两只乌鸦', '上升/下降跳空三法']

emaPeriod = [5, 10, 20, 30, 60, 120, 250]
#ma是为了计算BBI，和普通MA周期不同
maPeriod = [3, 6, 12, 24]
rsiPeriod=[6, 12, 24]
# 存储所有的港股通代码
codeHK = ['0001.HK',
          '0002.HK',
          '0003.HK',
          '0004.HK',
          '0005.HK',
          '0006.HK',
          '0008.HK',
          '0011.HK',
          '0012.HK',
          '0013.HK',
          '0014.HK',
          '0016.HK',
          '0017.HK',
          '0019.HK',
          '0020.HK',
          '0023.HK',
          '0027.HK',
          '0038.HK',
          '0066.HK',
          '0081.HK',
          '0083.HK',
          '0087.HK',
          '0095.HK',
          '0101.HK',
          '0107.HK',
          '0119.HK',
          '0123.HK',
          '0135.HK',
          '0136.HK',
          '0142.HK',
          '0144.HK',
          '0148.HK',
          '0151.HK',
          '0152.HK',
          '0165.HK',
          '0168.HK',
          '0175.HK',
          '0177.HK',
          '0179.HK',
          '0185.HK',
          '0187.HK',
          '0189.HK',
          '0200.HK',
          '0215.HK',
          '0220.HK',
          '0241.HK',
          '0256.HK',
          '0257.HK',
          '0267.HK',
          '0268.HK',
          '0270.HK',
          '0272.HK',
          '0285.HK',
          '0288.HK',
          '0291.HK',
          '0293.HK',
          '0297.HK',
          '0302.HK',
          '0303.HK',
          '0308.HK',
          '0314.HK',
          '0316.HK',
          '0317.HK',
          '0322.HK',
          '0323.HK',
          '0327.HK',
          '0336.HK',
          '0338.HK',
          '0345.HK',
          '0347.HK',
          '0354.HK',
          '0358.HK',
          '0363.HK',
          '0371.HK',
          '0384.HK',
          '0386.HK',
          '0388.HK',
          '0390.HK',
          '0392.HK',
          '0410.HK',
          '0425.HK',
          '0440.HK',
          '0460.HK',
          '0467.HK',
          '0489.HK',
          '0493.HK',
          '0506.HK',
          '0512.HK',
          '0520.HK',
          '0522.HK',
          '0525.HK',
          '0535.HK',
          '0546.HK',
          '0548.HK',
          '0551.HK',
          '0552.HK',
          '0553.HK',
          '0558.HK',
          '0564.HK',
          '0568.HK',
          '0570.HK',
          '0576.HK',
          '0581.HK',
          '0586.HK',
          '0587.HK',
          '0588.HK',
          '0590.HK',
          '0598.HK',
          '0604.HK',
          '0631.HK',
          '0636.HK',
          '0639.HK',
          '0650.HK',
          '0656.HK',
          '0658.HK',
          '0659.HK',
          '0665.HK',
          '0667.HK',
          '0669.HK',
          '0670.HK',
          '0683.HK',
          '0688.HK',
          '0694.HK',
          '0696.HK',
          '0697.HK',
          '0700.HK',
          '0710.HK',
          '0719.HK',
          '0728.HK',
          '0732.HK',
          '0743.HK',
          '0751.HK',
          '0753.HK',
          '0754.HK',
          '0762.HK',
          '0763.HK',
          '0772.HK',
          '0775.HK',
          '0777.HK',
          '0780.HK',
          '0788.HK',
          '0806.HK',
          '0811.HK',
          '0817.HK',
          '0819.HK',
          '0826.HK',
          '0836.HK',
          '0839.HK',
          '0853.HK',
          '0855.HK',
          '0856.HK',
          '0857.HK',
          '0861.HK',
          '0867.HK',
          '0868.HK',
          '0873.HK',
          '0874.HK',
          '0880.HK',
          '0881.HK',
          '0883.HK',
          '0884.HK',
          '0895.HK',
          '0902.HK',
          '0909.HK',
          '0914.HK',
          '0916.HK',
          '0921.HK',
          '0934.HK',
          '0939.HK',
          '0941.HK',
          '0956.HK',
          '0960.HK',
          '0966.HK',
          '0968.HK',
          '0973.HK',
          '0981.HK',
          '0990.HK',
          '0991.HK',
          '0992.HK',
          '0995.HK',
          '0998.HK',
          '1024.HK',
          '1030.HK',
          '1033.HK',
          '1038.HK',
          '1044.HK',
          '1052.HK',
          '1053.HK',
          '1055.HK',
          '1057.HK',
          '1060.HK',
          '1065.HK',
          '1066.HK',
          '1070.HK',
          '1071.HK',
          '1072.HK',
          '1083.HK',
          '1088.HK',
          '1093.HK',
          '1099.HK',
          '1108.HK',
          '1109.HK',
          '1112.HK',
          '1113.HK',
          '1114.HK',
          '1117.HK',
          '1119.HK',
          '1121.HK',
          '1128.HK',
          '1137.HK',
          '1138.HK',
          '1157.HK',
          '1167.HK',
          '1171.HK',
          '1176.HK',
          '1177.HK',
          '1186.HK',
          '1193.HK',
          '1196.HK',
          '1199.HK',
          '1208.HK',
          '1209.HK',
          '1211.HK',
          '1234.HK',
          '1238.HK',
          '1244.HK',
          '1250.HK',
          '1258.HK',
          '1268.HK',
          '1288.HK',
          '1299.HK',
          '1302.HK',
          '1308.HK',
          '1310.HK',
          '1313.HK',
          '1316.HK',
          '1330.HK',
          '1336.HK',
          '1339.HK',
          '1347.HK',
          '1349.HK',
          '1357.HK',
          '1359.HK',
          '1361.HK',
          '1368.HK',
          '1375.HK',
          '1378.HK',
          '1381.HK',
          '1382.HK',
          '1385.HK',
          '1398.HK',
          '1415.HK',
          '1448.HK',
          '1456.HK',
          '1458.HK',
          '1475.HK',
          '1477.HK',
          '1478.HK',
          '1513.HK',
          '1515.HK',
          '1516.HK',
          '1521.HK',
          '1528.HK',
          '1530.HK',
          '1548.HK',
          '1579.HK',
          '1585.HK',
          '1600.HK',
          '1610.HK',
          '1618.HK',
          '1622.HK',
          '1635.HK',
          '1658.HK',
          '1668.HK',
          '1675.HK',
          '1686.HK',
          '1691.HK',
          '1725.HK',
          '1755.HK',
          '1765.HK',
          '1766.HK',
          '1772.HK',
          '1776.HK',
          '1787.HK',
          '1788.HK',
          '1789.HK',
          '1797.HK',
          '1798.HK',
          '1799.HK',
          '1800.HK',
          '1801.HK',
          '1810.HK',
          '1811.HK',
          '1812.HK',
          '1813.HK',
          '1816.HK',
          '1818.HK',
          '1821.HK',
          '1833.HK',
          '1839.HK',
          '1858.HK',
          '1860.HK',
          '1873.HK',
          '1876.HK',
          '1877.HK',
          '1880.HK',
          '1882.HK',
          '1883.HK',
          '1888.HK',
          '1890.HK',
          '1896.HK',
          '1898.HK',
          '1907.HK',
          '1908.HK',
          '1910.HK',
          '1911.HK',
          '1919.HK',
          '1928.HK',
          '1929.HK',
          '1951.HK',
          '1952.HK',
          '1958.HK',
          '1963.HK',
          '1966.HK',
          '1972.HK',
          '1988.HK',
          '1992.HK',
          '1995.HK',
          '1996.HK',
          '1997.HK',
          '1999.HK',
          '2005.HK',
          '2007.HK',
          '2009.HK',
          '2013.HK',
          '2015.HK',
          '2016.HK',
          '2018.HK',
          '2019.HK',
          '2020.HK',
          '2038.HK',
          '2039.HK',
          '2068.HK',
          '2096.HK',
          '2099.HK',
          '2121.HK',
          '2128.HK',
          '2137.HK',
          '2138.HK',
          '2145.HK',
          '2150.HK',
          '2157.HK',
          '2158.HK',
          '2160.HK',
          '2162.HK',
          '2171.HK',
          '2172.HK',
          '2186.HK',
          '2192.HK',
          '2196.HK',
          '2197.HK',
          '2202.HK',
          '2208.HK',
          '2218.HK',
          '2233.HK',
          '2238.HK',
          '2252.HK',
          '2255.HK',
          '2257.HK',
          '2269.HK',
          '2273.HK',
          '2279.HK',
          '2282.HK',
          '2285.HK',
          '2291.HK',
          '2313.HK',
          '2314.HK',
          '2318.HK',
          '2319.HK',
          '2328.HK',
          '2331.HK',
          '2333.HK',
          '2338.HK',
          '2343.HK',
          '2356.HK',
          '2357.HK',
          '2359.HK',
          '2362.HK',
          '2367.HK',
          '2378.HK',
          '2380.HK',
          '2382.HK',
          '2388.HK',
          '2400.HK',
          '2402.HK',
          '2407.HK',
          '2500.HK',
          '2588.HK',
          '2600.HK',
          '2601.HK',
          '2607.HK',
          '2611.HK',
          '2616.HK',
          '2618.HK',
          '2628.HK',
          '2666.HK',
          '2669.HK',
          '2678.HK',
          '2688.HK',
          '2689.HK',
          '2727.HK',
          '2768.HK',
          '2772.HK',
          '2777.HK',
          '2799.HK',
          '2858.HK',
          '2866.HK',
          '2869.HK',
          '2880.HK',
          '2883.HK',
          '2888.HK',
          '2899.HK',
          '3309.HK',
          '3311.HK',
          '3319.HK',
          '3320.HK',
          '3323.HK',
          '3328.HK',
          '3331.HK',
          '3339.HK',
          '3347.HK',
          '3360.HK',
          '3369.HK',
          '3377.HK',
          '3380.HK',
          '3383.HK',
          '3396.HK',
          '3606.HK',
          '3613.HK',
          '3618.HK',
          '3633.HK',
          '3668.HK',
          '3669.HK',
          '3678.HK',
          '3690.HK',
          '3692.HK',
          '3709.HK',
          '3738.HK',
          '3759.HK',
          '3799.HK',
          '3800.HK',
          '3808.HK',
          '3866.HK',
          '3868.HK',
          '3877.HK',
          '3888.HK',
          '3896.HK',
          '3898.HK',
          '3899.HK',
          '3900.HK',
          '3908.HK',
          '3913.HK',
          '3918.HK',
          '3933.HK',
          '3958.HK',
          '3968.HK',
          '3969.HK',
          '3988.HK',
          '3990.HK',
          '3993.HK',
          '3996.HK',
          '3998.HK',
          '6030.HK',
          '6049.HK',
          '6055.HK',
          '6060.HK',
          '6066.HK',
          '6069.HK',
          '6078.HK',
          '6088.HK',
          '6098.HK',
          '6099.HK',
          '6100.HK',
          '6110.HK',
          '6127.HK',
          '6160.HK',
          '6169.HK',
          '6178.HK',
          '6185.HK',
          '6186.HK',
          '6196.HK',
          '6198.HK',
          '6600.HK',
          '6606.HK',
          '6610.HK',
          '6616.HK',
          '6618.HK',
          '6639.HK',
          '6655.HK',
          '6660.HK',
          '6680.HK',
          '6689.HK',
          '6690.HK',
          '6698.HK',
          '6699.HK',
          '6806.HK',
          '6808.HK',
          '6818.HK',
          '6821.HK',
          '6826.HK',
          '6837.HK',
          '6855.HK',
          '6862.HK',
          '6865.HK',
          '6869.HK',
          '6878.HK',
          '6881.HK',
          '6886.HK',
          '6929.HK',
          '6955.HK',
          '6968.HK',
          '6969.HK',
          '6988.HK',
          '6989.HK',
          '6993.HK',
          '9626.HK',
          '9633.HK',
          '9666.HK',
          '9668.HK',
          '9688.HK',
          '9696.HK',
          '9857.HK',
          '9858.HK',
          '9863.HK',
          '9868.HK',
          '9869.HK',
          '9877.HK',
          '9878.HK',
          '9886.HK',
          '9896.HK',
          '9909.HK',
          '9922.HK',
          '9923.HK',
          '9926.HK',
          '9939.HK',
          '9955.HK',
          '9956.HK',
          '9959.HK',
          '9966.HK',
          '9969.HK',
          '9979.HK',
          '9983.HK',
          '9985.HK',
          '9987.HK',
          '9989.HK',
          '9990.HK',
          '9992.HK',
          '9993.HK',
          '9995.HK',
          '9996.HK',
          '9997.HK', ]
