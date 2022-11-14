# Databricks notebook source
src__2019_olist_item_sales = """
ds,y
2019/01/01,14012.84403
2019/01/02,10648.10398
2019/01/03,15725.20802
2019/01/04,22366.36775
2019/01/05,13789.55999
2019/01/06,17383.06802
2019/01/07,17567.71191
2019/01/08,18553.81211
2019/01/09,21647.82008
2019/01/10,20281.5959
2019/01/11,23334.69584
2019/01/12,23437.68
2019/01/13,22501.45203
2019/01/14,15507.8279
2019/01/15,10992.29989
2019/01/16,18497.65208
2019/01/17,22112.98789
2019/01/18,26277.98395
2019/01/19,23882.1599
2019/01/20,22395.07188
2019/01/21,21660.86402
2019/01/22,13654.64389
2019/01/23,22837.42798
2019/01/24,31388.49594
2019/01/25,22131.33589
2019/01/26,29800.75218
2019/01/27,19266.00002
2019/01/28,13499.71206
2019/01/29,15887.20786
2019/01/30,12281.88
2019/01/31,23173.69215
2019/02/01,16617.88801
2019/02/02,24375.37189
2019/02/03,25083.8041
2019/02/04,24602.88003
2019/02/05,12750.93598
2019/02/06,18389.42405
2019/02/07,29382.01192
2019/02/08,17842.66795
2019/02/09,17209.76405
2019/02/10,22475.60403
2019/02/11,20498.41202
2019/02/12,10714.71596
2019/02/13,18348.19211
2019/02/14,16944.74407
2019/02/15,18602.17216
2019/02/16,24693.01206
2019/02/17,15728.67605
2019/02/18,16945.27197
2019/02/19,13813.2961
2019/02/20,20212.41617
2019/02/21,27915.97191
2019/02/22,14059.18799
2019/02/23,8467.511781
2019/02/24,11536.00799
2019/02/25,17338.98009
2019/02/26,10962.58813
2019/02/27,23234.86793
2019/02/28,16081.17597
2019/03/01,21750.66009
2019/03/02,18365.59198
2019/03/03,15849.31199
2019/03/04,17243.32807
2019/03/05,13969.88407
2019/03/06,14636.01609
2019/03/07,19646.61584
2019/03/08,20177.89203
2019/03/09,26500.37999
2019/03/10,26396.07598
2019/03/11,16952.92806
2019/03/12,13643.556
2019/03/13,11125.17606
2019/03/14,22272.89996
2019/03/15,24698.24393
2019/03/16,24569.20806
2019/03/17,18782.12398
2019/03/18,24132.83992
2019/03/19,14582.74802
2019/03/20,15747.38407
2019/03/21,26944.57198
2019/03/22,34315.27208
2019/03/23,19642.66795
2019/03/24,22271.87994
2019/03/25,15412.896
2019/03/26,14088.83992
2019/03/27,16085.028
2019/03/28,23665.24785
2019/03/29,21290.16007
2019/03/30,14212.24806
2019/03/31,20282.484
2019/04/01,23084.4239
2019/04/02,18960.56402
2019/04/03,15795.9479
2019/04/04,22599.81606
2019/04/05,26117.0279
2019/04/06,22740.75607
2019/04/07,23696.20808
2019/04/08,23564.72403
2019/04/09,16217.70001
2019/04/10,17306.54406
2019/04/11,28478.14798
2019/04/12,24059.29196
2019/04/13,23796.3839
2019/04/14,25379.89197
2019/04/15,24563.77218
2019/04/16,15175.00795
2019/04/17,14468.00396
2019/04/18,23328.24009
2019/04/19,30094.15206
2019/04/20,25402.46403
2019/04/21,23472.81601
2019/04/22,21659.13615
2019/04/23,16191.24003
2019/04/24,16560.76795
2019/04/25,26009.75982
2019/04/26,21763.15208
2019/04/27,16002.58802
2019/04/28,17416.51199
2019/04/29,18713.17184
2019/04/30,13520.21999
2019/05/01,17085.31204
2019/05/02,27854.31591
2019/05/03,42411.30003
2019/05/04,37547.31609
2019/05/05,23924.20772
2019/05/06,34690.29618
2019/05/07,21875.38809
2019/05/08,33030.17977
2019/05/09,32066.16009
2019/05/10,25958.14792
2019/05/11,31413.97204
2019/05/12,21853.77591
2019/05/13,14671.07994
2019/05/14,18048.87596
2019/05/15,22328.23197
2019/05/16,30298.37999
2019/05/17,30963.87602
2019/05/18,35591.98795
2019/05/19,27813.828
2019/05/20,24032.43581
2019/05/21,24938.11193
2019/05/22,26299.00774
2019/05/23,32352.36003
2019/05/24,24621.63609
2019/05/25,28563.34785
2019/05/26,23179.01998
2019/05/27,20087.18414
2019/05/28,12511.51194
2019/05/29,16847.65195
2019/05/30,21910.53598
2019/05/31,27700.55993
2019/06/01,25405.87193
2019/06/02,26654.03988
2019/06/03,19154.00396
2019/06/04,15513.732
2019/06/05,23354.38791
2019/06/06,26468.88012
2019/06/07,33813.98394
2019/06/08,27328.11615
2019/06/09,28456.3559
2019/06/10,21036.93596
2019/06/11,18668.55591
2019/06/12,21481.86013
2019/06/13,37014.63593
2019/06/14,35690.47207
2019/06/15,24032.68792
2019/06/16,22725.80387
2019/06/17,28256.85606
2019/06/18,17717.78385
2019/06/19,23729.84379
2019/06/20,38673.468
2019/06/21,35075.53192
2019/06/22,33097.95607
2019/06/23,29831.37586
2019/06/24,24507.10803
2019/06/25,18045.04793
2019/06/26,24898.52387
2019/06/27,26999.20802
2019/06/28,26940.00005
2019/06/29,22391.90412
2019/06/30,26541.33611
2019/07/01,23348.91601
2019/07/02,20975.80818
2019/07/03,21034.92006
2019/07/04,23823.85202
2019/07/05,26693.71211
2019/07/06,23373.63582
2019/07/07,25096.90827
2019/07/08,26611.20012
2019/07/09,22471.92007
2019/07/10,21648.01198
2019/07/11,35554.41633
2019/07/12,24520.5241
2019/07/13,24073.36818
2019/07/14,29905.23595
2019/07/15,23895.03607
2019/07/16,29735.13626
2019/07/17,29400.56393
2019/07/18,30745.82403
2019/07/19,34561.8121
2019/07/20,28240.45193
2019/07/21,35738.3039
2019/07/22,35837.11175
2019/07/23,28466.57996
2019/07/24,25935.68394
2019/07/25,53766.27577
2019/07/26,40883.61612
2019/07/27,30251.83204
2019/07/28,53217.29997
2019/07/29,183421.2835
2019/07/30,76349.19599
2019/07/31,54987.20404
2019/08/01,58061.06382
2019/08/02,59807.03995
2019/08/03,50234.3999
2019/08/04,40150.39216
2019/08/05,49043.07578
2019/08/06,40354.56
2019/08/07,36231.52801
2019/08/08,58285.37999
2019/08/09,42149.68808
2019/08/10,49324.47604
2019/08/11,34336.43995
2019/08/12,37164.5881
2019/08/13,26930.78393
2019/08/14,31624.84802
2019/08/15,43561.81182
2019/08/16,41416.8598
2019/08/17,35652.09578
2019/08/18,33289.34405
2019/08/19,32599.24793
2019/08/20,25512.94795
2019/08/21,24730.23589
2019/08/22,37422.6957
2019/08/23,26927.22007
2019/08/24,33502.86016
2019/08/25,24398.43586
2019/08/26,18253.44014
2019/08/27,16041.30006
2019/08/28,7888.211948
2019/08/29,13520.93988
2019/08/30,26007.33623
2019/08/31,22681.47613
2019/09/01,19888.04406
2019/09/02,20652.85202
2019/09/03,17040.5401
2019/09/04,11466.588
2019/09/05,9163.811982
2019/09/06,31276.52393
2019/09/07,40450.30797
2019/09/08,44543.5562
2019/09/09,32225.49622
2019/09/10,33109.56014
2019/09/11,35304.97201
2019/09/12,47226.37204
2019/09/13,38283.31208
2019/09/14,46261.46412
2019/09/15,48280.93207
2019/09/16,37158.79199
2019/09/17,36558.12014
2019/09/18,39801.39597
2019/09/19,49145.5441
2019/09/20,51962.15993
2019/09/21,47503.81216
2019/09/22,35675.44818
2019/09/23,36004.11595
2019/09/24,29362.15211
2019/09/25,28321.18814
2019/09/26,46504.24801
2019/09/27,41750.0641
2019/09/28,39437.42408
2019/09/29,36927.92405
2019/09/30,42822.45607
2019/10/01,27039.60007
2019/10/02,29544.65993
2019/10/03,37646.63996
2019/10/04,34760.97603
2019/10/05,41188.08028
2019/10/06,37421.48411
2019/10/07,32746.04426
2019/10/08,30182.23194
2019/10/09,30779.12395
2019/10/10,44367.04801
2019/10/11,41777.08804
2019/10/12,39751.70399
2019/10/13,34780.34401
2019/10/14,39453.65982
2019/10/15,27372.94806
2019/10/16,26317.44001
2019/10/17,29431.82398
2019/10/18,29031.46797
2019/10/19,45388.8002
2019/10/20,41009.6043
2019/10/21,33614.772
2019/10/22,32442.15596
2019/10/23,30642.87598
2019/10/24,35730.63591
2019/10/25,48125.07595
2019/10/26,40883.06398
2019/10/27,33650.58012
2019/10/28,37353.64812
2019/10/29,31927.15188
2019/10/30,36084.02402
2019/10/31,55663.76399
2019/11/01,49122.31193
2019/11/02,56451.44447
2019/11/03,48286.5359
2019/11/04,39892.392
2019/11/05,34825.35615
2019/11/06,41028.09603
2019/11/07,45211.11596
2019/11/08,50137.08024
2019/11/09,45630.57592
2019/11/10,34919.10018
2019/11/11,35809.11614
2019/11/12,33663.55201
2019/11/13,40697.94014
2019/11/14,39878.27984
2019/11/15,43853.23204
2019/11/16,28956.77986
2019/11/17,53987.52001
2019/11/18,41045.07605
2019/11/19,25816.536
2019/11/20,40159.40396
2019/11/21,54714.216
2019/11/22,45230.91577
2019/11/23,51942.82825
2019/11/24,39357.96006
2019/11/25,32990.07611
2019/11/26,30039.81599
2019/11/27,32353.96795
2019/11/28,43634.67589
2019/11/29,42555.63587
2019/11/30,35001.10791
2019/12/01,91491.48101
2019/12/02,70044.99037
2019/12/03,73131.53973
2019/12/04,92827.58991
2019/12/05,121969.6797
2019/12/06,114558.5996
2019/12/07,114813.2403
2019/12/08,117145.4399
2019/12/09,82256.03973
2019/12/10,79357.19964
2019/12/11,77351.30957
2019/12/12,153649.8
2019/12/13,88744.14003
2019/12/14,124887.6003
2019/12/15,111190.8598
2019/12/16,94544.75973
2019/12/17,56856.56992
2019/12/18,96034.56006
2019/12/19,128716.5895
2019/12/20,105911.55
2019/12/21,122528.4606
2019/12/22,130134.0305
2019/12/23,84589.20011
2019/12/24,62679.1503
2019/12/25,85883.45968
2019/12/26,121566.7795
2019/12/27,134125.1996
2019/12/28,129800.0396
2019/12/29,97395.63017
2019/12/30,123414.0902
2019/12/31,71796.78032
"""