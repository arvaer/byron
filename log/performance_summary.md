Loaded logs_get_10k_metrics.csv: 10 runs
Loaded logs_get_100k_metrics.csv: 10 runs
Loaded logs_put_1m_metrics.csv: 10 runs
Loaded logs_put_10m_metrics.csv: 10 runs
Loaded logs_range_gaussian_metrics.csv: 10 runs
Loaded logs_range_uniform_metrics.csv: 10 runs
Loaded logs_get_2500k_metrics.csv: 5 runs
Loaded logs_put_5m_metrics.csv: 10 runs
Loaded logs_get_1000k_metrics.csv: 10 runs
Loaded logs_delete_1m_metrics.csv: 10 runs
Loaded logs_delete_500k_metrics.csv: 10 runs
Loaded logs_delete_100k_metrics.csv: 10 runs
Loaded logs_get_rayon_1000k_metrics.csv: 10 runs
Loaded logs_put_rayon_1m_metrics.csv: 10 runs
Loaded logs_get_bloom_1m_metrics.csv: 10 runs
Loaded logs_get_bloom_10m_metrics.csv: 10 runs
Loaded logs_get_bloom_100k_metrics.csv: 10 runs
Loaded logs_put_rayon_5m_metrics.csv: 10 runs
Loaded logs_put_rayon_10m_metrics.csv: 3 runs
Loaded logs_get_mixed_1m_metrics.csv: 5 runs
Loaded logs_put_mixed_1m_metrics.csv: 5 runs

=== Operation: GET ===
-- Metric: cycles --
         10K : 12,691,860,236.90 ± 155,092,774.26
        100K : 14,900,775,049.90 ± 181,613,582.35
       1000K : 143,089,218,447.70 ± 956,938,178.60
       2500K : 526,493,964,416.80 ± 2,436,662,261.31
  BLOOM_100K : 267,147,700,152.20 ± 8,677,520,718.48
   BLOOM_10M : 256,976,284,689.90 ± 6,797,039,840.71
    BLOOM_1M : 249,706,549,400.60 ± 4,844,404,920.67
    MIXED_1M : 2,606,744,667,722.60 ± 47,894,239,073.86
 RAYON_1000K : 255,032,207,760.50 ± 4,590,445,863.18
Summary: mean of means = 481,420,358,653.01, range = 2,594,052,807,485.70, std of means = 811,794,365,509.59

-- Metric: instructions --
         10K : 40,734,277,643.50 ± 7,149,931.70
        100K : 48,481,965,518.30 ± 29,728,473.66
       1000K : 593,081,998,702.20 ± 524,897,260.83
       2500K : 2,293,661,674,916.80 ± 2,008,039,544.80
  BLOOM_100K : 431,897,811,952.20 ± 6,959,790,479.09
   BLOOM_10M : 434,411,657,422.90 ± 6,836,155,442.76
    BLOOM_1M : 434,738,496,040.60 ± 6,155,155,130.08
    MIXED_1M : 3,909,689,120,184.00 ± 38,001,817,656.32
 RAYON_1000K : 433,060,116,245.00 ± 6,408,450,372.88
Summary: mean of means = 957,750,790,958.39, range = 3,868,954,842,540.50, std of means = 1,294,146,319,445.30

-- Metric: cache-references --
         10K : 217,141,910.80 ± 4,311,149.68
        100K : 371,493,973.20 ± 4,254,260.60
       1000K : 2,500,965,126.30 ± 48,293,387.14
       2500K : 5,949,481,465.60 ± 150,113,944.50
  BLOOM_100K : 7,500,295,077.30 ± 448,387,778.37
   BLOOM_10M : 8,875,614,773.90 ± 382,774,469.13
    BLOOM_1M : 9,073,468,176.90 ± 322,946,291.75
    MIXED_1M : 50,872,588,261.20 ± 2,280,563,799.26
 RAYON_1000K : 7,992,495,499.00 ± 318,142,945.79
Summary: mean of means = 10,372,616,029.36, range = 50,655,446,350.40, std of means = 15,583,544,423.27

-- Metric: cache-misses --
         10K% : 10.14 ± 0.29%
        100K% : 6.68 ± 0.25%
       1000K% : 3.41 ± 0.07%
       2500K% : 4.08 ± 0.17%
  BLOOM_100K% : 9.99 ± 0.37%
   BLOOM_10M% : 7.92 ± 0.28%
    BLOOM_1M% : 7.54 ± 0.30%
    MIXED_1M% : 13.30 ± 0.50%
 RAYON_1000K% : 8.82 ± 0.27%
Summary: mean of means = 7.99%, range = 9.89%, std of means = 3.07%

-- Metric: seconds_time_elapsed --
         10K : 2.61 ± 0.04
        100K : 3.17 ± 0.04
       1000K : 29.97 ± 0.28
       2500K : 107.24 ± 1.64
  BLOOM_100K : 9.02 ± 0.35
   BLOOM_10M : 8.64 ± 0.39
    BLOOM_1M : 8.50 ± 0.32
    MIXED_1M : 59.58 ± 2.67
 RAYON_1000K : 8.79 ± 0.30
Summary: mean of means = 26.39, range = 104.63, std of means = 35.36

-- Metric: seconds_user --
         10K : 2.31 ± 0.03
        100K : 2.69 ± 0.02
       1000K : 25.79 ± 0.20
       2500K : 94.66 ± 0.44
  BLOOM_100K : 47.11 ± 1.57
   BLOOM_10M : 44.96 ± 1.26
    BLOOM_1M : 43.39 ± 0.78
    MIXED_1M : 466.07 ± 9.60
 RAYON_1000K : 44.49 ± 1.04
Summary: mean of means = 85.72, range = 463.77, std of means = 145.27

-- Metric: seconds_sys --
         10K : 0.28 ± 0.01
        100K : 0.43 ± 0.02
       1000K : 3.92 ± 0.09
       2500K : 11.84 ± 0.75
  BLOOM_100K : 28.65 ± 1.25
   BLOOM_10M : 29.12 ± 0.91
    BLOOM_1M : 29.83 ± 0.32
    MIXED_1M : 271.58 ± 7.71
 RAYON_1000K : 29.96 ± 0.84
Summary: mean of means = 45.07, range = 271.30, std of means = 85.94


=== Operation: PUT ===
-- Metric: cycles --
          1M : 12,504,757,554.50 ± 60,852,892.24
          5M : 68,557,098,837.20 ± 603,868,108.68
         10M : 276,458,066,529.20 ± 14,319,939,357.85
    MIXED_1M : 257,875,621,996.20 ± 8,864,072,629.26
   RAYON_10M : 142,163,696,256.00 ± 8,003,013,237.39
    RAYON_1M : 11,590,489,664.70 ± 67,875,176.68
    RAYON_5M : 68,028,195,232.80 ± 4,967,814,576.44
Summary: mean of means = 119,596,846,581.51, range = 264,867,576,864.50, std of means = 110,025,902,311.10

-- Metric: instructions --
          1M : 40,324,975,850.00 ± 4,234,191.62
          5M : 153,270,980,009.20 ± 224,023,204.47
         10M : 387,272,041,537.30 ± 885,919,038.98
    MIXED_1M : 455,471,381,084.00 ± 3,386,602,807.35
   RAYON_10M : 506,604,582,831.33 ± 11,598,452,242.90
    RAYON_1M : 42,264,782,480.30 ± 8,662,673.57
    RAYON_5M : 246,051,886,159.60 ± 7,296,133,721.98
Summary: mean of means = 261,608,661,421.68, range = 466,279,606,981.33, std of means = 192,550,562,140.59

-- Metric: cache-references --
          1M : 203,015,260.30 ± 2,918,433.13
          5M : 3,480,564,298.10 ± 86,407,432.71
         10M : 21,301,832,014.80 ± 982,226,375.98
    MIXED_1M : 7,979,843,977.40 ± 534,669,933.48
   RAYON_10M : 2,647,524,573.00 ± 681,402,138.47
    RAYON_1M : 195,480,758.00 ± 3,745,399.09
    RAYON_5M : 1,210,387,490.90 ± 422,504,889.19
Summary: mean of means = 5,288,378,338.93, range = 21,106,351,256.80, std of means = 7,553,647,230.53

-- Metric: cache-misses --
          1M% : 10.69 ± 0.23%
          5M% : 12.14 ± 0.29%
         10M% : 7.85 ± 0.36%
    MIXED_1M% : 8.69 ± 0.74%
   RAYON_10M% : 9.49 ± 1.30%
    RAYON_1M% : 4.99 ± 0.40%
    RAYON_5M% : 8.46 ± 1.32%
Summary: mean of means = 8.90%, range = 7.15%, std of means = 2.26%

-- Metric: seconds_time_elapsed --
          1M : 2.53 ± 0.03
          5M : 21.40 ± 0.40
         10M : 136.41 ± 7.56
    MIXED_1M : 9.05 ± 0.66
   RAYON_10M : 27.92 ± 0.75
    RAYON_1M : 2.25 ± 0.03
    RAYON_5M : 13.12 ± 0.33
Summary: mean of means = 30.38, range = 134.16, std of means = 47.69

-- Metric: seconds_user --
          1M : 2.28 ± 0.01
          5M : 12.30 ± 0.13
         10M : 49.93 ± 2.55
    MIXED_1M : 45.49 ± 2.01
   RAYON_10M : 25.48 ± 1.54
    RAYON_1M : 2.08 ± 0.02
    RAYON_5M : 12.16 ± 0.91
Summary: mean of means = 21.39, range = 47.85, std of means = 19.65

-- Metric: seconds_sys --
          1M : 0.23 ± 0.02
          5M : 8.88 ± 0.27
         10M : 85.09 ± 4.76
    MIXED_1M : 28.09 ± 1.30
   RAYON_10M : 2.71 ± 0.14
    RAYON_1M : 0.15 ± 0.01
    RAYON_5M : 1.04 ± 0.08
Summary: mean of means = 18.03, range = 84.94, std of means = 31.20


=== Operation: RANGE ===
-- Metric: cycles --
    GAUSSIAN : 54,717,533,020.90 ± 276,184,569.60
     UNIFORM : 55,030,126,947.70 ± 227,638,375.12
Summary: mean of means = 54,873,829,984.30, range = 312,593,926.80, std of means = 221,037,285.40

-- Metric: instructions --
    GAUSSIAN : 211,730,592,640.00 ± 61,822,996.75
     UNIFORM : 214,170,187,502.30 ± 46,810,178.03
Summary: mean of means = 212,950,390,071.15, range = 2,439,594,862.30, std of means = 1,725,054,070.48

-- Metric: cache-references --
    GAUSSIAN : 1,583,840,113.90 ± 13,451,327.14
     UNIFORM : 1,579,247,513.50 ± 14,954,660.51
Summary: mean of means = 1,581,543,813.70, range = 4,592,600.40, std of means = 3,247,458.89

-- Metric: cache-misses --
    GAUSSIAN% : 5.27 ± 0.24%
     UNIFORM% : 5.16 ± 0.32%
Summary: mean of means = 5.22%, range = 0.11%, std of means = 0.08%

-- Metric: seconds_time_elapsed --
    GAUSSIAN : 11.09 ± 0.10
     UNIFORM : 11.23 ± 0.10
Summary: mean of means = 11.16, range = 0.14, std of means = 0.10

-- Metric: seconds_user --
    GAUSSIAN : 10.12 ± 0.11
     UNIFORM : 10.17 ± 0.09
Summary: mean of means = 10.14, range = 0.06, std of means = 0.04

-- Metric: seconds_sys --
    GAUSSIAN : 0.88 ± 0.03
     UNIFORM : 0.95 ± 0.04
Summary: mean of means = 0.92, range = 0.07, std of means = 0.05


=== Operation: DELETE ===
-- Metric: cycles --
        100K : 8,585,361,901.50 ± 63,737,835.99
        500K : 7,633,066,918.90 ± 60,704,713.42
          1M : 8,610,048,653.30 ± 74,889,381.60
Summary: mean of means = 8,276,159,157.90, range = 976,981,734.40, std of means = 557,070,982.75

-- Metric: instructions --
        100K : 22,774,138,244.70 ± 12,186,749.09
        500K : 18,412,642,172.10 ± 17,142,010.48
          1M : 20,739,548,074.60 ± 35,133,613.47
Summary: mean of means = 20,642,109,497.13, range = 4,361,496,072.60, std of means = 2,182,380,055.15

-- Metric: cache-references --
        100K : 237,703,940.20 ± 5,841,191.30
        500K : 212,000,766.20 ± 1,942,231.23
          1M : 195,163,567.60 ± 1,331,290.94
Summary: mean of means = 214,956,091.33, range = 42,540,372.60, std of means = 21,423,615.13

-- Metric: cache-misses --
        100K% : 12.17 ± 0.21%
        500K% : 12.05 ± 0.26%
          1M% : 12.40 ± 0.12%
Summary: mean of means = 12.21%, range = 0.35%, std of means = 0.18%

-- Metric: seconds_time_elapsed --
        100K : 1.90 ± 0.01
        500K : 1.70 ± 0.02
          1M : 1.82 ± 0.01
Summary: mean of means = 1.81, range = 0.20, std of means = 0.10

-- Metric: seconds_user --
        100K : 1.55 ± 0.03
        500K : 1.39 ± 0.02
          1M : 1.57 ± 0.01
Summary: mean of means = 1.50, range = 0.19, std of means = 0.10

-- Metric: seconds_sys --
        100K : 0.34 ± 0.02
        500K : 0.31 ± 0.01
          1M : 0.23 ± 0.01
Summary: mean of means = 0.29, range = 0.11, std of means = 0.05

