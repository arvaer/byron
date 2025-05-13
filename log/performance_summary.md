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

=== Operation: GET ===
-- Metric: cycles --
         10K : 12,691,860,236.90 ± 155,092,774.26
        100K : 14,900,775,049.90 ± 181,613,582.35
       1000K : 143,089,218,447.70 ± 956,938,178.60
       2500K : 526,493,964,416.80 ± 2,436,662,261.31
Summary: mean of means = 174,293,954,537.83, range = 513,802,104,179.90, std of means = 242,583,322,498.28

-- Metric: instructions --
         10K : 40,734,277,643.50 ± 7,149,931.70
        100K : 48,481,965,518.30 ± 29,728,473.66
       1000K : 593,081,998,702.20 ± 524,897,260.83
       2500K : 2,293,661,674,916.80 ± 2,008,039,544.80
Summary: mean of means = 743,989,979,195.20, range = 2,252,927,397,273.30, std of means = 1,064,981,308,435.49

-- Metric: cache-references --
         10K : 217,141,910.80 ± 4,311,149.68
        100K : 371,493,973.20 ± 4,254,260.60
       1000K : 2,500,965,126.30 ± 48,293,387.14
       2500K : 5,949,481,465.60 ± 150,113,944.50
Summary: mean of means = 2,259,770,618.98, range = 5,732,339,554.80, std of means = 2,671,457,882.41

-- Metric: cache-misses --
         10K% : 10.14 ± 0.29%
        100K% : 6.68 ± 0.25%
       1000K% : 3.41 ± 0.07%
       2500K% : 4.08 ± 0.17%
Summary: mean of means = 6.08%, range = 6.73%, std of means = 3.05%

-- Metric: seconds_time_elapsed --
         10K : 2.61 ± 0.04
        100K : 3.17 ± 0.04
       1000K : 29.97 ± 0.28
       2500K : 107.24 ± 1.64
Summary: mean of means = 35.75, range = 104.63, std of means = 49.34

-- Metric: seconds_user --
         10K : 2.31 ± 0.03
        100K : 2.69 ± 0.02
       1000K : 25.79 ± 0.20
       2500K : 94.66 ± 0.44
Summary: mean of means = 31.36, range = 92.36, std of means = 43.60

-- Metric: seconds_sys --
         10K : 0.28 ± 0.01
        100K : 0.43 ± 0.02
       1000K : 3.92 ± 0.09
       2500K : 11.84 ± 0.75
Summary: mean of means = 4.12, range = 11.56, std of means = 5.42


=== Operation: PUT ===
-- Metric: cycles --
          1M : 12,504,757,554.50 ± 60,852,892.24
          5M : 68,557,098,837.20 ± 603,868,108.68
         10M : 276,458,066,529.20 ± 14,319,939,357.85
Summary: mean of means = 119,173,307,640.30, range = 263,953,308,974.70, std of means = 139,065,947,574.78

-- Metric: instructions --
          1M : 40,324,975,850.00 ± 4,234,191.62
          5M : 153,270,980,009.20 ± 224,023,204.47
         10M : 387,272,041,537.30 ± 885,919,038.98
Summary: mean of means = 193,622,665,798.83, range = 346,947,065,687.30, std of means = 176,958,358,115.14

-- Metric: cache-references --
          1M : 203,015,260.30 ± 2,918,433.13
          5M : 3,480,564,298.10 ± 86,407,432.71
         10M : 21,301,832,014.80 ± 982,226,375.98
Summary: mean of means = 8,328,470,524.40, range = 21,098,816,754.50, std of means = 11,354,147,400.59

-- Metric: cache-misses --
          1M% : 10.69 ± 0.23%
          5M% : 12.14 ± 0.29%
         10M% : 7.85 ± 0.36%
Summary: mean of means = 10.23%, range = 4.29%, std of means = 2.18%

-- Metric: seconds_time_elapsed --
          1M : 2.53 ± 0.03
          5M : 21.40 ± 0.40
         10M : 136.41 ± 7.56
Summary: mean of means = 53.45, range = 133.88, std of means = 72.47

-- Metric: seconds_user --
          1M : 2.28 ± 0.01
          5M : 12.30 ± 0.13
         10M : 49.93 ± 2.55
Summary: mean of means = 21.50, range = 47.65, std of means = 25.13

-- Metric: seconds_sys --
          1M : 0.23 ± 0.02
          5M : 8.88 ± 0.27
         10M : 85.09 ± 4.76
Summary: mean of means = 31.40, range = 84.86, std of means = 46.70


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

