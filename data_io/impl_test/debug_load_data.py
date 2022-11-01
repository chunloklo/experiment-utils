import os
import sys
sys.path.append(os.getcwd())

from data_io.configs import load_all_db_configs_and_keys, IdLinkedConfig
from analysis_common.configs import SweepInfo


configs = load_all_db_configs_and_keys('results_dbs/grazingworld_sweep')
# print(config_id_tuples[0])

sweepInfo = SweepInfo(configs)
sweepInfo.add_filter('alpha', 0.3)
print(sweepInfo.diff_list)
print(len(sweepInfo.filtered_configs))

print(sweepInfo.filtered_configs[0].get_id())
