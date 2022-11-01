from data_io.configs import save_data_zodb

def run(config, aux_config={}):
    save_data_zodb(config, {config['alpha']})