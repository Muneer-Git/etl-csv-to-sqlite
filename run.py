import pandas as pd, sqlite3, yaml, argparse, os

def load_csv_to_table(con, path, table):
    df = pd.read_csv(path)
    df.to_sql(table, con, if_exists='replace', index=False)

def main(cfg):
    db = cfg['warehouse']
    con = sqlite3.connect(db)
    try:
        for src in cfg['sources']:
            print('Loading', src['path'], '->', src['table'])
            load_csv_to_table(con, src['path'], src['table'])
        for t in cfg.get('transforms', []):
            print('Running transform SQL...')
            con.executescript(t['sql'])
        con.commit()
    finally:
        con.close()
    print('Done. Warehouse at', db)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='config.yaml')
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))
    os.makedirs('data', exist_ok=True)
    main(cfg)
