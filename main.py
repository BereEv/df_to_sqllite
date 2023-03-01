import sqlite3
import pandas as pd


with sqlite3.connect('product.db') as conn:
    cur = conn.cursor()
    
    tbl_f_drp = ('sku_prices', 
                'prices', 
                'sales_ozon_j', 
                'sales_ozon_s', 
                'sales_ozon_p',)
    
    for tbl in tbl_f_drp:
        try:
            cur.execute(f'''DROP TABLE {tbl}''')
        except:
            None
        
    cur.executescript('''
                CREATE TABLE prices AS
                SELECT *
                FROM stock
                RIGHT JOIN period_stock 
                ON period_stock.Артикул=stock.Артикул;
                ALTER TABLE prices
                ADD COLUMN cc_1_in REAL;
                UPDATE prices
                SET cc_1_in = (стоимость_1_ед);
                UPDATE prices
                SET cc_1_in = (Итого)
                WHERE стоимость_1_ед IS NULL;
               ''')
    col_f_drop = 'стоимость_1_ед', 'Артикул', 'Итого'
    for col in col_f_drop:
        cur.execute(f'''
                ALTER TABLE prices
                DROP COLUMN {col};
                ''')
    cur.execute('''
                ALTER TABLE prices
                RENAME COLUMN 'Артикул:1' TO Артикул;
                ''')
    
    cur.executescript('''
                CREATE TABLE sku_prices AS
                SELECT * 
                FROM prices
                LEFT JOIN sku_list 
                ON sku_list.Артикул = prices.Артикул;
                UPDATE sku_prices
                SET Номенклатура = Номенклатура
                WHERE Номенклатура <> NULL;
                ALTER TABLE sku_prices
                DROP COLUMN 'Артикул:1';
                ''')
    
    cur.executescript('''
                CREATE TABLE sales_ozon_j AS
                SELECT *
                FROM sales_ozon
                FULL OUTER JOIN curents 
                ON sales_ozon.Date = curents.Date;
                UPDATE sales_ozon_j
                SET Date = "Date:1"
                WHERE Date IS NULL;
                ''')
    
    cur.execute('''
                CREATE TABLE sales_ozon_p AS
                SELECT * 
                FROM sales_ozon_j
                LEFT JOIN sku_prices
                ON sales_ozon_j.Артикул = sku_prices.Артикул;
                ''')
    
    conn.commit()

df = pd.read_sql('SELECT * FROM sales_ozon_p', 
                 con=conn)
df = df.sort_values(by=['Date'])
for i in range(len(df['Value'])):
      if df['Value'][i] == None:
       # df['Value'][i] = df['Value'][i - 1]
            print(df['Value'][i], i)
dfs = pd.read_sql('SELECT * FROM period_stock', 
                 con=conn)
dfs.to_excel('log/result.xlsx')
#df.info()
#sqlite3.connect('product.db').close()
conn.close()
