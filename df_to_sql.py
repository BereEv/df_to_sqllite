import sqlite3

from read_sku_list import DataSku
from read_stock import DataStck
from read_per_stock import DataPeriodStkc
from curents import CurentsEuro
from read_sales_ozon import DataSalesOzon
import time


class EmptyTbl():
    tbl_name = ''
    mdl = ''
        
    def df_to_sql(self):
        with sqlite3.connect('product.db') as conn:
            self.mdl.to_sql(self.tbl_name, conn,
                            schema='append', if_exists='replace',
                            index=False,
                            index_label='id_sku',
                            )
            conn.commit()
        
with sqlite3.connect('product.db') as conn:

    cur = conn.cursor()
    table_name_sku = "sku_list"
    table_name_stock = 'stock'
    table_name_peiod = 'period_stock'
    table_name_curents= 'curents'
    table_name_sales_ozon = 'sales_ozon'

    DataSku.data_sku().to_sql(table_name_sku, conn,
                            schema='append', if_exists='replace',
                            index=False)

    DataStck.stock_data().to_sql(table_name_stock, conn,
                            schema='append', if_exists='replace',
                            index=False)

    DataPeriodStkc.df_period_stkc().to_sql(table_name_peiod, conn,
                            schema='append', if_exists='replace',
                            index=False)
    
    CurentsEuro.curent().to_sql(table_name_curents, conn,
                            schema='append', if_exists='replace',
                            index=False)
    
    DataSalesOzon.data_sales_ozon().to_sql(table_name_sales_ozon, conn,
                            schema='replace', if_exists='replace',
                            index=False)
    
    conn.commit()
conn.close()
