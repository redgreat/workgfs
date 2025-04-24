import pymysql
import yaml
import time
from loguru import logger


def get_db_conn(db_config):
    return pymysql.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset=db_config.get('charset', 'utf8')
    )

def get_last_month_range():
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    last_month_end = first_day_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start.strftime('%Y-%m-%d'), first_day_this_month.strftime('%Y-%m-%d')

def fetch_main_data(conn, start_date=None, end_date=None):
    """
    start_date, end_date: 字符串'YYYY-MM-DD'，闭开区间[start, end)。
    若为None则自动取上个月自然月。
    """
    if not start_date or not end_date:
        start_date, end_date = get_last_month_range()
    with conn.cursor() as cursor:
        sql_1 = f"""
        SELECT Id, 1 AS Type 
        FROM tb_feeapplicationinfo 
        WHERE AuditStatus=2
          AND OrgCode='1001'
          AND LastAuditTime>='{start_date}'
          AND LastAuditTime<'{end_date}'
          AND Deleted=0
        """
        sql_2 = f"""
        SELECT a.Id, 2 AS Type 
        FROM tb_workpriceedit_log a
        JOIN basic_ordertypeinfo b
        WHERE a.Status=1
          AND a.OrderType=b.TypeCode
          AND b.ServiceProviderCode='1001'
          AND a.OperTime>='{start_date}'
          AND a.OperTime<'{end_date}'
          AND a.Deleted=0
          AND b.Deleted=0
        """
        all_results = []
        cursor.execute(sql_1)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        cursor.execute(sql_2)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        return all_results

def fetch_goods_data(orderid):
    """独立方法：连接mall库并查商品信息"""
    if not orderid:
        return []
    import os
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml')
    import yaml
    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)
    if 'mall_db' not in config:
        return []
    mall_conn = get_db_conn(config['mall_db'])
    goods_list = []
    try:
        with mall_conn.cursor() as mall_cursor:
            goods_sql = """
            SELECT a.OrderPrice,c.MainPartId,c.MainPartName
            FROM tb_orderinfo a
            JOIN tb_orderitem b
              ON b.OrderId=a.Id
              AND b.Deleted=0
            JOIN tb_orderitemdetail c
              ON c.ItemId=b.Id
              AND c.Deleted=0
            WHERE a.Deleted=0
              AND a.Id=%s;
            """
            mall_cursor.execute(goods_sql, (orderid,))
            goods_list = [dict(zip([col[0] for col in mall_cursor.description], g)) for g in mall_cursor.fetchall()]
    finally:
        mall_conn.close()
    return goods_list

def fetch_detail_data(conn, main_data_list):
    results = []
    for item in main_data_list:
        main_id = item['Id']
        main_type = item['Type']
        row = None
        with conn.cursor() as cursor:
            if main_type == 1:
                sql = """
                SELECT a.Id,a.TargetId AS WorkOrderId,b.AppCode,CONCAT(b.AppCode,'-CT-',LPAD(FLOOR(RAND() * 1000000), 6, '0')) AS CostNo,l.Remark AS CostRemark,a.ApplyReason,
                (SELECT MallOrderId FROM tb_workgoodsinfo h WHERE h.WorkOrderId=b.Id AND h.GoodsType IN (5,10,11,18) AND h.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT OrderNo FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType IN (5,10,11,18) AND i.Deleted=0 LIMIT 1) AS OrderNo,
                (SELECT SaleName FROM tb_workgoodsinfo j WHERE j.WorkOrderId=b.Id AND j.GoodsType = 0 AND j.Deleted=0 LIMIT 1) AS SaleName,
                (SELECT SaleName FROM tb_workgoodsinfo k WHERE k.WorkOrderId=b.Id AND k.GoodsType IN (5,10,11,18) AND k.Deleted=0 LIMIT 1) AS ServiceSaleName,
                c.SubjectNameSummary,b.PreCustStoreId,b.PreCustStoreName,b.CustomerId,b.CustomerName,b.CustStoreId,b.CustStoreName,b.CustSettleId,
                b.CustSettleName,a.ApplyFee,a.FeeApplyTime,b.OrderType,fn_GetOrderTypeByCode(b.OrderType) AS OrderTypeName,a.ApplyWorkStatus AS WorkStatus,
                fn_GetStatusNameByCode(a.ApplyWorkStatus) AS WorkStatusName,b.ProCode,b.ProName,b.CityCode,b.CityName,b.AreaCode,b.AreaName,
                b.InstallAddress,d.Privoder,CASE d.Privoder WHEN 0 THEN '中瑞' WHEN 1 THEN '客户' END AS PrivoderName,d.ServiceType,
                CASE d.ServiceType WHEN 4 THEN '常规安装' WHEN 5 THEN '上门安装' WHEN 6 THEN '集中安装' WHEN 7 THEN '道路救援' END AS ServiceTypeName,
                g.VinNumber,f.`Value` AS TruckVin,d.ServiceCode,d.ServiceName,GetAscriptionByLoginName(d.ServiceCode,1) AS ServiceAscription,
                fn_GetRecordCodeById(a.TargetId) AS RecordCode,fn_GetRecordNameById(a.TargetId) AS RecordName,GetAscriptionByLoginName(fn_GetRecordCodeById(a.TargetId),1) AS RecordAscription,
                d.Remark AS ServiceRemark,fn_GetDispatchRemarkById(a.TargetId) AS DispatchRemark,0 AS IsChange,NULL AS ChangeRemark
                FROM tb_feeapplicationinfo a
                JOIN tb_workorderinfo b
                  ON a.TargetId=b.Id
                  AND b.Deleted=0
                LEFT JOIN tb_worksubjectsummary c 
                  ON b.Id = c.WorkOrderId
                  AND c.Deleted = 0
                LEFT JOIN tb_workserviceinfo d 
                  ON d.WorkOrderId = b.Id
                  AND d.Deleted = 0
                LEFT JOIN tb_custcolumn f
                  ON f.WorkOrderId=b.Id
                  AND f.TypeName='挂车车架号'
                  AND f.Deleted=0
                JOIN tb_workcarinfo g
                  ON g.WorkOrderId=b.Id
                  AND g.Deleted=0
                LEFT JOIN tb_feeiteminfo l
                  ON l.Id=a.FeeItemId
                  AND l.Deleted=0
                WHERE a.Deleted=0
                  AND a.Id = %s
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    mall_orderid = row_dict.get('OrderId')
                    row_dict['Goods'] = fetch_goods_data(mall_orderid)
                    results.append(row_dict)
            elif main_type == 2:
                sql = """
                SELECT a.Id,a.WorkOrderId,b.AppCode,CONCAT(b.AppCode,'-CT-',LPAD(FLOOR(RAND() * 1000000), 6, '0')) AS CostNo,a.Remark AS CostRemark,a.Reason AS ApplyReason,
                (SELECT MallOrderId FROM tb_workgoodsinfo h WHERE h.WorkOrderId=b.Id AND h.GoodsType IN (5,10,11,18) AND h.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT OrderNo FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType IN (5,10,11,18) AND i.Deleted=0 LIMIT 1) AS OrderNo,
                (SELECT SaleName FROM tb_workgoodsinfo j WHERE j.WorkOrderId=b.Id AND j.GoodsType = 0 AND j.Deleted=0 LIMIT 1) AS SaleName,
                (SELECT SaleName FROM tb_workgoodsinfo k WHERE k.WorkOrderId=b.Id AND k.GoodsType IN (5,10,11,18) AND k.Deleted=0 LIMIT 1) AS ServiceSaleName,
                c.SubjectNameSummary,b.PreCustStoreId,b.PreCustStoreName,b.CustomerId,b.CustomerName,b.CustStoreId,b.CustStoreName,b.CustSettleId,
                b.CustSettleName,a.InternalPrice AS ApplyFee,a.OperTime AS FeeApplyTime,b.OrderType,fn_GetOrderTypeByCode(b.OrderType) AS OrderTypeName,b.WorkStatus,
                fn_GetStatusNameByCode(b.WorkStatus) AS WorkStatusName,b.ProCode,b.ProName,b.CityCode,b.CityName,b.AreaCode,b.AreaName,
                b.InstallAddress,d.Privoder,CASE d.Privoder WHEN 0 THEN '中瑞' WHEN 1 THEN '客户' END AS PrivoderName,d.ServiceType,
                CASE d.ServiceType WHEN 4 THEN '常规安装' WHEN 5 THEN '上门安装' WHEN 6 THEN '集中安装' WHEN 7 THEN '道路救援' END AS ServiceTypeName,
                g.VinNumber,f.`Value` AS TruckVin,d.ServiceCode,d.ServiceName,GetAscriptionByLoginName(d.ServiceCode,1) AS ServiceAscription,
                fn_GetRecordCodeById(a.WorkOrderId) AS RecordCode,fn_GetRecordNameById(a.WorkOrderId) AS RecordName,GetAscriptionByLoginName(fn_GetRecordCodeById(a.WorkOrderId),1) AS RecordAscription,
                d.Remark AS ServiceRemark,fn_GetDispatchRemarkById(a.WorkOrderId) AS DispatchRemark,1 AS IsChange,a.Remark AS ChangeRemark
                FROM tb_workpriceedit_log a
                JOIN tb_workorderinfo b
                  ON a.WorkOrderId=b.Id
                  AND b.Deleted=0
                LEFT JOIN tb_worksubjectsummary c 
                  ON b.Id = c.WorkOrderId
                  AND c.Deleted = 0
                LEFT JOIN tb_workserviceinfo d 
                  ON d.WorkOrderId = b.Id
                  AND d.Deleted = 0
                LEFT JOIN tb_custcolumn f
                  ON f.WorkOrderId=b.Id
                  AND f.TypeName='挂车车架号'
                  AND f.Deleted=0
                JOIN tb_workcarinfo g
                  ON g.WorkOrderId=b.Id
                  AND g.Deleted=0
                WHERE a.Deleted=0
                  AND a.Id = %s
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    mall_orderid = row_dict.get('OrderId')
                    row_dict['Goods'] = fetch_goods_data(mall_orderid)
                    results.append(row_dict)
    return results

def insert_to_target(conn, data):
    if not data:
        return
    # 字段顺序与tm_order_costinfo表建表语句一致
    keys = [
        'Id', 'WorkOrderId', 'AppCode', 'CostNo', 'CostRemark', 'ApplyReason', 'OrderId', 'OrderNo',
        'SaleName', 'ServiceSaleName', 'SubjectNameSummary', 'PreCustStoreId', 'PreCustStoreName',
        'CustomerId', 'CustomerName', 'CustStoreId', 'CustStoreName', 'CustSettleId', 'CustSettleName',
        'ApplyFee', 'FeeApplyTime', 'LastAuditTime', 'OrderType', 'OrderTypeName', 'WorkStatus',
        'WorkStatusName', 'ProCode', 'ProName', 'CityCode', 'CityName', 'AreaCode', 'AreaName',
        'InstallAddress', 'Privoder', 'PrivoderName', 'ServiceType', 'ServiceTypeName', 'VinNumber',
        'TruckVin', 'ServiceCode', 'ServiceName', 'ServiceAscription', 'RecordCode', 'RecordName',
        'RecordAscription', 'ServiceRemark', 'DispatchRemark', 'IsChange', 'ChangeRemark', 'GoodsInfo'
    ]
    placeholders = ','.join(['%s'] * len(keys))
    columns = ','.join(f'`{k}`' for k in keys)
    sql = f"INSERT INTO tm_order_costinfo ({columns}) VALUES ({placeholders})"
    values = []
    for item in data:
        values.append(tuple(item.get(k) for k in keys))
    with conn.cursor() as cursor:
        cursor.executemany(sql, values)
    conn.commit()


def sync_task(start_date=None, end_date=None):
    import os
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml')
    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)
        batch_size = config['batch_size']

    src_conn = get_db_conn(config['source_db'])
    tgt_conn = get_db_conn(config['target_db'])

    total = 0
    start_time = time.time()
    logger.info(f"[SYNC] Start batch data sync task, start_date={start_date}, end_date={end_date}")
    try:
        all_main_data = fetch_main_data(src_conn, start_date, end_date)
        logger.info(f"[SYNC] Total main ids to process: {len(all_main_data)}")
        for i in range(0, len(all_main_data), batch_size):
            batch_data = all_main_data[i:i+batch_size]
            data = fetch_detail_data(src_conn, batch_data)
            insert_to_target(tgt_conn, data)
            total += len(batch_data)
            logger.info(f"[SYNC] Batch synced: {len(batch_data)} records, batch {i//batch_size+1}")
        duration = time.time() - start_time
        logger.info(f"[SYNC] Sync finished, total={total}, duration={duration:.2f}s")
    except Exception as e:
        logger.exception(f"[SYNC] Sync task failed: {e}")
    finally:
        src_conn.close()
        tgt_conn.close()
