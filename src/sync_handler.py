import pymysql
import yaml
import time
import datetime
from datetime import timedelta, datetime
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
        SELECT DISTINCT Id, 1 AS Type 
        FROM vi_workcount_log
        WHERE CompleteTime>='{start_date}'
          AND CompleteTime<'{end_date}';
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
        sql_3 = f"""
        SELECT Id, 3 AS Type 
        FROM tb_feeapplicationinfo 
        WHERE AuditStatus=2
          AND OrgCode='1001'
          AND LastAuditTime>='{start_date}'
          AND LastAuditTime<'{end_date}'
          AND Deleted=0
        """
        
        all_results = []
        cursor.execute(sql_1)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        cursor.execute(sql_2)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        cursor.execute(sql_3)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        return all_results

def fetch_goods_data(orderid, salename):
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
            SELECT b.GoodsPrice,c.MainPartId,c.MainPartName
            FROM tb_orderinfo a
            JOIN tb_orderitem b
              ON b.OrderId=a.Id
              AND b.SaleName=%s
              AND b.Deleted=0
            JOIN tb_orderitemdetail c
              ON c.ItemId=b.Id
              AND c.Deleted=0
            WHERE a.Deleted=0
              AND a.Id=%s
            LIMIT 1;
            """
            mall_cursor.execute(goods_sql, (salename, orderid))
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
                SELECT a.Id,
                a.WorkOrderId,
                CONCAT(a.AppCode,'-CT-',RIGHT(a.WorkOrderId,10)) AS CostNo,
                a.AppCode,
                (SELECT MallOrderId FROM tb_workgoodsinfo b WHERE b.WorkOrderId=a.WorkOrderId AND b.GoodsType IN (5,10,11,18,37,38) AND b.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT OrderNo FROM tb_workgoodsinfo c WHERE c.WorkOrderId=a.WorkOrderId AND c.GoodsType IN (5,10,11,18,37,38) AND c.Deleted=0 LIMIT 1) AS OrderNo,
                1 AS OrderType,
                a.OrderTypeName AS WorkOrderType,
                a.WorkStatusName AS WorkStatus,
                a.ProName,
                a.CityName,
                a.AreaName,
                a.InstallAddress,
                a.CustSettleId,
                a.CustSettleName,
                a.CustomerId,
                a.CustomerName,
                a.CustStoreId,
                a.CustStoreName,
                a.ActualCustStoreName,
                NULL AS MainPartId,
                NULL AS MainPartName,
                a.GeneralGoodsNames,
                a.ArtificialServicePriceName,
                a.ArtificialServicePrice,
                a.ServiceSubjectName,
                a.SubjectClassCode,
                a.ServiceSubjectCode,
                a.InternalPrice,
                a.PricingMethodName AS CostReason,
                '基础计价' AS CostRemark,
                a.CompleteTime AS FinishTime,
                a.CompleteTime AS CostConfirmTime,
                a.Privoder,
                a.IsCentralize,
                a.VinNumber,
                a.GuaVin,
                a.PlateNumber,
                a.CompleteTime,
                a.CreatePersonName,
                a.ServiceCode,
                a.ServiceName,
                a.ServiceAscription,
                a.ActualRecordPersonCode,
                a.ActualRecordPersonName,
                a.ActualRecordPersonAscription,
                a.SendRemark,
                a.ServiceRemark,
                a.TagSign,
                NULL AS ChangeRemark
                FROM vi_workcount_log a
                WHERE a.Id = %s
                LIMIT 1
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    mall_orderid = row_dict.get('OrderId')
                    mall_salenname = row_dict.get('ArtificialServicePriceName')
                    goods_info = fetch_goods_data(mall_orderid, mall_salenname)
                    row_dict['GoodsInfo'] = goods_info
                    if goods_info and isinstance(goods_info, list) and len(goods_info) > 0 and 'MainPartId' in goods_info[0]:
                        row_dict['MainPartId'] = goods_info[0]['MainPartId']
                    if goods_info and isinstance(goods_info, list) and len(goods_info) > 0 and 'MainPartName' in goods_info[0]:
                        row_dict['MainPartName'] = goods_info[0]['MainPartName']
                    results.append(row_dict)
            elif main_type == 2:
                sql = """
                SELECT a.Id,CONCAT(a.AppCode,'-CT-',RIGHT(a.WorkOrderId,10)) AS CostNo,a.WorkOrderId,
                a.AppCode,
                (SELECT c.MallOrderId FROM tb_workgoodsinfo c WHERE c.WorkOrderId=a.WorkOrderId AND c.GoodsType IN (5,10,11,18,37,38) AND c.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT d.OrderNo FROM tb_workgoodsinfo d WHERE d.WorkOrderId=a.WorkOrderId AND d.GoodsType IN (5,10,11,18,37,38) AND d.Deleted=0 LIMIT 1) AS OrderNo,
                1 AS OrderType,
                a.OrderTypeName AS WorkOrderType,
                b.WorkStatusName AS WorkStatus,
                b.ProName,
                b.CityName,
                b.AreaName,
                b.InstallAddress,
                b.CustSettleName,
                b.CustomerName,
                b.CustStoreName,
                b.ActualCustStoreName,
                NULL AS MainPartId,
                NULL AS MainPartName,
                b.GeneralGoodsNames,
                a.ArtificialServicePriceName,
                b.ArtificialServicePrice,
                b.ServiceSubjectName,
                b.SubjectClassCode,
                b.ServiceSubjectCode,
                a.InternalPrice-b.InternalPrice AS InternalPrice,
                b.PricingMethodName AS CostReason,
                '基础计价' AS CostRemark,
                a.OperTime AS FinishTime,
                a.OperTime AS CostConfirmTime,
                b.Privoder,
                b.IsCentralize,
                b.VinNumber,
                b.GuaVin,
                b.PlateNumber,
                b.CompleteTime,
                b.CreatePersonName,
                b.ServiceCode,
                b.ServiceName,
                b.ServiceAscription,
                b.ActualRecordPersonCode,
                b.ActualRecordPersonName,
                b.ActualRecordPersonAscription,
                b.SendRemark,
                b.ServiceRemark,
                '是' AS TagSign,
                a.Remark AS ChangeRemark
                FROM tb_workpriceedit_log a
                JOIN vi_workcount_log b
                  ON b.WorkOrderId=a.WorkOrderId
                  AND a.ArtificialServicePriceId=b.ArtificialServicePriceId
                  AND b.TagSign='是'
                WHERE a.Deleted=0
                  AND MONTH(a.OperTime)<>MONTH(b.CompleteTime)
                  AND a.Id = %s
                ORDER BY a.OperTime DESC
                LIMIT 1
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    mall_orderid = row_dict.get('OrderId')
                    mall_salenname = row_dict.get('ArtificialServicePriceName')
                    goods_info = fetch_goods_data(mall_orderid, mall_salenname)
                    row_dict['GoodsInfo'] = goods_info
                    if goods_info and isinstance(goods_info, list) and len(goods_info) > 0 and 'MainPartId' in goods_info[0]:
                        row_dict['MainPartId'] = goods_info[0]['MainPartId']
                    if goods_info and isinstance(goods_info, list) and len(goods_info) > 0 and 'MainPartName' in goods_info[0]:
                        row_dict['MainPartName'] = goods_info[0]['MainPartName']
                    results.append(row_dict)
            elif main_type == 3:
                sql = """
                SELECT a.Id,CONCAT(b.AppCode,'-CT-',RIGHT(a.TargetId,10)) AS CostNo,a.TargetId AS WorkOrderId,b.AppCode,
                (SELECT MallOrderId FROM tb_workgoodsinfo h WHERE h.WorkOrderId=b.Id AND h.GoodsType IN (5,10,11,18) AND h.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT OrderNo FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType IN (5,10,11,18,37,38) AND i.Deleted=0 LIMIT 1) AS OrderNo,
                2 AS OrderType,fn_GetOrderTypeByCode(b.OrderType) AS WorkOrderType,fn_GetStatusNameByCode(a.ApplyWorkStatus) AS WorkStatus,
                b.ProName,b.CityName,b.AreaName,b.InstallAddress,b.CustSettleId,b.CustSettleName,b.CustomerId,b.CustomerName,b.CustStoreId,
                b.CustStoreName,NULL AS MainPartId, NULL AS MainPartName, NULL AS ActualCustStoreName,
                (SELECT SaleName FROM tb_workgoodsinfo j WHERE j.WorkOrderId=b.Id AND j.GoodsType = 0 AND j.Deleted=0 LIMIT 1) AS GeneralGoodsNames,
                (SELECT SaleName FROM tb_workgoodsinfo k WHERE k.WorkOrderId=b.Id AND k.GoodsType IN (5,10,11,18,37,38) AND k.Deleted=0 LIMIT 1) AS ArtificialServicePriceName,
                NULL AS ArtificialServicePrice,c.SubjectNameSummary AS ServiceSubjectName,c.SubjectCodeSummary AS SubjectClassCode,
                a.ApplyFee AS InternalPrice,l.Remark AS CostRemark,a.ApplyReason AS CostReason,a.FeeApplyTime AS FinishTime,a.LastAuditTime AS CostConfirmTime,
                CASE d.Privoder WHEN 0 THEN '中瑞' WHEN 1 THEN '客户' END AS Privoder,
                CASE d.ServiceType WHEN 4 THEN '常规安装' WHEN 5 THEN '上门安装' WHEN 6 THEN '集中安装' WHEN 7 THEN '道路救援' END AS IsCentralize,
                g.VinNumber,f.`Value` AS GuaVin,g.PlateNumber,a.LastAuditTime AS CompleteTime, a.ApplyPersonName AS CreatePersonName,
                d.ServiceCode,d.ServiceName,GetAscriptionByLoginName(d.ServiceCode,1) AS ServiceAscription,
                fn_GetRecordCodeById(a.TargetId) AS ActualRecordPersonCode,fn_GetRecordNameById(a.TargetId) AS ActualRecordPersonName,
                GetAscriptionByLoginName(fn_GetRecordCodeById(a.TargetId),1) AS ActualRecordPersonAscription,
                d.Remark AS ServiceRemark,fn_GetDispatchRemarkById(a.TargetId) AS DispatchRemark,'否' AS TagSign,NULL AS ChangeRemark
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
                    mall_salenname = row_dict.get('ArtificialServicePriceName')
                    goods_info = fetch_goods_data(mall_orderid, mall_salenname)
                    row_dict['GoodsInfo'] = goods_info
                    # 覆盖 MainPartName
                    if goods_info and isinstance(goods_info, list) and len(goods_info) > 0:
                        if 'MainPartId' in goods_info[0]:
                            row_dict['MainPartId'] = goods_info[0]['MainPartId']
                        if 'MainPartName' in goods_info[0]:
                            row_dict['MainPartName'] = goods_info[0]['MainPartName']
                        if 'GoodsPrice' in goods_info[0]:
                            row_dict['ArtificialServicePrice'] = goods_info[0]['GoodsPrice']
                    results.append(row_dict) 
    return results

def insert_to_target(conn, data):
    if not data:
        return
    # 字段顺序与workcount_log表建表语句一致
    keys = ['Id', 'CostNo', 'WorkOrderId', 'AppCode', 'OrderId', 'OrderNo', 'OrderType', 'WorkOrderType', 'WorkStatus',
        'ProName', 'CityName', 'AreaName', 'InstallAddress', 'CustSettleId', 'CustSettleName', 'CustomerId', 'CustomerName',
        'CustStoreId','CustStoreName', 'MainPartId', 'MainPartName', 'ActualCustStoreName', 'GeneralGoodsNames', 
        'ArtificialServicePriceName', 'ArtificialServicePrice', 'ServiceSubjectName', 'SubjectClassCode', 'ServiceSubjectCode',
        'InternalPrice', 'CostRemark', 'CostReason', 'FinishTime', 'CostConfirmTime', 'Privoder', 'IsCentralize', 'VinNumber', 
        'GuaVin', 'PlateNumber', 'CompleteTime', 'CreatePersonName', 'ServiceCode', 'ServiceName', 'ServiceAscription',
        'ActualRecordPersonCode', 'ActualRecordPersonName', 'ActualRecordPersonAscription',
        'SendRemark', 'ServiceRemark', 'TagSign', 'ChangeRemark'
    ]
    placeholders = ','.join(['%s'] * len(keys))
    columns = ','.join(f'`{k}`' for k in keys)
    sql = f"REPLACE INTO workcount_log ({columns}) VALUES ({placeholders})"
    values = []
    from decimal import Decimal
    for item in data:
        row = []
        for k in keys:
            v = item.get(k)
            if isinstance(v, Decimal):
                v = str(v)
            row.append(v)
        values.append(tuple(row))
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
