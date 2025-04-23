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

def fetch_main_data(conn):
    with conn.cursor() as cursor:
        sql_1 = """
        SELECT Id, 1 AS Type 
        FROM tb_feeapplicationinfo 
        WHERE AuditStatus=2
          AND OrgCode='1001'
          AND FeeApplyTime>=DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -12 MONTH), '%Y-%m-01')
          AND FeeApplyTime<DATE_FORMAT(CURDATE(), '%Y-%m-01')
          AND Deleted=0
        """
        sql_2 = """
        SELECT a.Id, 2 AS Type 
        FROM tb_workpriceedit_log a
        JOIN basic_ordertypeinfo b
        WHERE a.Status=1
          AND a.OrderType=b.TypeCode
          AND b.ServiceProviderCode='1001'
          AND a.OperTime>=DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL -12 MONTH), '%Y-%m-01')
          AND a.OperTime<DATE_FORMAT(CURDATE(), '%Y-%m-01')
          AND a.Deleted=0
          AND b.Deleted=0
        """
        all_results = []
        cursor.execute(sql_1)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        cursor.execute(sql_2)
        all_results.extend([{'Id': row[0], 'Type': row[1]} for row in cursor.fetchall()])
        return all_results

def get_goods_by_appid(appid):
    """独立方法：连接mall库并查商品信息"""
    if not appid:
        return []
    import yaml
    with open('../config/config.yaml', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    if 'mall_db' not in config:
        return []
    mall_conn = get_db_conn(config['mall_db'])
    goods_list = []
    try:
        with mall_conn.cursor() as mall_cursor:
            goods_sql = "SELECT * FROM tb_orderinfo WHERE AppId=%s"
            mall_cursor.execute(goods_sql, (appid,))
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
                SELECT a.Id,a.TargetId AS WorkOrderId,CONCAT(b.AppCode,'-CT-',LPAD(FLOOR(RAND() * 1000000), 6, '0')) AS CostNo,
                (SELECT MallOrderId FROM tb_workgoodsinfo h WHERE h.WorkOrderId=b.Id AND h.GoodsType IN (5,10,11,18) AND h.Deleted=0 LIMIT 1) AS OrderId,
                (SELECT OrderNo FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType IN (5,10,11,18) AND i.Deleted=0 LIMIT 1) AS OrderNo,
                (SELECT SaleName FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType = 0 AND i.Deleted=0 LIMIT 1) AS SaleName,
                (SELECT SaleName FROM tb_workgoodsinfo i WHERE i.WorkOrderId=b.Id AND i.GoodsType IN (5,10,11,18) AND i.Deleted=0 LIMIT 1) AS ServiceSaleName,
                b.AppCode,b.CustomerId,b.CustomerName,b.CustStoreId,b.CustStoreName,b.CustSettleId,b.CustSettleName,a.ApplyFee,a.FeeApplyTime,
                a.FeeApplyTime,b.OrderType,fn_GetOrderTypeByCode(b.OrderType) AS OrderTypeName,a.ApplyWorkStatus,
                fn_GetStatusNameByCode(a.ApplyWorkStatus) AS WorkStatusName,b.ProCode,b.ProName,b.CityCode,b.CityName,b.AreaCode,b.AreaName,
                b.InstallAddress,d.Privoder,d.ServiceType,g.VinNumber,f.`Value` AS TruckVin,a.LastAuditTime,
                d.ServiceCode,d.ServiceName,GetAscriptionByLoginName(d.ServiceCode,1) AS ServiceAscription,fn_GetRecordCodeById(a.TargetId) AS RecordCode,
                fn_GetRecordNameById(a.TargetId) AS RecordName,GetAscriptionByLoginName(fn_GetRecordCodeById(a.TargetId),1) AS RecordAscription,
                d.Remark AS ServiceRemark,fn_GetDispatchRemarkById(a.TargetId) AS DispatchRemark
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
                WHERE a.Deleted=0
                  AND a.Id = %s
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    mall_appid = row_dict.get('WorkOrderId')
                    row_dict['Goods'] = get_goods_by_appid(mall_appid)
                    results.append(row_dict)
            elif main_type == 2:
                sql = """
                SELECT CONCAT(b.AppCode,'-CT-',LPAD(FLOOR(RAND() * 1000000), 6, '0')) AS CostNo,
                (SELECT OrderNo FROM tb_workgoodsinfo h WHERE h.WorkOrderId=b.Id AND h.Deleted=0 AND h.OrderNo IS NOT NULL LIMIT 1) AS OrderNo,
                b.AppCode,NULL AS RelateCostNo,0 AS CostType,b.CustomerId,b.CustomerName,b.CustStoreId,b.CustStoreName,b.CustSettleId,b.CustSettleName,
                NULL AS MainpartName,a.ApplyFee,a.FeeApplyTime,a.FeeApplyTime,b.OrderType,fn_GetOrderTypeByCode(b.OrderType) AS OrderTypeName,
                a.ApplyWorkStatus,fn_GetStatusNameByCode(a.ApplyWorkStatus) AS WorkStatusName,b.ProCode,b.ProName,b.CityCode,b.CityName,b.AreaCode,b.AreaName,
                b.InstallAddress,b.CustStoreId,b.CustStoreName,d.Privoder,d.ServiceType,g.VinNumber,f.`Value` AS TruckVin,a.LastAuditTime,d.ServiceCode,d.ServiceName,
                GetAscriptionByLoginName(d.ServiceCode,1) AS ServiceAscription  
                FROM tb_workpriceedit_log a
                JOIN tb_workorderinfo b
                ON a.TargetId=b.Id
                AND b.Deleted=0
                LEFT JOIN tb_worksubjectsummary c 
                ON b.Id = c.WorkOrderId
                AND c.Deleted = 0
                LEFT JOIN tb_workserviceinfo d 
                ON d.WorkOrderId = b.Id
                AND d.Deleted = 0
                LEFT JOIN tb_feeiteminfo e 
                ON a.FeeItemId = e.Id
                LEFT JOIN tb_custcolumn f
                ON f.WorkOrderId=b.Id
                AND f.TypeName='挂车车架号'
                AND f.Deleted=0
                JOIN tb_workcarinfo g
                ON g.WorkOrderId=b.Id
                AND g.Deleted=0
                WHERE a.Id = %s
                """
                cursor.execute(sql, (main_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(zip([col[0] for col in cursor.description], row))
                    # 可在此处用mall_conn等查其他库并拼接数据
                    results.append(row_dict)
            else:
                # 其他类型可加elif
                pass
    if src_conn:
        src_conn.close()
    if mall_conn:
        mall_conn.close()
    return results

def insert_to_target(conn, data):
    if not data:
        return
    with conn.cursor() as cursor:
        # TODO: 按目标结构插入，写INSERT语句
        # cursor.execute("INSERT INTO ...", ...)
        pass
    conn.commit()

def sync_task():
    with open('../config/config.yaml', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    batch_size = config['batch_size']

    src_conn = get_db_conn(config['source_db'])
    tgt_conn = get_db_conn(config['target_db'])

    total = 0
    start_time = time.time()
    logger.info("[SYNC] Start batch data sync task")
    try:
        all_main_data = fetch_main_data(src_conn)
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
