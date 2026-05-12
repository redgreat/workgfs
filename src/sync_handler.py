import pymysql
import yaml
import time
from loguru import logger

# 同步完成后在 target_db 调用的无参存储过程（写死，不从配置读取）
POST_COST_SYNC_PROCEDURE = 'finance_main.proc_InsertCostInfo_ehcf'


def get_db_conn(db_config):
    return pymysql.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset=db_config.get('charset', 'utf8')
    )


def fetch_pending_cost_sync_main_ids(conn):
    """
    一次性取出当前待同步主单 Id 列表（任务启动时快照）。
    条件：AuditState=1 且 CostSyncState=0，未删除。
    """
    sql = """
        SELECT Id
        FROM finance_main.main_costsyncinfo
        WHERE Deleted = 0
          AND AuditState = 1
          AND CostSyncState = 0
        ORDER BY CreatedAt ASC, Id ASC
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return [r[0] for r in rows]


def fetch_cost_sync_work_order_ids(conn, cost_sync_id):
    """主单下已验证通过的明细工单 Id。"""
    sql = """
        SELECT WorkOrderId
        FROM finance_main.main_costsyncdetail
        WHERE Deleted = 0
          AND CostSyncId = %s
          AND AuditState = 1
          AND WorkOrderId IS NOT NULL
          AND WorkOrderId <> ''
        ORDER BY Id ASC
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (cost_sync_id,))
        return [r[0] for r in cursor.fetchall()]


def fetch_detail_data(conn, work_order_id):
    """
    对单个工单 Id 在壹好车服三张来源各查一条（有则加入结果）。
    返回 0～3 条 dict；三张都有则三条。
    """
    results = []
    with conn.cursor() as cursor:
        sql_1 = """
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
                WHERE a.WorkOrderId = %s
                ORDER BY a.CompleteTime DESC
                LIMIT 1
                """
        cursor.execute(sql_1, (work_order_id,))
        row = cursor.fetchone()
        if row:
            results.append(dict(zip([col[0] for col in cursor.description], row)))

        sql_2 = """
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
                  AND a.WorkOrderId = %s
                ORDER BY a.OperTime DESC
                LIMIT 1
                """
        cursor.execute(sql_2, (work_order_id,))
        row = cursor.fetchone()
        if row:
            results.append(dict(zip([col[0] for col in cursor.description], row)))

        sql_3 = """
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
                  AND a.TargetId = %s
                ORDER BY a.LastAuditTime DESC
                LIMIT 1
                """
        cursor.execute(sql_3, (work_order_id,))
        row = cursor.fetchone()
        if row:
            results.append(dict(zip([col[0] for col in cursor.description], row)))

    return results


def insert_to_target(conn, data, commit=True):
    """写入 workcount_log；commit=False 时由外层事务统一提交。"""
    if not data:
        return
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
    if commit:
        conn.commit()


def _run_one_main_sync_transaction(tgt_conn, main_id, rows_to_insert):
    """
    同一事务：清空 workcount_log、写入本批数据、调用存储过程、主单标记已同步，最后提交。
    TRUNCATE 会隐式提交，故用 DELETE。
    调用前须将 tgt_conn 置于 autocommit(False)。
    """
    proc = POST_COST_SYNC_PROCEDURE.replace('`', '')
    with tgt_conn.cursor() as cursor:
        cursor.execute('DELETE FROM workcount_log')
    insert_to_target(tgt_conn, rows_to_insert, commit=False)
    with tgt_conn.cursor() as cursor:
        cursor.execute(f'CALL `{proc}`()')
        while cursor.nextset():
            pass
    with tgt_conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE finance_main.main_costsyncinfo
            SET CostSyncState = 1, UpdatedAt = NOW()
            WHERE Id = %s AND Deleted = 0
            """,
            (main_id,),
        )
    tgt_conn.commit()


def sync_cost_sync_queue():
    """
    扫描 main_costsyncinfo（AuditState=1, CostSyncState=0），按明细 WorkOrderId
    逐单调用 fetch_detail_data（三张表各最多一条），汇总写入；
    每个主单：目标库单事务内 DELETE workcount_log → REPLACE 写入 → CALL 过程 → 更新主单完成。
    注意：本次任务只处理“启动时快照”的主单集合，避免跳过记录造成循环内重复命中。
    每日定时跑一次，下次任务再重新扫描未同步主单。
    """
    import os
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf', 'config.yaml')
    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)

    src_conn = get_db_conn(config['source_db'])
    tgt_conn = get_db_conn(config['target_db'])
    tgt_conn.autocommit(True)

    start_time = time.time()
    logger.info('[SYNC][COST] Start main_costsyncinfo queue sync')
    try:
        processed = 0
        pending_main_ids = fetch_pending_cost_sync_main_ids(tgt_conn)
        logger.info(f'[SYNC][COST] Snapshot pending mains: {len(pending_main_ids)}')
        for cost_sync_id in pending_main_ids:
            work_order_ids = fetch_cost_sync_work_order_ids(tgt_conn, cost_sync_id)
            logger.info(
                f'[SYNC][COST] Main Id={cost_sync_id}, work orders count={len(work_order_ids)}'
            )

            rows_to_insert = []
            for woid in work_order_ids:
                rows_to_insert.extend(fetch_detail_data(src_conn, woid))

            if not rows_to_insert and work_order_ids:
                logger.warning(
                    f'[SYNC][COST] Main Id={cost_sync_id}: 明细工单在三张来源均无数据，跳过本单（不更新 CostSyncState）'
                )
                continue

            try:
                tgt_conn.autocommit(False)
                _run_one_main_sync_transaction(tgt_conn, cost_sync_id, rows_to_insert)
                processed += 1
                logger.info(
                    f'[SYNC][COST] Main Id={cost_sync_id} completed, rows={len(rows_to_insert)}, CostSyncState=1'
                )
            except Exception as ex:
                tgt_conn.rollback()
                logger.exception(
                    f'[SYNC][COST] Main Id={cost_sync_id} transaction failed: {ex}'
                )
            finally:
                tgt_conn.autocommit(True)
        duration = time.time() - start_time
        logger.info(f'[SYNC][COST] Finished mains processed={processed}, duration={duration:.2f}s')
    except Exception as e:
        logger.exception(f'[SYNC][COST] Queue sync failed: {e}')
    finally:
        src_conn.close()
        tgt_conn.close()


def sync_task():
    """仅执行 main_costsyncinfo 单据同步队列。"""
    sync_cost_sync_queue()
