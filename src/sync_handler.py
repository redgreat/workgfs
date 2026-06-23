import os
import pymysql
import yaml
import time
from loguru import logger


def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf', 'config.yaml')
    with open(config_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_db_conn(db_config):
    # 使用 utf8mb4，避免 utf8(utf8mb3) 与库内四字节字符/函数返回值不兼容触发 1366
    return pymysql.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset=db_config.get('charset', 'utf8mb4'),
        use_unicode=True,
    )


_mainpart_cache_by_order = {}
_mainpart_cache_by_customer = {}
_cached_config = None


def _get_config_cached():
    global _cached_config
    if _cached_config is None:
        _cached_config = load_config()
    return _cached_config


def _ensure_conn_alive(conn, conn_name):
    """在长任务关键节点前探活连接，必要时自动重连。"""
    try:
        conn.ping(reconnect=True)
    except Exception:
        logger.exception(f'[SYNC][DB] {conn_name} ping/reconnect failed')
        raise


def _close_conn_quietly(conn, conn_name):
    if conn is None:
        return
    try:
        conn.close()
    except Exception as ex:
        logger.warning(f'[SYNC][DB] {conn_name} close failed: {ex}')


def _normalize_batch_size(batch_size):
    try:
        value = int(batch_size)
    except (TypeError, ValueError):
        value = 10000
    return max(1, value)


def _is_blank(v):
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == '':
        return True
    return False


def _guess_first_name(s):
    if _is_blank(s):
        return None
    raw = str(s).strip()
    if not raw:
        return None
    for sep in [',', '，', ';', '；', '、', '|', '\n', '\r', '\t', '/', '\\']:
        if sep in raw:
            head = raw.split(sep, 1)[0].strip()
            return head or raw
    return raw


def _fetch_mainpart_by_order(order_id, sale_name, config):
    if _is_blank(order_id) or _is_blank(sale_name):
        return None, None
    cache_key = (str(order_id), str(sale_name))
    if cache_key in _mainpart_cache_by_order:
        return _mainpart_cache_by_order[cache_key]
    if 'mall_db' not in config:
        _mainpart_cache_by_order[cache_key] = (None, None)
        return None, None
    mall_conn = get_db_conn(config['mall_db'])
    try:
        with mall_conn.cursor() as cursor:
            sql = """
                SELECT c.MainPartId, c.MainPartName
                FROM tb_orderinfo a
                JOIN tb_orderitem b
                  ON b.OrderId = a.Id
                 AND b.SaleName = %s
                 AND b.Deleted = 0
                JOIN tb_orderitemdetail c
                  ON c.ItemId = b.Id
                 AND c.Deleted = 0
                WHERE a.Deleted = 0
                  AND a.Id = %s
                LIMIT 1
            """
            cursor.execute(sql, (sale_name, order_id))
            r = cursor.fetchone()
            if not r:
                _mainpart_cache_by_order[cache_key] = (None, None)
                return None, None
            main_part_id, main_part_name = r[0], r[1]
            _mainpart_cache_by_order[cache_key] = (main_part_id, main_part_name)
            return main_part_id, main_part_name
    except Exception:
        logger.exception(
            f'[SYNC][MAINPART] mallcenter 查询主体失败, OrderId={order_id}, SaleName={sale_name}'
        )
        _mainpart_cache_by_order[cache_key] = (None, None)
        return None, None
    finally:
        mall_conn.close()


def _fetch_mainpart_by_customer(customer_id, config):
    if _is_blank(customer_id):
        return None, None
    cache_key = str(customer_id)
    if cache_key in _mainpart_cache_by_customer:
        return _mainpart_cache_by_customer[cache_key]
    if 'cust_db' not in config:
        _mainpart_cache_by_customer[cache_key] = (None, None)
        return None, None
    cust_conn = get_db_conn(config['cust_db'])
    try:
        with cust_conn.cursor() as cursor:
            sql = """
                SELECT d.Id AS MainPartId, d.MainPartName
                FROM tb_composecust a
                JOIN tb_matemainpartcom b
                  ON b.ComposeId = a.ComposeId
                 AND b.Deleted = 0
                JOIN tb_materialmainpart c
                  ON c.Id = b.MateMainPartId
                 AND c.MaterialTypeCode LIKE '03%%'
                 AND c.Enabled = 1
                 AND c.Deleted = 0
                JOIN tb_contractmainpart d
                  ON d.Id = c.ConMainPartId
                 AND d.Enabled = 1
                 AND d.Deleted = 0
                WHERE a.Deleted = 0
                  AND a.CustId = %s
                LIMIT 1
            """
            cursor.execute(sql, (customer_id,))
            r = cursor.fetchone()
            if not r:
                _mainpart_cache_by_customer[cache_key] = (None, None)
                return None, None
            main_part_id, main_part_name = r[0], r[1]
            _mainpart_cache_by_customer[cache_key] = (main_part_id, main_part_name)
            return main_part_id, main_part_name
    except Exception:
        logger.exception(f'[SYNC][MAINPART] customercenter 查询主体失败, CustomerId={customer_id}')
        _mainpart_cache_by_customer[cache_key] = (None, None)
        return None, None
    finally:
        cust_conn.close()


def _fill_mainpart_fields(detail_row, config):
    if not isinstance(detail_row, dict):
        return detail_row
    if (not _is_blank(detail_row.get('MainPartId'))) and (not _is_blank(detail_row.get('MainPartName'))):
        return detail_row

    order_id = detail_row.get('OrderId')
    candidates = []
    general_goods = detail_row.get('GeneralGoodsNames')
    artificial_goods = detail_row.get('ArtificialServicePriceName')
    for s in [general_goods, artificial_goods]:
        first = _guess_first_name(s)
        if first and first not in candidates:
            candidates.append(first)
        if not _is_blank(s):
            raw = str(s).strip()
            if raw and raw not in candidates:
                candidates.append(raw)

    if not _is_blank(order_id):
        for sale_name in candidates:
            main_part_id, main_part_name = _fetch_mainpart_by_order(order_id, sale_name, config)
            if not _is_blank(main_part_id) or not _is_blank(main_part_name):
                detail_row['MainPartId'] = main_part_id
                detail_row['MainPartName'] = main_part_name
                return detail_row

    customer_id = detail_row.get('CustomerId')
    main_part_id, main_part_name = _fetch_mainpart_by_customer(customer_id, config)
    if not _is_blank(main_part_id) or not _is_blank(main_part_name):
        detail_row['MainPartId'] = main_part_id
        detail_row['MainPartName'] = main_part_name
    return detail_row


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


def fetch_detail_data(conn, work_order_id, cost_sync_id):
    """
    按工单 Id 从壹好车服合并查询（vi_workcount / 调价 / 费用申请），
    以 tb_workorderinfo 为主表，同一工单固定返回 0～1 条。
    """
    sql = """
        SELECT
          wo.Id AS WorkOrderId,
          CONCAT(
            IFNULL(wo.Id, ''),
            '-CT-',
            %s
          ) AS CostNo,
          COALESCE(v.AppCode, e.AppCode, f.AppCode, wo.AppCode) AS AppCode,
          IFNULL(wo.ServiceProviderCode, '1001') AS ServiceProviderCode,
          COALESCE(v.OrderId, e.OrderId, f.OrderId) AS OrderId,
          COALESCE(v.OrderNo, e.OrderNo, f.OrderNo) AS OrderNo,
          COALESCE(v.OrderType, e.OrderType, f.OrderType, 1) AS OrderType,
          COALESCE(
            v.WorkOrderType,
            e.WorkOrderType,
            f.WorkOrderType,
            fn_GetOrderTypeByCode(wo.OrderType)
          ) AS WorkOrderType,
          COALESCE(
            v.WorkStatus,
            e.WorkStatus,
            f.WorkStatus,
            fn_GetServiceOrderStatus(wo.WorkStatus)
          ) AS WorkStatus,
          COALESCE(v.ProName, e.ProName, f.ProName, wo.ProName) AS ProName,
          COALESCE(v.CityName, e.CityName, f.CityName, wo.CityName) AS CityName,
          COALESCE(v.AreaName, e.AreaName, f.AreaName, wo.AreaName) AS AreaName,
          COALESCE(v.InstallAddress, e.InstallAddress, f.InstallAddress, wo.InstallAddress) AS InstallAddress,
          COALESCE(v.CustSettleId, e.CustSettleId, f.CustSettleId, wo.CustSettleId) AS CustSettleId,
          COALESCE(v.CustSettleName, e.CustSettleName, f.CustSettleName, wo.CustSettleName) AS CustSettleName,
          COALESCE(v.CustomerId, e.CustomerId, f.CustomerId, wo.CustomerId) AS CustomerId,
          COALESCE(v.CustomerName, e.CustomerName, f.CustomerName, wo.CustomerName) AS CustomerName,
          COALESCE(v.CustStoreId, e.CustStoreId, f.CustStoreId, wo.CustStoreId) AS CustStoreId,
          COALESCE(v.CustStoreName, e.CustStoreName, f.CustStoreName, wo.CustStoreName) AS CustStoreName,
          COALESCE(v.ActualCustStoreName, e.ActualCustStoreName, f.ActualCustStoreName) AS ActualCustStoreName,
          NULL AS MainPartId,
          NULL AS MainPartName,
          COALESCE(v.GeneralGoodsNames, e.GeneralGoodsNames, f.GeneralGoodsNames) AS GeneralGoodsNames,
          COALESCE(v.ArtificialServicePriceName, e.ArtificialServicePriceName, f.ArtificialServicePriceName) AS ArtificialServicePriceName,
          COALESCE(v.ArtificialServicePrice, e.ArtificialServicePrice, f.ArtificialServicePrice) AS ArtificialServicePrice,
          COALESCE(v.ServiceSubjectName, e.ServiceSubjectName, f.ServiceSubjectName) AS ServiceSubjectName,
          COALESCE(v.SubjectClassCode, e.SubjectClassCode, f.SubjectClassCode) AS SubjectClassCode,
          COALESCE(v.ServiceSubjectCode, e.ServiceSubjectCode, f.ServiceSubjectCode) AS ServiceSubjectCode,
          COALESCE(v.InternalPrice, e.InternalPrice, f.InternalPrice) AS InternalPrice,
          COALESCE(v.CostReason, e.CostReason, f.CostReason) AS CostReason,
          COALESCE(v.CostRemark, e.CostRemark, f.CostRemark, '基础计价') AS CostRemark,
          COALESCE(v.FinishTime, e.FinishTime, f.FinishTime) AS FinishTime,
          COALESCE(v.CostConfirmTime, e.CostConfirmTime, f.CostConfirmTime) AS CostConfirmTime,
          COALESCE(v.Privoder, e.Privoder, f.Privoder) AS Privoder,
          COALESCE(v.IsCentralize, e.IsCentralize, f.IsCentralize) AS IsCentralize,
          COALESCE(v.VinNumber, e.VinNumber, f.VinNumber) AS VinNumber,
          COALESCE(v.GuaVin, e.GuaVin, f.GuaVin) AS GuaVin,
          COALESCE(v.PlateNumber, e.PlateNumber, f.PlateNumber) AS PlateNumber,
          COALESCE(v.CompleteTime, e.CompleteTime, f.CompleteTime) AS CompleteTime,
          COALESCE(v.CreatePersonName, e.CreatePersonName, f.CreatePersonName, wo.CreatePersonName) AS CreatePersonName,
          COALESCE(v.ServiceCode, e.ServiceCode, f.ServiceCode) AS ServiceCode,
          COALESCE(v.ServiceName, e.ServiceName, f.ServiceName) AS ServiceName,
          COALESCE(v.ServiceAscription, e.ServiceAscription, f.ServiceAscription) AS ServiceAscription,
          COALESCE(v.ActualRecordPersonCode, e.ActualRecordPersonCode, f.ActualRecordPersonCode) AS ActualRecordPersonCode,
          COALESCE(v.ActualRecordPersonName, e.ActualRecordPersonName, f.ActualRecordPersonName) AS ActualRecordPersonName,
          COALESCE(v.ActualRecordPersonAscription, e.ActualRecordPersonAscription, f.ActualRecordPersonAscription) AS ActualRecordPersonAscription,
          COALESCE(v.SendRemark, e.SendRemark, f.DispatchRemark, wo.Remark) AS SendRemark,
          COALESCE(v.ServiceRemark, e.ServiceRemark, f.ServiceRemark) AS ServiceRemark,
          COALESCE(v.TagSign, e.TagSign, f.TagSign, '否') AS TagSign,
          COALESCE(v.ChangeRemark, e.ChangeRemark, f.ChangeRemark) AS ChangeRemark
        FROM tb_workorderinfo wo
        LEFT JOIN (
          SELECT
            a.Id,
            a.WorkOrderId,
            a.AppCode,
            (SELECT MallOrderId FROM tb_workgoodsinfo b WHERE b.WorkOrderId = a.WorkOrderId AND b.GoodsType IN (5, 10, 11, 18, 37, 38) AND b.Deleted = 0 LIMIT 1) AS OrderId,
            (SELECT OrderNo FROM tb_workgoodsinfo c WHERE c.WorkOrderId = a.WorkOrderId AND c.GoodsType IN (5, 10, 11, 18, 37, 38) AND c.Deleted = 0 LIMIT 1) AS OrderNo,
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
            GetAscriptionByLoginName(
              CAST(CONVERT(TRIM(IFNULL(a.ServiceCode, '')) USING latin1) AS CHAR(50) CHARACTER SET utf8),
              1
            ) AS ServiceAscription,
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
        ) v ON TRUE
        LEFT JOIN (
          SELECT
            a.Id,
            a.WorkOrderId,
            a.AppCode,
            (SELECT c.MallOrderId FROM tb_workgoodsinfo c WHERE c.WorkOrderId = a.WorkOrderId AND c.GoodsType IN (5, 10, 11, 18, 37, 38) AND c.Deleted = 0 LIMIT 1) AS OrderId,
            (SELECT d.OrderNo FROM tb_workgoodsinfo d WHERE d.WorkOrderId = a.WorkOrderId AND d.GoodsType IN (5, 10, 11, 18, 37, 38) AND d.Deleted = 0 LIMIT 1) AS OrderNo,
            1 AS OrderType,
            a.OrderTypeName AS WorkOrderType,
            b.WorkStatusName AS WorkStatus,
            b.ProName,
            b.CityName,
            b.AreaName,
            b.InstallAddress,
            NULL AS CustSettleId,
            b.CustSettleName,
            NULL AS CustomerId,
            b.CustomerName,
            NULL AS CustStoreId,
            b.CustStoreName,
            b.ActualCustStoreName,
            b.GeneralGoodsNames,
            a.ArtificialServicePriceName,
            b.ArtificialServicePrice,
            b.ServiceSubjectName,
            b.SubjectClassCode,
            b.ServiceSubjectCode,
            (a.InternalPrice - b.InternalPrice) AS InternalPrice,
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
            GetAscriptionByLoginName(
              CAST(CONVERT(TRIM(IFNULL(b.ServiceCode, '')) USING latin1) AS CHAR(50) CHARACTER SET utf8),
              1
            ) AS ServiceAscription,
            b.ActualRecordPersonCode,
            b.ActualRecordPersonName,
            b.ActualRecordPersonAscription,
            b.SendRemark,
            b.ServiceRemark,
            '是' AS TagSign,
            a.Remark AS ChangeRemark
          FROM tb_workpriceedit_log a
          JOIN vi_workcount_log b
            ON b.WorkOrderId = a.WorkOrderId
           AND a.ArtificialServicePriceId = b.ArtificialServicePriceId
           AND b.TagSign = '是'
          WHERE MONTH(a.OperTime) <> MONTH(b.CompleteTime)
            AND a.WorkOrderId = %s
          ORDER BY a.OperTime DESC
          LIMIT 1
        ) e ON TRUE
        LEFT JOIN (
          SELECT
            a.Id,
            a.TargetId AS WorkOrderId,
            b.AppCode,
            (SELECT MallOrderId FROM tb_workgoodsinfo h WHERE h.WorkOrderId = b.Id AND h.GoodsType IN (5, 10, 11, 18) AND h.Deleted = 0 LIMIT 1) AS OrderId,
            (SELECT OrderNo FROM tb_workgoodsinfo i WHERE i.WorkOrderId = b.Id AND i.GoodsType IN (5, 10, 11, 18, 37, 38) AND i.Deleted = 0 LIMIT 1) AS OrderNo,
            2 AS OrderType,
            fn_GetOrderTypeByCode(b.OrderType) AS WorkOrderType,
            fn_GetStatusNameByCode(a.ApplyWorkStatus) AS WorkStatus,
            b.ProName,
            b.CityName,
            b.AreaName,
            b.InstallAddress,
            b.CustSettleId,
            b.CustSettleName,
            b.CustomerId,
            b.CustomerName,
            b.CustStoreId,
            b.CustStoreName,
            NULL AS ActualCustStoreName,
            (SELECT SaleName FROM tb_workgoodsinfo j WHERE j.WorkOrderId = b.Id AND j.GoodsType = 0 AND j.Deleted = 0 LIMIT 1) AS GeneralGoodsNames,
            (SELECT SaleName FROM tb_workgoodsinfo k WHERE k.WorkOrderId = b.Id AND k.GoodsType IN (5, 10, 11, 18, 37, 38) AND k.Deleted = 0 LIMIT 1) AS ArtificialServicePriceName,
            NULL AS ArtificialServicePrice,
            c.SubjectNameSummary AS ServiceSubjectName,
            c.SubjectCodeSummary AS SubjectClassCode,
            NULL AS ServiceSubjectCode,
            a.ApplyFee AS InternalPrice,
            a.ApplyReason AS CostReason,
            l.Remark AS CostRemark,
            a.FeeApplyTime AS FinishTime,
            a.LastAuditTime AS CostConfirmTime,
            CASE d.Privoder WHEN 0 THEN '中瑞' WHEN 1 THEN '客户' END AS Privoder,
            CASE d.ServiceType WHEN 4 THEN '常规安装' WHEN 5 THEN '上门安装' WHEN 6 THEN '集中安装' WHEN 7 THEN '道路救援' END AS IsCentralize,
            g.VinNumber,
            f.`Value` AS GuaVin,
            g.PlateNumber,
            a.LastAuditTime AS CompleteTime,
            a.ApplyPersonName AS CreatePersonName,
            d.ServiceCode,
            d.ServiceName,
            /* 入参可能含非法 UTF-8，直接进 GetAscriptionByLoginName 会与 loginname 比较触发 1366；先经 latin1 有损转码再 CAST 回 utf8 */
            GetAscriptionByLoginName(
              CAST(CONVERT(TRIM(IFNULL(d.ServiceCode, '')) USING latin1) AS CHAR(50) CHARACTER SET utf8),
              1
            ) AS ServiceAscription,
            fn_GetRecordCodeById(a.TargetId) AS ActualRecordPersonCode,
            fn_GetRecordNameById(a.TargetId) AS ActualRecordPersonName,
            GetAscriptionByLoginName(
              CAST(CONVERT(TRIM(IFNULL(fn_GetRecordCodeById(a.TargetId), '')) USING latin1) AS CHAR(50) CHARACTER SET utf8),
              1
            ) AS ActualRecordPersonAscription,
            NULL AS SendRemark,
            d.Remark AS ServiceRemark,
            /* 避免 fn_GetDispatchRemarkById 内 INTO/脏字节触发 1366：与函数同逻辑内联，并对 Remark 做 utf8 安全清洗 */
            (SELECT CAST(CONVERT(TRIM(IFNULL(wa.Remark, '')) USING latin1) AS CHAR(500) CHARACTER SET utf8)
             FROM workflowruntimeitems wi
             JOIN workflowruntimesteps ws ON ws.RuntimeItemId = wi.Id
                AND ws.Name IN ('分派工单', '分派', '重新派单', '分配工单', '派单', '重新调度', '调度工单')
                AND ws.`Status` = 'ACCEPTED'
                AND ws.Deleted = 0
             JOIN workflowruntimeactors wa ON wa.RuntimeStepId = ws.Id
                AND wa.`Status` = 'ACCEPTED'
                AND wa.Deleted = 0
             WHERE wi.TargetEntityId = a.TargetId
               AND wi.Deleted = 0
             ORDER BY ws.DoneAt DESC
             LIMIT 1) AS DispatchRemark,
            '否' AS TagSign,
            NULL AS ChangeRemark
          FROM tb_feeapplicationinfo a
          JOIN tb_workorderinfo b
            ON a.TargetId = b.Id
           AND b.Deleted = 0
          LEFT JOIN tb_worksubjectsummary c
            ON b.Id = c.WorkOrderId
           AND c.Deleted = 0
          LEFT JOIN tb_workserviceinfo d
            ON d.WorkOrderId = b.Id
           AND d.Deleted = 0
          LEFT JOIN tb_custcolumn f
            ON f.WorkOrderId = b.Id
           AND f.TypeName = '挂车车架号'
           AND f.Deleted = 0
          JOIN tb_workcarinfo g
            ON g.WorkOrderId = b.Id
           AND g.Deleted = 0
          LEFT JOIN tb_feeiteminfo l
            ON l.Id = a.FeeItemId
           AND l.Deleted = 0
          WHERE a.TargetId = %s
          ORDER BY a.LastAuditTime DESC
          LIMIT 1
        ) f ON TRUE
        WHERE wo.Id = %s
    """
    params = (cost_sync_id, work_order_id, work_order_id, work_order_id, work_order_id)
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        desc = cursor.description
    if not row:
        return []
    detail = dict(zip([col[0] for col in desc], row))
    _fill_mainpart_fields(detail, _get_config_cached())
    return [detail]


def insert_to_target(conn, data, commit=True, debug=False, batch_size=10000):
    """写入 workcount_log；commit=False 时由外层事务统一提交。"""
    if not data:
        if debug:
            logger.warning('[SYNC][INSERT] workcount_log 无数据，跳过写入')
        return
    batch_size = _normalize_batch_size(batch_size)
    keys = ['WorkOrderId', 'CostNo', 'AppCode', 'ServiceProviderCode', 'OrderId', 'OrderNo', 'OrderType', 'WorkOrderType', 'WorkStatus',
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
    from decimal import Decimal
    with conn.cursor() as cursor:
        if debug:
            total_batches = (len(data) + batch_size - 1) // batch_size
            logger.info(
                f'[SYNC][INSERT] workcount_log 待写入 {len(data)} 行, batch_size={batch_size}, '
                f'batches={total_batches}, SQL 模板: {sql}'
            )
        total_rowcount = 0
        for start in range(0, len(data), batch_size):
            batch = data[start:start + batch_size]
            values = []
            for item in batch:
                row = []
                for k in keys:
                    v = item.get(k)
                    if isinstance(v, Decimal):
                        v = str(v)
                    row.append(v)
                values.append(tuple(row))
            if debug:
                batch_no = start // batch_size + 1
                logger.info(
                    f'[SYNC][INSERT] batch {batch_no} 开始写入, rows={len(values)}, '
                    f'range={start + 1}-{start + len(values)}'
                )
            cursor.executemany(sql, values)
            total_rowcount += cursor.rowcount
            if debug:
                logger.info(
                    f'[SYNC][INSERT] batch {start // batch_size + 1} 写入完成, rowcount={cursor.rowcount}'
                )
        if debug:
            logger.info(f'[SYNC][INSERT] executemany 全部完成, total_rowcount={total_rowcount}')
    if commit:
        conn.commit()
        if debug:
            logger.info('[SYNC][INSERT] workcount_log 已 commit')


def _run_one_main_sync_transaction(tgt_conn, main_id, rows_to_insert, debug=False, batch_size=10000):
    """
    同一事务：清空 workcount_log、写入本批数据、调用存储过程、主单标记已同步，最后提交。
    TRUNCATE 会隐式提交，故用 DELETE。
    调用前须将 tgt_conn 置于 autocommit(False)。
    """
    if debug:
        logger.info(
            f'[SYNC][INSERT] 主单 main_costsyncinfo.Id={main_id}, '
            f'本批写入前 DELETE workcount_log, 行数={len(rows_to_insert)}'
        )
    logger.info(
        f'[SYNC][COST] Main Id={main_id}: transaction start, rows_to_insert={len(rows_to_insert)}, '
        f'batch_size={batch_size}'
    )
    with tgt_conn.cursor() as cursor:
        cursor.execute('DELETE FROM workcount_log')
    insert_to_target(tgt_conn, rows_to_insert, commit=False, debug=debug, batch_size=batch_size)
    logger.info(f'[SYNC][COST] Main Id={main_id}: workcount_log write finished, calling procedure')
    with tgt_conn.cursor() as cursor:
        cursor.execute('CALL finance_main.proc_InsertCostInfo_ehcf(%s)', (main_id,))
        while cursor.nextset():
            pass
    logger.info(f'[SYNC][COST] Main Id={main_id}: procedure finished, updating CostSyncState')
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
    logger.info(f'[SYNC][COST] Main Id={main_id}: transaction committed')


def _fetch_work_order_ids_for_main(config, cost_sync_id):
    """每个主单使用短连接查询其已验证通过的工单列表。"""
    tgt_conn = get_db_conn(config['target_db'])
    try:
        tgt_conn.autocommit(True)
        _ensure_conn_alive(tgt_conn, 'target_db')
        return fetch_cost_sync_work_order_ids(tgt_conn, cost_sync_id)
    finally:
        _close_conn_quietly(tgt_conn, 'target_db')


def _sync_one_main_with_retry(config, cost_sync_id, rows_to_insert, debug=False, max_attempts=2):
    """每个主单事务使用独立目标库连接；失败时自动重试一次。"""
    last_ex = None
    batch_size = _normalize_batch_size(config.get('batch_size', 10000))
    for attempt in range(1, max_attempts + 1):
        tgt_conn = None
        try:
            tgt_conn = get_db_conn(config['target_db'])
            tgt_conn.autocommit(True)
            _ensure_conn_alive(tgt_conn, 'target_db')
            tgt_conn.autocommit(False)
            _run_one_main_sync_transaction(
                tgt_conn,
                cost_sync_id,
                rows_to_insert,
                debug=debug,
                batch_size=batch_size,
            )
            return True
        except Exception as ex:
            last_ex = ex
            try:
                if tgt_conn is not None:
                    _ensure_conn_alive(tgt_conn, 'target_db')
                    tgt_conn.rollback()
            except Exception as rollback_ex:
                logger.warning(
                    f'[SYNC][COST] Main Id={cost_sync_id} attempt={attempt} rollback skipped: {rollback_ex}'
                )
            logger.exception(
                f'[SYNC][COST] Main Id={cost_sync_id} attempt={attempt}/{max_attempts} transaction failed: {ex}'
            )
            if attempt >= max_attempts:
                return False
            logger.warning(
                f'[SYNC][COST] Main Id={cost_sync_id} retrying transaction, attempt={attempt + 1}/{max_attempts}'
            )
        finally:
            if tgt_conn is not None:
                try:
                    _ensure_conn_alive(tgt_conn, 'target_db')
                    tgt_conn.autocommit(True)
                except Exception as autocommit_ex:
                    logger.warning(
                        f'[SYNC][COST] Main Id={cost_sync_id} reset autocommit failed: {autocommit_ex}'
                    )
                _close_conn_quietly(tgt_conn, 'target_db')
    if last_ex:
        raise last_ex
    return False


def sync_cost_sync_queue():
    """
    扫描 main_costsyncinfo（AuditState=1, CostSyncState=0），按明细 WorkOrderId
    逐单调用 fetch_detail_data（合并查询，每工单 0～1 条），汇总写入；
    每个主单：目标库单事务内 DELETE workcount_log → REPLACE 写入 → CALL 过程 → 更新主单完成。
    注意：本次任务只处理“启动时快照”的主单集合，避免跳过记录造成循环内重复命中。
    每日定时跑一次，下次任务再重新扫描未同步主单。
    """
    config = load_config()
    global _cached_config
    _cached_config = config
    _mainpart_cache_by_order.clear()
    _mainpart_cache_by_customer.clear()
    debug = bool(config.get('sync_debug', False))
    if debug:
        logger.info('[SYNC][COST] sync_debug=true，已开启 INSERT SQL 及逐工单拉数明细日志')

    src_conn = get_db_conn(config['source_db'])
    snapshot_conn = get_db_conn(config['target_db'])
    snapshot_conn.autocommit(True)

    start_time = time.time()
    logger.info('[SYNC][COST] Start main_costsyncinfo queue sync')
    try:
        processed = 0
        _ensure_conn_alive(snapshot_conn, 'target_db')
        pending_main_ids = fetch_pending_cost_sync_main_ids(snapshot_conn)
        logger.info(f'[SYNC][COST] Snapshot pending mains: {len(pending_main_ids)}')
        total_mains = len(pending_main_ids)
        for main_idx, cost_sync_id in enumerate(pending_main_ids, 1):
            main_start_time = time.time()
            logger.info(f'[SYNC][COST] Main progress {main_idx}/{total_mains}, Id={cost_sync_id}: start')
            work_order_ids = _fetch_work_order_ids_for_main(config, cost_sync_id)
            logger.info(
                f'[SYNC][COST] Main Id={cost_sync_id}, work orders count={len(work_order_ids)}'
            )

            rows_to_insert = []
            last_progress_log_at = main_start_time
            total_work_orders = len(work_order_ids)
            for work_idx, woid in enumerate(work_order_ids, 1):
                now = time.time()
                if (
                    work_idx == 1
                    or work_idx == total_work_orders
                    or work_idx % 500 == 0
                    or now - last_progress_log_at >= 30
                ):
                    logger.info(
                        f'[SYNC][COST] Main Id={cost_sync_id}: fetching detail progress '
                        f'{work_idx}/{total_work_orders}, rows_buffered={len(rows_to_insert)}'
                    )
                    last_progress_log_at = now
                _ensure_conn_alive(src_conn, 'source_db')
                rows = fetch_detail_data(src_conn, woid, cost_sync_id)
                if debug:
                    if not rows:
                        logger.warning(
                            f'[SYNC][COST] Main Id={cost_sync_id}, WorkOrderId={woid}: '
                            '合并查询无行（tb_workorderinfo 不存在或未命中）'
                        )
                    else:
                        logger.info(
                            f'[SYNC][COST] Main Id={cost_sync_id}, WorkOrderId={woid}: '
                            f'拉取 1 行, Id={rows[0].get("Id")}, CostNo={rows[0].get("CostNo")}'
                        )
                rows_to_insert.extend(rows)

            if not rows_to_insert and work_order_ids:
                logger.warning(
                    f'[SYNC][COST] Main Id={cost_sync_id}: 明细工单在 tb_workorderinfo 无数据，跳过本单（不更新 CostSyncState）'
                )
                continue

            logger.info(
                f'[SYNC][COST] Main Id={cost_sync_id}: detail fetch finished, rows_to_insert={len(rows_to_insert)}, '
                f'elapsed={time.time() - main_start_time:.2f}s'
            )
            if _sync_one_main_with_retry(config, cost_sync_id, rows_to_insert, debug=debug):
                processed += 1
                logger.info(
                    f'[SYNC][COST] Main Id={cost_sync_id} completed, rows={len(rows_to_insert)}, '
                    f'CostSyncState=1, elapsed={time.time() - main_start_time:.2f}s'
                )
        duration = time.time() - start_time
        logger.info(f'[SYNC][COST] Finished mains processed={processed}, duration={duration:.2f}s')
    except Exception as e:
        logger.exception(f'[SYNC][COST] Queue sync failed: {e}')
    finally:
        src_conn.close()
        _close_conn_quietly(snapshot_conn, 'target_db')


def sync_task():
    """仅执行 main_costsyncinfo 单据同步队列。"""
    sync_cost_sync_queue()
