WITH 
	grass_date(grass_date) AS (SELECT * FROM UNNEST(SEQUENCE(date'2015-01-01', current_date - interval '1' day))) 

-- COST
, pickup_order AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status =2
    )
, schedule_order AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status =1
    )
, pickup_failed AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status = 4
    )



, airpay_fsv AS 
	(SELECT
	    *
	FROM
	    shopee_vn_bi_team__fsv_type
	WHERE
	    ingestion_timestamp = (SELECT MAX(ingestion_timestamp) FROM shopee_vn_bi_team__fsv_type)
	AND
	    fsv_type like '%FSS%')

, log_cong_data as 
    (SELECT
        orderid
        , sum(scheduled_net_cpo) scheduled_net_cpo
        , sum(true_cost_usd) true_cost_usd
        , sum(cashback) cashback
        , sum(be_esf_usd) be_esf_usd
        , sum(seller_rebate) seller_rebate
        , sum(scheduled_bpsf_usd) scheduled_bpsf_usd
        , sum(return_charged) return_charged
    FROM shopee_vn_anlys.shopee_bi_team_logistics_masking_cpo
    GROUP BY 1
    )


, data AS 
    (
        (SELECT
            shop_id shopid,
            DATE(CAST(o.create_datetime AS TIMESTAMP)) AS grass_date,
            order_id,
            CASE WHEN p.orderid IS NOT NULL THEN 1 --  pickup order
                WHEN s.orderid IS NOT NULL AND  pf.orderid IS NULL AND o.order_be_status <> 'INVALID' THEN 1
                WHEN o.order_be_status IN ('ESCROW_PENDING','ESCROW_VERIFIED','ESCROW_CREATED','ESCROW_PAYOUT','ESCROW_PAID','PAID','COMPLETED','UNPAID') THEN 1 -- if not yet picked up, use not yet cancelled order
             ELSE 0 END AS is_pick_up,
            cpo.scheduled_net_cpo
            -- CASE WHEN a.promotionid IS NULL THEN 0 ELSE 1 END AS is_ap
        FROM
            order_mart_dwd_order_all_event_final_status_df o 
        JOIN
            log_cong_data cpo
        ON
            cpo.orderid = o.order_id
        LEFT JOIN
            pickup_order p
        ON
            p.orderid = o.order_id
        LEFT JOIN
            schedule_order s
        ON
            s.orderid = o.order_id
        LEFT JOIN
            pickup_failed pf
        ON
            pf.orderid = o.order_id
        JOIN
            airpay_fsv a
        ON
            TRY_CAST(a.promotionid AS BIGINT) = o.fsv_promotion_id
        WHERE
            DATE(CAST(o.create_datetime AS TIMESTAMP)) BETWEEN date'2021-01-01' AND date_add('day',-1,current_date)
            AND trim(o.shipping_carrier) not in ('Nhanh', 'TiếtKiệm', 'HỏaTốc')
        )
        UNION
        (SELECT
        shop_id shopid,
            DATE(CAST(o.create_datetime AS TIMESTAMP)) AS grass_date,
            order_id,
            CASE WHEN p.orderid IS NOT NULL THEN 1 --  pickup order
                WHEN s.orderid IS NOT NULL AND  pf.orderid IS NULL AND o.order_be_status <> 'INVALID' THEN 1
                WHEN o.order_be_status IN ('ESCROW_PENDING','ESCROW_VERIFIED','ESCROW_CREATED','ESCROW_PAYOUT','ESCROW_PAID','PAID','COMPLETED','UNPAID') THEN 1 -- if not yet picked up, use not yet cancelled order
             ELSE 0 END AS is_pick_up,
            cpo.scheduled_net_cpo
            -- CASE WHEN a.promotionid IS NULL THEN 0 ELSE 1 END AS is_ap
        FROM
            order_mart_dwd_order_all_event_final_status_df o 
        JOIN
            shopee_vn_anlys.shopee_bi_team_logistics_masking_cpo_2020 cpo
        ON
            cpo.orderid = o.order_id
        LEFT JOIN
            pickup_order p
        ON
            p.orderid = o.order_id
        LEFT JOIN
            schedule_order s
        ON
            s.orderid = o.order_id
        LEFT JOIN
            pickup_failed pf
        ON
            pf.orderid = o.order_id
        JOIN
            airpay_fsv a
        ON
            TRY_CAST(a.promotionid AS BIGINT) = o.fsv_promotion_id
        WHERE
            DATE(CAST(o.create_datetime AS TIMESTAMP)) BETWEEN date'2020-07-01' AND date'2020-12-31'
            AND trim(o.shipping_carrier) not in ('Nhanh', 'TiếtKiệm', 'HỏaTốc')
        ))


-- REVENUE 
, ex AS (
   SELECT
     grass_date
   , exchange_rate
   FROM
     shopee_vn.order_mart_dim_exchange_rate
   WHERE (grass_region = 'VN')
  AND grass_date >= date'2015-01-01'
) 

, paid_ads AS (
   SELECT
     o.grass_date
   -- , group_cat
   -- , main_category
   -- , fss_status
   -- , seller_type
   , ((sum(CAST(COALESCE(ads_expenditure_amt_local, 0) AS double)) / avg(exchange_rate)) / DECIMAL '1.1') paid_ads_rev
   FROM
     	shopee_vn.paid_ads_ads_advertise_mkt_1d o
   LEFT JOIN ex e ON (e.grass_date = o.grass_date)
   -- LEFT JOIN final_sellers f ON ((o.grass_date = f.grass_date) AND (o.shop_id = f.shop_id)))
   WHERE ((o.grass_date BETWEEN DATE'2015-01-01' AND "date_add"('day', -1, current_date))  AND ((is_ads_active = 1) OR (has_performance = 1)))
   GROUP BY 1
) 
, revenue AS (
   SELECT
     "date"(CAST(o.create_datetime AS timestamp)) grass_date
   -- , group_cat
   -- , main_category
   -- , fss_status
   -- , seller_type
   , ("sum"((CASE WHEN (is_net_order = 1) THEN commission_fee_usd ELSE 0 END)) / DECIMAL '1.1') commission_fee
   , ("sum"((CASE WHEN (is_net_order = 1) THEN service_fee_usd ELSE 0 END)) / DECIMAL '1.1') service_fee
   , ("sum"((CASE WHEN (is_net_order = 1) THEN seller_txn_fee_usd ELSE 0 END)) / DECIMAL '1.1') handling_fee
   FROM
   		shopee_vn.order_mart_dwd_order_item_all_event_final_status_df o
   -- LEFT JOIN final_sellers f ON ((o.shop_id = f.shop_id) AND ("date"(CAST(o.create_datetime AS timestamp)) = f.grass_date)))
   LEFT JOIN ex e ON ("date"(CAST(o.create_datetime AS timestamp)) = e.grass_date)
   WHERE (((date(CAST(o.create_datetime AS timestamp)) BETWEEN DATE'2015-01-01' AND date_add('day', -1, current_date)) AND (bi_exclude_reason IS NULL)))
   GROUP BY 1
) 


, sales AS -- add logistics cost 
	(
	SELECT
	    YEAR(CAST(o.create_datetime AS timestamp)) year,
		month(CAST(o.create_datetime AS timestamp)) month,
		count(distinct date(CAST(o.create_datetime AS timestamp)) domonth,
		count(distinct case when o.is_bi_excluded = 0 then o.order_id else null end) orders,
		sum(case when o.is_bi_excluded = 0 then o.gmv_usd else 0 end) gmv,
		count(distinct o.shop_id) sellers,
		count(distinct o.buyer_id) buyers,
		-- logistics cost
		SUM(CASE WHEN d.is_pick_up = 1 THEN COALESCE(d.scheduled_net_cpo,0) ELSE 0 END) AS logistics_cost
    FROM order_mart_dwd_order_all_event_final_status_df o
    LEFT JOIN data d
    	ON o.order_id = d.order_id
    WHERE date(CAST(create_datetime AS timestamp)) BETWEEN DATE'2015-01-01' AND current_date - INTERVAL '1' DAY
    GROUP BY 1,2
	)

, cost_uni AS 
	(
	SELECT distinct

		date(CAST(create_datetime AS timestamp)) grass_date,
		order_id,
		pv_voucher_code,
		pv_rebate_by_shopee_amt_usd,
		sv_rebate_by_shopee_amt_usd,
		pv_coin_earn_by_shopee_amt_usd,
		sv_coin_earn_by_shopee_amt_usd,
		item_rebate_by_shopee_amt_usd
    FROM order_mart_dwd_order_item_all_event_final_status_df 
    WHERE date(CAST(create_datetime AS timestamp)) BETWEEN DATE'2015-01-01' AND current_date - INTERVAL '1' DAY
    	AND is_net_order = 1
    -- GROUP BY 1
	)

, cost AS 
	(
	SELECT 
		grass_date,
		-- voucher direct
		SUM(CASE WHEN regexp_like(pv_voucher_code,'^(SPGC).*') THEN COALESCE(sv_rebate_by_shopee_amt_usd,0) 
		    ELSE COALESCE(pv_rebate_by_shopee_amt_usd,0)+COALESCE(sv_rebate_by_shopee_amt_usd,0) END) as voucher_cost,	
		-- Voucher Coin Cost
		SUM(CASE WHEN regexp_like(pv_voucher_code,'^(SPGC).*') THEN COALESCE(sv_coin_earn_by_shopee_amt_usd,0) 
		    ELSE COALESCE(pv_coin_earn_by_shopee_amt_usd,0)+COALESCE(sv_coin_earn_by_shopee_amt_usd,0) END) as coin_cost,
		-- rebate 
		SUM(coalesce(item_rebate_by_shopee_amt_usd,0)) rebate_cost
    FROM cost_uni 
    -- WHERE date(CAST(create_datetime AS timestamp)) BETWEEN DATE'2015-01-01' AND current_date - INTERVAL '1' DAY
    	-- AND is_net_order = 1
    GROUP BY 1
	)

, rev_cost AS 
	(
	SELECT 
	    YEAR(c.grass_date) year,
		month(c.grass_date) month,
		count(distinct date(CAST(c.create_datetime AS timestamp)) domonth,
		SUM(COALESCE(c.voucher_cost,0)) voucher_cost,
		SUM(COALESCE(c.coin_cost,0)) coin_cost,
		SUM(COALESCE(c.rebate_cost,0)) rebate_cost,
		SUM(COALESCE(r.service_fee,0)) service_fee,
		SUM(COALESCE(r.handling_fee,0)) handling_fee,
		SUM(COALESCE(r.commission_fee,0)) commission_fee,
		SUM(COALESCE(p.paid_ads_rev,0)) paid_ads_rev
	FROM cost c
	LEFT JOIN revenue r
		ON c.grass_date = r.grass_date
	LEFT JOIN paid_ads p 
		ON c.grass_date = p.grass_date
	GROUP BY 1,2
	)

, detail AS 
	(
	SELECT 
		s.year,
		s.month,
		s.eomonth,
		s.orders,
		CAST(s.gmv AS DOUBLE) gmv,
		s.sellers,
		s.buyers,
		CAST(r.voucher_cost AS DOUBLE) voucher_cost,
		CAST(r.coin_cost AS DOUBLE) coin_cost,
		CAST(r.rebate_cost AS DOUBLE) rebate_cost,
		s.logistics_cost,
		CAST(r.service_fee AS DOUBLE) service_fee,
		CAST(r.handling_fee AS DOUBLE) handling_fee,
		CAST(r.commission_fee AS DOUBLE) commission_fee,
		r.paid_ads_rev 
	FROM sales s
	LEFT JOIN rev_cost r
		ON s.year = r.year AND s.month = r.month 
	)

------- FINAL
SELECT
    *
FROM detail