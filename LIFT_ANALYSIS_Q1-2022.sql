-- QBR lift analysis
-- Katiana Khouri 3/2/2022

-- 1. Gather ER and Control Comm data
    -- a. On remote run ER camp hist for current quarter with adhoc dates
    -- b. on remote run  PULL_CONTROL_TRIGGERED_ENTP after updating code dates
    -- c. verify finished, email will arrive for control comm and sql dates check for er

-- 2. Replace all old names/dates with new for this quarter
DEFINE qtr_yr = 'Q122';
DEFINE Curr_SEG =  '';
DEFINE Curr_SEG1 =  'SMASTER_202201';-- define seg master as first month in the quarter being analyzed, referred to as &Curr_SEG1
DEFINE Curr_SEG2 =  'SMASTER202202';
DEFINE Curr_SEG3 =  'SMASTER202203';
--DEFINE Curr_SEG =  'SMASTER202201'; -- referred to as &Curr_SEG
define CurrER = 'TMP_Q122_ER';  -- referred to as &CurrER
Define Curr_UCG = 'HMA_UCG_FINAL_BK012022';--MOST CURRENT BACKUP - CHECK FILE NAMES IN DW WORLD
Define Curr_UCG_1 = 'HMA_UCG_FINAL_BK102021'; -- SECOND MOST CURRENT BACKUP - CHECK FILE NAMES IN DW WORLD
define curr_cntrl_file = 'QBR_CONTROL_COMM19'; -- referred to as &curr_cntrl_file
define Curr_cntrl = 'TMP_QBR_Q122_CONTROL_COMM'; -- referred to as &Curr_cntrl
define begin_qtr_date = '01-JAN-22'; -- referred to as '&&begin_qtr_date'
define end_qtr_date = '31-MAR-22'; --referred to as '&&end_qtr_date'
define end_first_month = '31-JAN-22' ;-- reffered to as '&&end_first_month'
define end_response_date = '31-MAY-22';-- TWO MONTH AFTER END OF THE QUARTER
define prev_ER = 'TMP_Q421_ER'; --referred to as '&&prev_ER'
define curr_ro = 'QBR_SRV_RAW_Q122';
define curr_ro_index = 'TMP_IDX_SRV_RAW_Q122';
DEFINE YEAR1 = '2022';  -- year of the quarter, format of '# of the Year'
DEFINE MONTH_1 = '1'; -- first month of the quarter, format of '# of the month'
DEFINE MONTH_2 = '2'; -- second month of the quarter
DEFINE MONTH_3 = '3'; -- third month of the quarter

-- verify ER and Control tables are populated:
select count(*) as control_count from &curr_cntrl_file ;
select MIN(SOLICIT_DATE), MAX(SOLICIT_DATE) from ER_CAMP_HIST_EXT;

-- Create universal seg master table
drop table &Curr_SEG;
CREATE TABLE &Curr_SEG
AS
SELECT DISTINCT VIN, SEGMENT , model_year FROM &Curr_SEG1
UNION
SELECT DISTINCT VIN, SEGMENT , model_year FROM &Curr_SEG2
WHERE VIN NOT IN (SELECT VIN FROM &Curr_SEG1)
UNION
SELECT DISTINCT VIN, SEGMENT , model_year FROM &Curr_SEG3
WHERE VIN NOT IN (SELECT VIN FROM &Curr_SEG1)
AND VIN NOT IN (SELECT VIN FROM &Curr_SEG2);

/*
CREATE TABLE &Curr_SEG
AS
SELECT DISTINCT CASE WHEN A.SEGMENT IS NOT NULL THEN A.SEGMENT
WHEN A.SEGMENT IS NULL AND B.SEGMENT IS NOT NULL THEN B.SEGMENT
WHEN A.SEGMENT IS NULL AND B.SEGMENT IS NULL THEN C.SEGMENT
ELSE NULL
END AS SEGMENT,
CASE WHEN A.VIN IS NOT NULL THEN A.VIN
WHEN A.VIN IS NULL AND B.VIN IS NOT NULL THEN B.VIN
WHEN A.VIN IS NULL AND B.VIN IS NULL THEN C.VIN
ELSE NULL
END AS VIN
FROM &Curr_SEG1 A
FULL OUTER JOIN &Curr_SEG2 B
ON A.VIN =B.VIN
FULL OUTER JOIN &Curr_SEG3 C
ON A.VIN = C.VIN;
*/

-- 3. Create ER table and check date range is solicit dates in quarter only
/*CREATE TABLE &CurrER AS
SELECT * FROM ER_CAMP_HIST_EXT;*/

DROP TABLE "&CurrER";
CREATE TABLE "&CurrER" AS
SELECT E.*,
  S.SEGMENT
FROM ER_CAMP_HIST_EXT E
LEFT JOIN "&Curr_SEG" S ON E.VIN = S.VIN;

-- 4. Check variables working
select MIN(SOLICIT_DATE), MAX(SOLICIT_DATE) from &CurrER;
select * from &Curr_UCG order by UPDATE_DT DESC;

-- 5. CREATING WORKABLE UCG TABLE (NEW RULES) and CONTROL TABLE
drop table TMP_UCG_FINAL;
CREATE TABLE TMP_UCG_FINAL AS
SELECT *
FROM &Curr_UCG
WHERE UPDATE_DT < '&&begin_qtr_date'; commit;  select count (*) from TMP_UCG_FINAL;

INSERT INTO TMP_UCG_FINAL
SELECT *
FROM &Curr_UCG_1
WHERE VIN NOT IN (SELECT VIN FROM TMP_UCG_FINAL); COMMIT;

--DROP TABLE "&Curr_cntrl";
create table "&Curr_cntrl" AS
SELECT Q.*,
  S.SEGMENT
FROM &curr_cntrl_file Q
JOIN TMP_UCG_FINAL U ON Q.VIN = U.VIN --AND Q.ACTION_COMP_DATE < U.STATUS_DT
LEFT JOIN &Curr_SEG S ON U.VIN = S.VIN;



-- 6. Gives us the Control triggered communications from the campaigns we trigger (LB, WB, CQ)
-- The join helps us identify which dealers were sending out those communication given that they were most likely OM enrolled
INSERT INTO &Curr_cntrl
SELECT RUN_ID AS OBJID, NULL AS PART_DEALERID, H.DEALER AS OEM_DEALERID, OFFER_COM AS ACT_GROUP, CHANNEL AS ACT_TYPE, NULL AS ACT_SUBTYPE, RUN_DATE AS ACTION_COMP_DATE, NULL AS BCV, H.VIN, NULL AS OEM_CUSTOMER_ID,
  S.SEGMENT
FROM HMA_ANALYTICS_CAMPAIGN_HIST H
JOIN (select DISTINCT FULFILLED_BY AS DEALER from &CurrER WHERE LINE_ITEM in ('Winback','Overdue Maintenance') ) x on h.DEALER = x.dealer
LEFT JOIN &Curr_SEG S ON H.VIN = S.VIN
WHERE RUN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
AND CONTROL = 'Y'
;COMMIT;

--small clean-up
UPDATE &Curr_cntrl
SET ACT_GROUP = RTRIM(ACT_GROUP)
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
;COMMIT;

-- check to see control table updated appropriately - should not be empty
SELECT COUNT(*) as cntrl_record_count
FROM &Curr_cntrl;

--Take out Cadence+ADhoc(exclude OnDemand) contaminated VINs
DELETE FROM &Curr_cntrl
WHERE VIN IN (SELECT DISTINCT Q.VIN FROM &Curr_cntrl Q
JOIN &CurrER E ON Q.VIN = E.VIN  where E.line_item is not null and E.line_item not in ('On Demand')
) OR VIN IN (SELECT DISTINCT A.VIN FROM &Curr_cntrl A
JOIN &Prev_ER B ON A.VIN = B.VIN  where B.line_item is not null and B.line_item not in ('On Demand')
);COMMIT;

-- Take out VDPS contaminated VINs
DELETE FROM &Curr_cntrl
WHERE VIN IN (SELECT VIN
              FROM HYUNDAI.HYU_TP_VDPS_CAMP_COMM_DTL
              WHERE TO_DATE(BATCH_PROCESSING_DATE, 'DD-MON-YY') BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date' and campaign_name not in ('ON_DEMAND','OnDemand - DM') );
commit;

select count(*) from &Curr_cntrl;


--Select statements to check for any more contaminated VINs
SELECT COUNT(DISTINCT Q.VIN)
FROM &Curr_cntrl Q
JOIN &CurrER E ON Q.VIN = E.VIN where
E.line_item is not null and
E.line_item not in ('On Demand')
;
select distinct line_item from &CurrER;
select distinct campaign_name from &CurrER
where line_item is null;
--final counts from cadence... Need to add AD-HOC next
SELECT ACT_GROUP, COUNT(*), COUNT(DISTINCT VIN) FROM &Curr_cntrl GROUP BY ACT_GROUP;

--7. CREATING A GENERAL SERVICE TABLE TO USE
DROP TABLE "&curr_ro";
CREATE TABLE "&curr_ro"    AS
SELECT ROW_NUMBER() OVER( ORDER BY RO_OPEN_DATE) AS RO_IDENT,
        H.*
FROM HYUNDAI.HYU_CORE_RO_FULL_HEADER H
WHERE RO_OPEN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_response_date' -- adding an extra 2 months past the quarter end for the 60-day response window
;

--drop index "&curr_ro_index";
CREATE INDEX "&curr_ro_index"  ON &curr_ro(VIN, RO_ID);


-- 8. update campagin date for LB and WB
-- the oracle version....
UPDATE &CURR_CNTRL TEST
SET TEST.action_comp_date =
        (SELECT MIN(RUN_DATE) from hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_1
         AND EXTRACT(YEAR FROM run_date) = &YEAR1)
         WHERE RTRIM(TEST.ACT_GROUP) IN ('LB','WB') AND TEST.OBJID IN
         (SELECT DISTINCT RUN_ID
         FROM hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_1
         AND EXTRACT(YEAR FROM run_date) = &YEAR1
         );

UPDATE &CURR_CNTRL TEST
SET TEST.action_comp_date =
        (SELECT MIN(RUN_DATE) from hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_2
         AND EXTRACT(YEAR FROM run_date) = &YEAR1)
         WHERE RTRIM(TEST.ACT_GROUP) IN ('LB','WB') AND TEST.OBJID IN
         (SELECT DISTINCT RUN_ID
         FROM hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_2
         AND EXTRACT(YEAR FROM run_date) = &YEAR1
         );

UPDATE &CURR_CNTRL TEST
SET TEST.action_comp_date =
        (SELECT MIN(RUN_DATE) from hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_3
         AND EXTRACT(YEAR FROM run_date) = &YEAR1)
         WHERE RTRIM(TEST.ACT_GROUP) IN ('LB','WB') AND TEST.OBJID IN
         (SELECT DISTINCT RUN_ID
         FROM hma_analytics_campaign_hist
         WHERE OFFER_COM IN ('LB','WB')
         AND EXTRACT(MONTH FROM run_date) = &MONTH_3
         AND EXTRACT(YEAR FROM run_date) = &YEAR1
         );COMMIT;


-- 9. Excel work

----- Tab 2, om oc, with wp
----- CC_LB_WB  TAB 2 --NO 'Thank You for Service' ----
----- treated

SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, C.OM_STATUS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.FULFILLED_BY) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT + warranty_total_amount) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated', C.OM_STATUS;

------CONTROL

SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS, C.OM_STATUS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &Curr_cntrl EE LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.oem_dealerid) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
  AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT + warranty_total_amount) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr' , C.SEGMENTS, 'cntrl', C.OM_STATUS;

---TREATED DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,  t.ENROLLMENT, COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT + r.warranty_total_amount) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
WHERE RO_ID IN (SELECT  DISTINCT RO_ID
                FROM &CurrER C
                LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT + warranty_total_amount) > 0)
                WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')   OR  (ACT_GROUP IN ('ADHOC_VEHICLE') and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand')

group by '&qtr_yr','treated', t.ENROLLMENT
UNION ALL

----CONTROL DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'cntrl' AS CAMPS, t.ENROLLMENT,COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT + r.warranty_total_amount) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
WHERE RO_ID IN (
SELECT DISTINCT RO_ID
FROM &Curr_cntrl C
JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
 AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT + warranty_total_amount) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY'))
group by '&qtr_yr', 'cntrl', t.ENROLLMENT;

----- Tab 2, om oc, without wp
----- CC_LB_WB  TAB 2 --NO 'Thank You for Service' ----
----- treated

SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, C.OM_STATUS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.FULFILLED_BY) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated', C.OM_STATUS;

------CONTROL

SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS, C.OM_STATUS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &Curr_cntrl EE LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.oem_dealerid) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
  AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr' , C.SEGMENTS, 'cntrl', C.OM_STATUS;

---TREATED DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,  t.ENROLLMENT, COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
WHERE RO_ID IN (SELECT  DISTINCT RO_ID
                FROM &CurrER C
                LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
                WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')   OR  (ACT_GROUP IN ('ADHOC_VEHICLE') and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand')

group by '&qtr_yr','treated', t.ENROLLMENT
UNION ALL

----CONTROL DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'cntrl' AS CAMPS, t.ENROLLMENT,COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
WHERE RO_ID IN (
SELECT DISTINCT RO_ID
FROM &Curr_cntrl C
JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
 AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY'))
group by '&qtr_yr', 'cntrl', t.ENROLLMENT;

-- without enrollment breakout, without wp
----------------------------------------------------------------
-----------------------------------------
-------------------------------------------
----- Tab 2, om oc, without wp
----- CC_LB_WB  TAB 2 --NO 'Thank You for Service' ----
----- treated

SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        ) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated';

------CONTROL

SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS FROM &Curr_cntrl EE LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        ) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
  AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr' , C.SEGMENTS, 'cntrl';

---TREATED DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'treated' AS CAMPS, COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r
WHERE RO_ID IN (SELECT  DISTINCT RO_ID
                FROM &CurrER C
                LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
                WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')   OR  (ACT_GROUP IN ('ADHOC_VEHICLE') and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand')

group by '&qtr_yr','treated'
UNION ALL

----CONTROL DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'cntrl' AS CAMPS,COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT
FROM &curr_ro r

WHERE RO_ID IN (
SELECT DISTINCT RO_ID
FROM &Curr_cntrl C
JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
 AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY'))
group by '&qtr_yr', 'cntrl';

----------------- Model year version --------------------------------------------------------------------------

----- Tab 2, om oc, without wp
----- CC_LB_WB  TAB 2 --NO 'Thank You for Service' ----

----- treated
SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, C.OM_STATUS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN , c.model_year
FROM (  SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS ,  ss.model_year
        FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.FULFILLED_BY) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated', C.OM_STATUS, c.model_year;

------CONTROL

SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS, C.OM_STATUS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, c.model_year
FROM (  SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS ,  ss.model_year
        FROM &Curr_cntrl EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        LEFT JOIN dlr_enrollment t on t.dealer = EE.oem_dealerid) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
                      AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB'))
OR (act_group in ('AFTER_SERVICE') AND  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr' , C.SEGMENTS, 'cntrl', C.OM_STATUS, c.model_year;

---TREATED DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,  t.ENROLLMENT, COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT , t.model_year
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
left join &Curr_SEG t on t.vin = r.vin
WHERE RO_ID IN (SELECT  DISTINCT RO_ID
                FROM &CurrER C
                LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
                WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')   OR  (ACT_GROUP IN ('ADHOC_VEHICLE') and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand')

group by '&qtr_yr','treated', t.ENROLLMENT, t.model_year
UNION ALL

----CONTROL DOLLARS
SELECT '&qtr_yr' AS QUARTER, 'cntrl' AS CAMPS, t.ENROLLMENT,COUNT(DISTINCT r.RO_ID) AS TOTAL_RO, SUM(r.CUSTOMER_PAY_TOTAL_AMOUNT) AS CUST_PAY, SUM(r.RO_TOTAL_AMOUNT)  RO_AMOUNT , t.model_year
FROM &curr_ro r
left join dlr_enrollment t on t.dealer = r.dealer_code
left join &Curr_SEG t on t.vin = r.vin
WHERE RO_ID IN (    SELECT DISTINCT RO_ID
                    FROM &Curr_cntrl C
                    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
                    WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY'))
group by '&qtr_yr', 'cntrl', t.ENROLLMENT, t.model_year;




----- Tab 2, om oc, without wp, with CAMP
----- CC_LB_WB  TAB 2 --NO 'Thank You for Service' ----
----- treated

SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, C.OM_STATUS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN , act_group
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.FULFILLED_BY) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated', C.OM_STATUS, act_group;

------CONTROL

SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS, C.OM_STATUS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, act_group
FROM (SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS FROM &Curr_cntrl EE LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.oem_dealerid) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
  AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB')) or (act_group in ('AFTER_SERVICE') and  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr', C.SEGMENTS, 'cntrl', C.OM_STATUS, act_group;

----------------------------- Adding MY -------------------------------------------------------------------

----- treated
SELECT  '&qtr_yr' AS QUARTER, 'treated' AS CAMPS,C.SEGMENTS, C.OM_STATUS, COUNT( DISTINCT C.VIN) CONTACTED, COUNT( DISTINCT S.VIN ) CAME_IN , c.model_year, act_group
FROM (  SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS ,  ss.model_year
        FROM &CurrER EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        left join dlr_enrollment t on t.dealer = EE.FULFILLED_BY) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN
                     AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
                     AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)

WHERE (ACT_GROUP IN ('AFTER_SERVICE','MAINT') and LINE_ITEM <>'Thank You for Service')
OR  (ACT_GROUP IN ('ADHOC_VEHICLE')
and  LINE_ITEM in ('Loyalty Booster','Winback'))
AND LINE_ITEM <> 'On Demand'
and LINE_ITEM <>'Thank You for Service'
GROUP BY C.SEGMENTS, '&qtr_yr', 'treated', C.OM_STATUS, c.model_year, act_group;

------CONTROL
SELECT '&qtr_yr' AS QUARTER,'cntrl'  AS CAMPS,C.SEGMENTS, C.OM_STATUS,COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, c.model_year, act_group
FROM (  SELECT EE.*,SS.SEGMENT SEGMENTS, T.ENROLLMENT AS OM_STATUS ,  ss.model_year
        FROM &Curr_cntrl EE
        LEFT JOIN &Curr_SEG SS ON EE.VIN = SS.VIN
        LEFT JOIN dlr_enrollment t on t.dealer = EE.oem_dealerid) C

LEFT JOIN  &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
                      AND  ( (CUSTOMER_PAY_TOTAL_AMOUNT) > 0)
WHERE (ACT_GROUP IN ('MAINT','LB','WB'))
OR (act_group in ('AFTER_SERVICE') AND  ACT_SUBTYPE <> 'AS_TY')
GROUP BY  '&qtr_yr' , C.SEGMENTS, 'cntrl', C.OM_STATUS, c.model_year, act_group;







































----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
-------------ARCHIVED--------------------------------------------------------------------------------
--Q221 AND PRIOR EXCEL TABLE
--First Excel Table: Check to make sure all campaigns dropped on time and within the same month
select RUN_ID, TO_DATE(RUN_DATE, 'DD-MON-YY'), RTRIM(ANA_OFFER_COM), COUNT(*), COUNT(DISTINCT VIN)
FROM HMA_ANALYTICS_CAMPAIGN_HIST
WHERE RUN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
GROUP BY RUN_ID, TO_DATE(RUN_DATE, 'DD-MON-YY'), RTRIM(ANA_OFFER_COM)
ORDER BY 2, 1
;

--Second Excel working table: look at the ER volumes
SELECT SOLICIT_DATE, FULLFULL_DATE, LINE_ITEM, CHANNEL, COUNT(*), COUNT(DISTINCT E.VIN), COUNT(DISTINCT A.VIN)
FROM &CurrER E
LEFT JOIN  (
    SELECT VIN
    FROM HMA_ANALYTICS_CAMPAIGN_HIST
    WHERE RUN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_first_month'
) A ON E.VIN = A.VIN
WHERE ACT_GROUP = 'ADHOC_VEHICLE' AND LINE_ITEM NOT IN 'On Demand'
GROUP BY SOLICIT_DATE, FULLFULL_DATE, LINE_ITEM, CHANNEL
ORDER BY 3,1;
/* update solicit date below before running
select CHANNEL, COUNT(*)
from &CurrER
where solicit_date = '20190119' and line_item = 'Winback'
GROUP BY CHANNEL
;
*/

--- Third working table for excel sheet
select distinct a.RUN_ID,
    e.channel, e.solicit_date, e.line_item
FROM  &CurrER E
LEFT JOIN  (
    SELECT VIN, run_id, substr(RTRIM(offer_com),1,1) as camp,
    extract(month from TO_DATE(RUN_DATE, 'DD-MON-YY')) as mnth
    FROM HMA_ANALYTICS_CAMPAIGN_HIST
    WHERE RUN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
) A ON E.VIN = A.VIN
    and a.mnth = extract( month from to_date(e.solicit_date, 'YYYYMMDD'))
    and a.camp = substr(e.line_item,1,1)
WHERE ACT_GROUP = 'ADHOC_VEHICLE' AND LINE_ITEM NOT IN 'On Demand'
and line_item not in ('New to Area')
ORDER BY run_id, line_item
;

SELECT * FROM &Curr_cntrl;
SELECT COUNT(*) FROM &Curr_cntrl;
SELECT COUNT(*) FROM TMP_QBR_Q221_CONTROL_COMM;



--------
------------


----
--select * from &curr_ro
--SELECT MIN(RO_OPEN_DATE),MAX(RO_OPEN_DATE) FROM &curr_ro
--Response on Control ----------------------------------------------------------------
-- FIRST QUERY UPODATES 2 OF 5 CONTROL NUMBERS, SECOND QUERY UPDATES 3/5 CONTROL NUMBERS
-- RUN THRREE TIMES WITH APPROPRIATE CAMPAIGNS

SELECT COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, 'FIRST FOUR'  AS CAMPS--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS'
--, 'MONTHLY' -- UNCOMMENT MONTHLY FOR SECOND BOX
)
UNION ALL

SELECT COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, 'ALL FIVE'  AS CAMPS--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('LB','AFTER_SERVICE',         'WB','MAINT','THANKS'
, 'MONTHLY' -- UNCOMMENT MONTHLY FOR SECOND BOX
)

UNION ALL

SELECT COUNT( DISTINCT C.VIN) AS WOULD_BE_CONTACTED, COUNT( DISTINCT S.VIN ) AS CAME_IN_FOR_SERV, 'MONTHLY'  AS CAMPS--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ( 'MONTHLY' )
;



--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS (UPDATE IN EXCEL LIFT TAB)
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'FIRST FOUR' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
SELECT DISTINCT RO_ID
FROM &Curr_cntrl C
JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('LB','AFTER_SERVICE', 'WB','MAINT','THANKS'))

UNION ALL
 SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'ALL FIVE' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
SELECT DISTINCT RO_ID
FROM &Curr_cntrl C
JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('LB','AFTER_SERVICE', 'WB','MAINT','THANKS', 'MONTHLY'))

UNION ALL
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'MONTHLY' AS CAMPS
        FROM &curr_ro WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('MONTHLY'))
;

--Response on Treated -------------------------------

select ACT_GROUP, COUNT(DISTINCT VIN) from &CurrER GROUP BY ACT_GROUP;
-- FIRST TABLE FOR TREATED GROUP FOR 2/5 NUMBERS
SELECT COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ), 'FIRST FOUR' AS CAMPS
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('ADHOC_VEHICLE','AFTER_SERVICE', 'MAINT','THANKS')
AND LINE_ITEM <> 'On Demand'

UNION ALL

SELECT COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ), 'ALL FIVE' AS CAMPS
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('ADHOC_VEHICLE','AFTER_SERVICE', 'MAINT','THANKS', 'MONTHLY')
AND LINE_ITEM <> 'On Demand'

UNION ALL

SELECT COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ), 'MONTHLY' AS CAMPS
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('MONTHLY')
AND LINE_ITEM <> 'On Demand'
;

select min(solicit_date), max(solicit_date)
from &CurrER
;

----- SECOND TABLE FOR TREATED NUMBERS 3/5 NUMBERS BELOW
--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'FIRST FOUR' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('ADHOC_VEHICLE','AFTER_SERVICE', 'MAINT','THANKS')
AND LINE_ITEM <> 'On Demand'
)

UNION ALL

SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'ALL FIVE' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('ADHOC_VEHICLE','AFTER_SERVICE', 'MAINT','THANKS', 'MONTHLY')
AND LINE_ITEM <> 'On Demand'
)

UNION ALL

SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'FIRST FOUR' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN ('MONTHLY')
AND LINE_ITEM <> 'On Demand'
)
;
---- END LIFT SUMMARY EXCEL TAB

-- Split by campaign ---------------------------------------------------------------------------------------------

--Response on Control

SELECT ACT_GROUP, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM &Curr_cntrl C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY ACT_GROUP
;


--Response on Treated

select ACT_GROUP, COUNT(DISTINCT VIN) from &CurrER GROUP BY ACT_GROUP;

SELECT CASE WHEN ACT_GROUP = 'ADHOC_VEHICLE'  THEN C.LINE_ITEM
      ELSE ACT_GROUP
      END AS ACT_GROUP,
COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
AND LINE_ITEM <> 'New to Area'
GROUP BY CASE WHEN ACT_GROUP = 'ADHOC_VEHICLE'  THEN C.LINE_ITEM
      ELSE ACT_GROUP
      END
;
-- AGGREGATE RO FOR TREATED
-- NEED TO REVAMP THIS POSSIBLY A UNION WITH EACH CAMP DONE SEPARATELY (AND ADHOC VEHICLE FOR LB AND WB)
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'Loyalty Booster' AS CAMPS
FROM &curr_ro
WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('ADHOC_VEHICLE')
        AND LINE_ITEM <> 'On Demand'
        and line_item = 'Loyalty Booster'
        )
union all
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'Winback' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('ADHOC_VEHICLE')
        AND LINE_ITEM <> 'On Demand'
        and line_item = 'Winback'
)

union all
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'AFTER_SERVICE' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('AFTER_SERVICE')
        AND LINE_ITEM <> 'On Demand'
        )

union all
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'maint' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('MAINT')
        AND LINE_ITEM <> 'On Demand'
        )

union all
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'thanks' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('THANKS')
        AND LINE_ITEM <> 'On Demand'
        )
union all
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'monthly' AS CAMPS
FROM &curr_ro WHERE RO_ID IN (
        SELECT  DISTINCT RO_ID
        FROM &CurrER C
        LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('MONTHLY')
        AND LINE_ITEM <> 'On Demand'
        )

;

-- Aggregate for control
--'LB','AFTER_SERVICE','WB','MAINT','THANKS','MONTHLY'

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS (UPDATE IN EXCEL LIFT TAB)
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'LB' as camp
FROM &curr_ro
WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('LB'))

union all

SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'AFTER_SERVICE' as camp
FROM &curr_ro
WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('AFTER_SERVICE'))

  UNION ALL
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'MONTHLY' AS CAMPS
        FROM &curr_ro WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('MONTHLY'))


UNION ALL
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'THANKS' AS CAMPS
        FROM &curr_ro WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('THANKS'))


UNION ALL
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'MAINT' AS CAMPS
        FROM &curr_ro WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('MAINT'))


UNION ALL
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT) , 'WB' AS CAMPS
        FROM &curr_ro WHERE RO_ID IN (
        SELECT DISTINCT RO_ID
        FROM &Curr_cntrl C
        JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
        AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
        WHERE ACT_GROUP IN ('WB'))
      ;



----------
/*
--Need to track dealers too
--drop table TMP_Q22019_ER_2;
create table TMP_Q22019_ER_2 AS
SELECT E.SOLICIT_DATE, E.FULLFULL_DATE, E.VIN, E.CHANNEL, e.act_sub_type, e.act_group, e.line_item, e.campaign_id, e.campaign_name, c.fulfilled_by
from TMP_Q22019_ER E
JOIN ER_CAMP_HIST_EXT C ON E.SOLICIT_DATE = C.SOLICIT_DATE AND E.VIN = C.VIN AND E.CHANNEL = C.CHANNEL AND E.ACT_SUB_TYPE = C.ACT_SUB_TYPE
;
*/

9,067,895
select count(*) from &CurrER;




-------

--Look at it by Tier

select * from HMA_HX_TIER_CURR;
select * from &Curr_cntrl;

--Response on Control

SELECT T.Tier, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY T.Tier
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Tier, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM &Curr_cntrl C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS', 'MONTHLY'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Tier
ORDER BY 1
;


--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY T.Region
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM &Curr_cntrl C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS', 'MONTHLY'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Region
ORDER BY 1
;

select TIER, COUNT(*)
from HMA_HX_TIER_CURR WHERE enroll_status = 'Active'
group by TIER
order by 1
;

select REGION, COUNT(*)
from HMA_HX_TIER_CURR WHERE enroll_status = 'Active'
group by REGION
order by 1
;


=-=-=-=-=-=-=-=
select * from &CurrER;

SELECT T.TIER, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &CurrER C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY T.TIER
;

--Dealers
SELECT T.TIER, COUNT( DISTINCT T.DEALER_NAME)
FROM &CurrER C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY T.TIER
ORDER BY TIER ASC
;

select TIER, COUNT(*)
from hma_hx_tier_curr
WHERE to_date(live_date,'mm\dd\yy') < '&&live_date'
group by TIER
order by 1
;

select REGION, COUNT(*)
from hma_hx_tier_curr
WHERE to_date(live_date,'mm\dd\yy') < '&&live_date'
group by REGION
order by 1
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.TIER, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON S.DEALER_CODE = t.dealer_code
GROUP BY T.TIER
ORDER BY 1
;



SELECT T.Region, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &CurrER C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY T.Region
;

--Dealers
SELECT T.Region, COUNT( DISTINCT T.DEALER_NAME)
FROM &CurrER C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY T.Region
ORDER BY T.Region ASC
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON S.DEALER_CODE = t.dealer_code
GROUP BY T.Region
ORDER BY 1
;




---==-=-=-=-=-=-=-


--Response on Treated

select ACT_GROUP, COUNT(DISTINCT VIN) from '&&prev_ER' GROUP BY ACT_GROUP;

SELECT COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ), COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'
)
AND LINE_ITEM <> 'On Demand'
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM &CurrER C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'
)
AND LINE_ITEM <> 'On Demand'
)
;

------------------------
-- ACTIVE VS INACTIVE --
------------------------

--Response on Control

SELECT C.SEGMENT, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY C.SEGMENT
ORDER BY 1
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM &Curr_cntrl C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS', 'MONTHLY'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT FROM &Curr_cntrl) T ON T.VIN = Q.VIN
GROUP BY T.SEGMENT
ORDER BY 1
;

-- Adding Tier
SELECT C.SEGMENT, T.TIER, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY C.SEGMENT, T.TIER
ORDER BY 1, 2
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, C.TIER, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM &Curr_cntrl C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS', 'MONTHLY'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT FROM &Curr_cntrl) T ON T.VIN = Q.VIN
JOIN HMA_HX_TIER_CURR C ON C.dealer_code = Q.dealer_code
GROUP BY T.SEGMENT, C.TIER
ORDER BY 1, 2
;

-- Adding Region
SELECT C.SEGMENT, T.REGION, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &Curr_cntrl C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS', 'MONTHLY'
)
GROUP BY C.SEGMENT, T.REGION
ORDER BY 1, 2
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, C.REGION, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM &Curr_cntrl C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS', 'MONTHLY'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT FROM &Curr_cntrl) T ON T.VIN = Q.VIN
JOIN HMA_HX_TIER_CURR C ON C.dealer_code = Q.dealer_code
GROUP BY T.SEGMENT, C.REGION
ORDER BY 1, 2
;

--Response on Treated

SELECT C.SEGMENT, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM (SELECT E.*,
        CASE
          WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
          ELSE 'INACTIVE'
        END AS SEGMENT
      FROM &CurrER E
      LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY C.SEGMENT
ORDER BY 1
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT
      FROM (SELECT E.*,
              CASE
                WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
                ELSE 'INACTIVE'
              END AS SEGMENT
            FROM &CurrER E
            LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN)) T ON T.VIN = S.VIN
GROUP BY T.SEGMENT
ORDER BY 1
;

SELECT T.SEGMENT, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT FROM &CurrER) T ON T.VIN = S.VIN
GROUP BY T.SEGMENT
ORDER BY 1
;

-- Adding Tier
SELECT C.SEGMENT, T.TIER, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM (SELECT E.*,
        CASE
          WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
          ELSE 'INACTIVE'
        END AS SEGMENT
      FROM &CurrER E
      LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN) C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY C.SEGMENT, T.TIER
ORDER BY 1,2
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, C.TIER, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT
      FROM (SELECT E.*,
              CASE
                WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
                ELSE 'INACTIVE'
              END AS SEGMENT
            FROM &CurrER E
            LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN)) T ON T.VIN = S.VIN
JOIN HMA_HX_TIER_CURR C ON S.DEALER_CODE = C.dealer_code
GROUP BY T.SEGMENT, C.TIER
ORDER BY 1,2
;

-- Adding Region
SELECT C.SEGMENT, T.REGION, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM (SELECT E.*,
        CASE
          WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
          ELSE 'INACTIVE'
        END AS SEGMENT
      FROM &CurrER E
      LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN) C
JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY C.SEGMENT, T.REGION
ORDER BY 1,2
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.SEGMENT, C.REGION, COUNT(DISTINCT S.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro S
JOIN (
    SELECT  DISTINCT RO_ID
    FROM &CurrER C
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS', 'MONTHLY'
    )
    AND LINE_ITEM <> 'On Demand'
    ) R ON S.RO_ID = R.RO_ID
JOIN (SELECT DISTINCT VIN, SEGMENT
      FROM (SELECT E.*,
              CASE
                WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
                ELSE 'INACTIVE'
              END AS SEGMENT
            FROM &CurrER E
            LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN)) T ON T.VIN = S.VIN
JOIN HMA_HX_TIER_CURR C ON S.DEALER_CODE = C.dealer_code
GROUP BY T.SEGMENT, C.REGION
ORDER BY 1,2
;

-------------------------------
-- BROKEN OUT BY MODEL YEARS --
-------------------------------
SELECT DISTINCT MODEL_YEAR -- now includes 2020 vehicles, adjust CASE statements accordingly
FROM HYUNDAI.HYU_CUST_TRANS
ORDER BY 1 DESC;

SELECT E.*,
  CASE
    WHEN MODEL_YEAR BETWEEN 2020 AND 2021 THEN '2020 + 2021'
    WHEN MODEL_YEAR BETWEEN 2018 AND 2019 THEN '2018 + 2019'
    WHEN MODEL_YEAR BETWEEN 2016 AND 2017 THEN '2016 + 2017'
    WHEN MODEL_YEAR BETWEEN 2014 AND 2015 THEN '2014 + 2015'
    WHEN MODEL_YEAR <= 2013 THEN '2013-'
    END AS MODEL_GROUP
FROM &CurrER E
LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                FROM HYUNDAI.HYU_CUST_TRANS) S
ON E.VIN = S.VIN;

--Response on Treated

SELECT MODEL_GROUP, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM (SELECT E.*,
        CASE
          WHEN MODEL_YEAR BETWEEN 2018 AND 2021 THEN '2018 - 2021'
          WHEN MODEL_YEAR BETWEEN 2015 AND 2017 THEN '2015 - 2017'
          WHEN MODEL_YEAR <= 2014 THEN '2014-'
          END AS MODEL_GROUP
      FROM &CurrER E
      LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                  FROM HYUNDAI.HYU_CUST_TRANS) V
      ON E.VIN = V.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY MODEL_GROUP
;

SELECT MODEL_YEAR, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM (SELECT E.*, V.MODEL_YEAR
      FROM &CurrER E
      LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                  FROM HYUNDAI.HYU_CUST_TRANS) V
      ON E.VIN = V.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'
)
AND LINE_ITEM <> 'On Demand'
GROUP BY MODEL_YEAR
ORDER BY MODEL_YEAR ASC
;

-- PULLING RO AGGREGATED DATA
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM (SELECT E.*,
        CASE
          WHEN MODEL_YEAR BETWEEN 2018 AND 2020 THEN '2018 - 2020'
          WHEN MODEL_YEAR BETWEEN 2015 AND 2017 THEN '2015 - 2017'
          WHEN MODEL_YEAR <= 2014 THEN '2014-'
          END AS MODEL_GROUP
      FROM &CurrER E
      LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                  FROM HYUNDAI.HYU_CUST_TRANS) V
      ON E.VIN = V.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'
)
AND LINE_ITEM <> 'On Demand'
AND MODEL_GROUP = '2018 - 2020' -- change as needed
)
;

--Response on Control

SELECT C.MODEL_GROUP, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM (SELECT E.*,
        CASE
          WHEN MODEL_YEAR BETWEEN 2018 AND 2020 THEN '2018 - 2020'
          WHEN MODEL_YEAR BETWEEN 2015 AND 2017 THEN '2015 - 2017'
          WHEN MODEL_YEAR <= 2014 THEN '2014-'
          END AS MODEL_GROUP
      FROM &Curr_cntrl E
      LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                  FROM HYUNDAI.HYU_CUST_TRANS) V
      ON E.VIN = V.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS'
)
GROUP BY C.MODEL_GROUP
;

SELECT C.MODEL_YEAR, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )
FROM (SELECT E.*, V.MODEL_YEAR
      FROM &Curr_cntrl E
      LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                  FROM HYUNDAI.HYU_CUST_TRANS) V
      ON E.VIN = V.VIN) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS'
)
GROUP BY C.MODEL_YEAR
ORDER BY MODEL_YEAR ASC
;

-- PULLING RO AGGREGATED DATA
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro
WHERE RO_ID IN (
    SELECT DISTINCT RO_ID
    FROM (SELECT E.*,
            CASE
              WHEN MODEL_YEAR BETWEEN 2018 AND 2020 THEN '2018 - 2020'
              WHEN MODEL_YEAR BETWEEN 2015 AND 2017 THEN '2015 - 2017'
              WHEN MODEL_YEAR <= 2014 THEN '2014-'
              END AS MODEL_GROUP
          FROM &Curr_cntrl E
          LEFT JOIN ( SELECT DISTINCT VIN, MODEL_YEAR
                      FROM HYUNDAI.HYU_CUST_TRANS) V
          ON E.VIN = V.VIN) C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS'
    )
    AND MODEL_GROUP = '2014-' -- change as needed
  )
;



----
-----
------
---

select DISTINCT ACT_GROUP
FROM &Curr_cntrl C
;

select *
FROM &Curr_cntrl C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN HMA_ANALYTICS_CAMPAIGN_HIST H ON C.VIN = H.VIN AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-2012') OR (TRIM(H.ANA_OFFER_COM) LIKE '%-EA')) AND ACT_GROUP IN ('WB','LB')
    AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10
;


---WE NEED TO ISOLATE THE BOOSTED CAMPAIGNS AND PULL THE LIFT

/*
--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM TMP_QBR_Q118_CONTROL_COMM C
JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
LEFT JOIN (select * from HMA_ANALYTICS_CAMPAIGN_HIST WHERe RUN_DATE BETWEEN '01-JAN-19' AND '31-MAR-19') H
    ON C.VIN = H.VIN AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-2012') OR (TRIM(H.ANA_OFFER_COM) LIKE '%-EA')) AND ACT_GROUP IN ('WB','LB')
    AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE H.VIN IS NULL
AND ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS'
)
GROUP BY T.Region
;
*/

--TRY THIS INSTEAD
SELECT C.Region, COUNT( DISTINCT C_VIN), COUNT( DISTINCT S_VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM (
    SELECT T.Region, C.VIN AS C_VIN, S.VIN AS S_VIN, ACT_GROUP, ACTION_COMP_DATE
    FROM &Curr_cntrl C
    JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE  ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS'
    )
) C
LEFT JOIN (
        SELECT H.*, C.ACTION_COMP_DATE from HMA_ANALYTICS_CAMPAIGN_HIST H
        JOIN &Curr_cntrl C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10 AND ACT_GROUP IN ('LB')
        WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
        AND CONTROL = 'Y'
        AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
        ) H
    ON C.C_VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
WHERE H.VIN IS NULL
GROUP BY C.Region
ORDER BY 1
;



--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM (
        SELECT C.*
        FROM &Curr_cntrl C
        LEFT JOIN (
                SELECT H.*, C.ACTION_COMP_DATE from HMA_ANALYTICS_CAMPAIGN_HIST H
                JOIN &Curr_cntrl C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10 AND ACT_GROUP IN ('LB')
                WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
                AND CONTROL = 'Y'
                AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
                ) H
            ON C.VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
        WHERE H.VIN IS NULL
    ) C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Region
ORDER BY 1
;


---
--TREATED

SELECT C.Region, COUNT( DISTINCT C_VIN), COUNT( DISTINCT S_VIN )
FROM (
    SELECT T.Region, C.VIN AS C_VIN, S.VIN AS S_VIN, ACT_GROUP, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
    FROM &CurrER C
    JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS'
    )
    AND LINE_ITEM <> 'On Demand'
) C
LEFT JOIN (
        SELECT H.*, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
        from HMA_ANALYTICS_CAMPAIGN_HIST H
        JOIN &CurrER C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')-10 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')+10
            AND ACT_GROUP IN ('ADHOC_VEHICLE')
        WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
        AND CONTROL = 'N'
        AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
        ) H
    ON C.C_VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
WHERE H.VIN IS NULL
GROUP BY C.Region
ORDER BY 1
;


--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM (
        SELECT C.*
        FROM &CurrER C
        LEFT JOIN (
                SELECT H.*, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
                from HMA_ANALYTICS_CAMPAIGN_HIST H
                JOIN &CurrER C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')-10 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')+10
                    AND ACT_GROUP IN ('ADHOC_VEHICLE')
                WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
                AND CONTROL = 'N'
                AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
                ) H
            ON C.VIN = H.VIN AND H.ACTION_COMP_DATE = TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')
        WHERE H.VIN IS NULL
    ) C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
        'ADHOC_VEHICLE',
        'AFTER_SERVICE',
        'MAINT',
        'THANKS'
            )
    ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Region
ORDER BY 1
;

-------
----====================
--Looking at W/ Boosted Only now


--TRY THIS INSTEAD
SELECT C.Region, COUNT( DISTINCT C_VIN), COUNT( DISTINCT S_VIN ) --, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM (
    SELECT T.Region, C.VIN AS C_VIN, S.VIN AS S_VIN, ACT_GROUP, ACTION_COMP_DATE
    FROM &Curr_cntrl C
    JOIN HMA_HX_TIER_CURR T ON C.OEM_DEALERID = t.dealer_code
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE  ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS'
    )
) C
JOIN (
        SELECT H.*, C.ACTION_COMP_DATE from HMA_ANALYTICS_CAMPAIGN_HIST H
        JOIN &Curr_cntrl C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10 AND ACT_GROUP IN ('LB')
        WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
        AND CONTROL = 'Y'
        AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
        ) H
    ON C.C_VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
GROUP BY C.Region
ORDER BY 1
;



--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM (
        SELECT C.*
        FROM &Curr_cntrl C
        JOIN (
                SELECT H.*, C.ACTION_COMP_DATE from HMA_ANALYTICS_CAMPAIGN_HIST H
                JOIN &Curr_cntrl C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN C.ACTION_COMP_DATE-10 AND C.ACTION_COMP_DATE+10 AND ACT_GROUP IN ('LB')
                WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
                AND CONTROL = 'Y'
                AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
                ) H
            ON C.VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
    ) C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'LB',
    'AFTER_SERVICE',
    'WB',
    'MAINT',
    'THANKS'
    ) ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Region
ORDER BY 1
;


---
--TREATED

SELECT C.Region, COUNT( DISTINCT C_VIN), COUNT( DISTINCT S_VIN )
FROM (
    SELECT T.Region, C.VIN AS C_VIN, S.VIN AS S_VIN, ACT_GROUP, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
    FROM &CurrER C
    JOIN HMA_HX_TIER_CURR T ON C.FULFILLED_BY = t.dealer_code
    LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60 AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS'
    )
    AND LINE_ITEM <> 'On Demand'
) C
JOIN (
        SELECT H.*, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
        from HMA_ANALYTICS_CAMPAIGN_HIST H
        JOIN &CurrER C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')-10 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')+10
            AND ACT_GROUP IN ('ADHOC_VEHICLE')
        WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
        AND CONTROL = 'N'
        AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
        ) H
    ON C.C_VIN = H.VIN AND H.ACTION_COMP_DATE = C.ACTION_COMP_DATE
GROUP BY C.Region
ORDER BY 1
;


--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT T.Region, COUNT(DISTINCT Q.RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro Q
JOIN (
    SELECT DISTINCT RO_ID
    FROM (
        SELECT C.*
        FROM &CurrER C
        JOIN (
                SELECT H.*, TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') AS ACTION_COMP_DATE
                from HMA_ANALYTICS_CAMPAIGN_HIST H
                JOIN &CurrER C ON H.VIN = C.VIN AND H.RUN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')-10 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')+10
                    AND ACT_GROUP IN ('ADHOC_VEHICLE')
                WHERE RUN_DATE BETWEEN '&&begin_qtr_date' and '&&end_qtr_date'
                AND CONTROL = 'N'
                AND ((TRIM(H.ANA_OFFER_COM) LIKE '%-2012') OR (TRIM(H.ANA_OFFER_COM) LIKE '%-EA'))
                ) H
            ON C.VIN = H.VIN AND H.ACTION_COMP_DATE = TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD')
    ) C
    JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
    AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
    WHERE ACT_GROUP IN (
        'ADHOC_VEHICLE',
        'AFTER_SERVICE',
        'MAINT',
        'THANKS'
            )
    ) R ON Q.RO_ID = R.RO_ID
JOIN HMA_HX_TIER_CURR T ON T.dealer_code = Q.dealer_code
GROUP BY T.Region
ORDER BY 1
;

/*
LOOKING AT THE TREATED VINS WHO WERE ALSO EXPOSED TO THE 953 CAMPAIGNS
*/
-- VINs who received the 953 campaigns
SELECT VIN
FROM TMP_CAMP_953_VDPS;

-- How many of the treated VINs were also exposed to the 953 campaigns?
-- 489,499 (out of 3,880,456)
SELECT COUNT(DISTINCT VIN)
FROM &CurrER
WHERE VIN IN (SELECT VIN FROM TMP_CAMP_953_VDPS WHERE BATCH_FILE_GEN_ID IN (242108,260111,248403,259845,260110,271646,272119,278087));

-- Table with the 953 exposed VINs (treated)
DROP TABLE "&non_953";
CREATE TABLE "&non_953" AS
SELECT *
FROM &CurrER
WHERE VIN IN (SELECT VIN FROM TMP_CAMP_953_VDPS WHERE BATCH_FILE_GEN_ID IN (242108,260111,248403,259845,260110,271646,272119,278087));

-- Removing the 953 exposed VINs (treated)
DROP TABLE &CurrER_NO_953;
CREATE TABLE &CurrER_NO_953 AS
SELECT *
FROM &CurrER
WHERE VIN NOT IN (SELECT VIN FROM &non_953);

--Response on Treated
SELECT COUNT( DISTINCT C.VIN), COUNT( DISTINCT S.VIN )--, COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &CurrER_NO_953 C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS'--, 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
;

--PULLING RO AGGREGATED DATA, OTHER QUERY DOUBLE COUNTS
SELECT COUNT(DISTINCT RO_ID), SUM(CUSTOMER_PAY_TOTAL_AMOUNT), SUM(RO_TOTAL_AMOUNT)
FROM &curr_ro WHERE RO_ID IN (
SELECT  DISTINCT RO_ID
FROM &CurrER_NO_953 C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
/*'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS',*/ 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
)
;


/*
LOOKING AT THE MONTHLY VINS
*/
-- MONTHLY VINS
SELECT COUNT(DISTINCT VIN) -- 1,381,138 VINs received the Monthly campaign
FROM &CurrER
WHERE ACT_GROUP IN ('MONTHLY');

-- VINS WHO RECEIVED A MONTHLY CAMPAIGN ALONG WITH AT LEAST ONE OTHER CAMPAIGN AS WELL
SELECT VIN -- Of those who received the Monthly campaign, 972,337 VINs received at least one other campaign communication as well
FROM &CurrER
WHERE VIN IN (SELECT DISTINCT VIN
              FROM &CurrER
              WHERE ACT_GROUP IN ('MONTHLY'))
GROUP BY VIN
HAVING COUNT(DISTINCT ACT_GROUP) > 1;

-- Of those who received at least 2 different campaigns (that includes a Monthly campaign), 345,158 were responders
SELECT COUNT(DISTINCT C.VIN), COUNT(DISTINCT S.VIN )
FROM (SELECT *
      FROM &CurrER
      WHERE VIN IN (SELECT VIN
                    FROM &CurrER
                    WHERE VIN IN (SELECT DISTINCT VIN
                                  FROM &CurrER
                                  WHERE ACT_GROUP IN ('MONTHLY'))
                    GROUP BY VIN
                    HAVING COUNT(DISTINCT ACT_GROUP) > 1)) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
;

-- Of those who received only the Monthly campaign (408,801), 71,129 were responders
SELECT COUNT(DISTINCT C.VIN), COUNT(DISTINCT S.VIN )
FROM (SELECT *
      FROM &CurrER
      WHERE VIN IN (SELECT VIN
                    FROM &CurrER
                    WHERE VIN IN (SELECT DISTINCT VIN
                                  FROM &CurrER
                                  WHERE ACT_GROUP IN ('MONTHLY'))
                    GROUP BY VIN
                    HAVING COUNT(DISTINCT ACT_GROUP) = 1)) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 1 AND TO_DATE(C.SOLICIT_DATE, 'YYYYMMDD') + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'ADHOC_VEHICLE',
'AFTER_SERVICE',
'MAINT',
'THANKS', 'MONTHLY'
)
AND LINE_ITEM <> 'On Demand'
;

--Control
SELECT COUNT(DISTINCT VIN) -- 27,929 VINs would have received the Monthly campaign
FROM &Curr_cntrl
WHERE ACT_GROUP IN ('MONTHLY');

-- VINS WHO RECEIVED A MONTHLY CAMPAIGN ALONG WITH AT LEAST ONE OTHER CAMPAIGN AS WELL
SELECT VIN -- Of those who would have received the Monthly campaign, 14,586 VINs would have also received at least one other campaign communication as well
FROM &Curr_cntrl
WHERE VIN IN (SELECT DISTINCT VIN
              FROM &Curr_cntrl
              WHERE ACT_GROUP IN ('MONTHLY'))
GROUP BY VIN
HAVING COUNT(DISTINCT ACT_GROUP) > 1;

-- Of those who would have received at least 2 different campaigns (that includes a Monthly campaign), 4,290 were responders
SELECT COUNT(DISTINCT C.VIN), COUNT(DISTINCT S.VIN )
FROM (SELECT *
      FROM &Curr_cntrl
      WHERE VIN IN (SELECT VIN
                    FROM &Curr_cntrl
                    WHERE VIN IN (SELECT DISTINCT VIN
                                  FROM &Curr_cntrl
                                  WHERE ACT_GROUP IN ('MONTHLY'))
                    GROUP BY VIN
                    HAVING COUNT(DISTINCT ACT_GROUP) > 1)) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS',
'MONTHLY'
)
;

-- Of those who would have received only the Monthly campaign (13,343), 1,948 were responders
SELECT COUNT(DISTINCT C.VIN), COUNT(DISTINCT S.VIN )
FROM (SELECT *
      FROM &Curr_cntrl
      WHERE VIN IN (SELECT VIN
                    FROM &Curr_cntrl
                    WHERE VIN IN (SELECT DISTINCT VIN
                                  FROM &Curr_cntrl
                                  WHERE ACT_GROUP IN ('MONTHLY'))
                    GROUP BY VIN
                    HAVING COUNT(DISTINCT ACT_GROUP) = 1)) C
LEFT JOIN &curr_ro S ON C.VIN = S.VIN AND S.RO_OPEN_DATE BETWEEN C.ACTION_COMP_DATE + 1 AND C.ACTION_COMP_DATE + 60
AND S.CUSTOMER_PAY_FLAG = 'Y' AND CUSTOMER_PAY_TOTAL_AMOUNT > 0
WHERE ACT_GROUP IN (
'LB',
'AFTER_SERVICE',
'WB',
'MAINT',
'THANKS',
'MONTHLY'
)
;




---======
--Looking at control summary vs treated

select segment, count(*), count(distinct VIN)
from &Curr_cntrl
group by segment
;

select segment, count(*), count(distinct vin)
    FROM
    (SELECT     E.*, CASE
          WHEN S.SEGMENT IN ('NEW','ACTIVE') THEN 'ACTIVE'
          ELSE 'INACTIVE'
        END AS SEGMENT
      FROM &CurrER E
      LEFT JOIN &Curr_SEG S ON E.VIN = S.VIN
      ) C
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS'
    )
    group by segment
    ;

    (SELECT E.*,



select MODEL_YEAR, count(*), count(distinct C.VIN)
from &Curr_cntrl C
left join HYUNDAI.HYU_EPSILON_VEHICLE V on C.VIN = V.VIN
group by MODEL_YEAR
ORDER BY 1
;

select * from HYUNDAI.HYU_EPSILON_VEHICLE;

select model_year, count(*), count(distinct vin)
    FROM
    (SELECT     E.*, model_year
      FROM &CurrER E
        left join HYUNDAI.HYU_EPSILON_VEHICLE V on E.VIN = V.VIN
      ) C
    WHERE ACT_GROUP IN (
    'ADHOC_VEHICLE',
    'AFTER_SERVICE',
    'MAINT',
    'THANKS'
    )
    group by model_year
    order by 1
    ;

------------ ARCHIVE -----------------------------------------------------------


-- Begin function
select  *
from QBR_CONTROL_COMM17
;

select min(action_comp_date), max(action_comp_date)
from QBR_CONTROL_COMM17;

select min(action_comp_date), max(action_comp_date)
from &curr_cntrl_file;

select min(action_comp_date), max(action_comp_date)
from QBR_CONTROL_COMM15;

select *
from HMA_UCG_FINAL
;

--some checks
select *
from (
select Q.*
from &curr_cntrl_file Q
LEFT JOIN HMA_UCG_FINAL U ON Q.VIN = U.VIN
where U.VIN IS NULL
) X
LEFT JOIN HMA_UCG_FINAL_B U ON X.VIN = U.VIN
;

SELECT *
FROM HMA_UCG_FINAL Q
LEFT JOIN &curr_cntrl_file U ON Q.VIN = U.VIN
where U.VIN IS NULL
;


----------------

/* old code to use old vins as well as new ucg vins
--CREATING WORKABLE UCG TABLE
CREATE TABLE TMP_UCG_FINAL AS
SELECT U.*, 1 AS ACTIVE, sysdate as STATUS_DT
FROM HMA_UCG_FINAL U
;

INSERT INTO TMP_UCG_FINAL
SELECT U.*, 0 AS ACTIVE, '&&status_ucg_date' AS STATUS_DT
FROM &Curr_UCG U
WHERE VIN NOT IN (SELECT VIN FROM TMP_UCG_FINAL)
;
*/


-----COMP_MAINT INVESTIGATION-----

----- Complimentary maintenance adhoc code
---create table  TMP_COMP_MAINT_Q32021 AS
/*
select distinct VIN, c.COMP_MAINT
from vdps.job@dbl_vdps_eis j, vdps.contact@dbl_vdps_eis c, vdps.fulfillment_option_view@dbl_vdps_eis ff
where j.job_id = c.job_id
and j.FULFILLMENT_OPTION_ID = ff.FULFILLMENT_OPTION_ID
and j.FULFILLMENT_OPTION_ID in (79,80,93)
and job_status_id = 9
and j.created between '01-JUL-21' AND '30-SEP-21'
;

------2 QC
SELECT  * FROM TMP_COMP_MAINT_Q32021
SELECT COUNT(*) FROM TMP_COMP_MAINT_Q32021    -----2740216
SELECT COUNT(VIN) FROM TMP_COMP_MAINT_Q32021    -----2740216
SELECT COUNT(DISTINCT VIN) FROM TMP_COMP_MAINT_Q32021    -----2601269

SELECT COUNT(DISTINCT VIN) FROM TMP_COMP_MAINT_Q32021 where comp_maint='Y'---200,630
SELECT COUNT( VIN) FROM TMP_COMP_MAINT_Q32021 where comp_maint='Y'---200,630
*/
------SECOND VERSION TABLES



--CREATING WORKABLE UCG TABLE
--drop table TMP_UCG_FINAL;
--CREATE TABLE TMP_UCG_FINAL AS
--SELECT U.*, '&&status_ucg_date' AS STATUS_DT
--FROM &Curr_UCG U

--;
--select * from HMA_UCG_FINAL_BK072021
---select min(update_dt), max(update_dt) from HMA_UCG_FINAL_BK102021
--- select min(update_dt), max(update_dt) from HMA_UCG_FINAL_BK072021


/*Q22021
ACT_GROUP	COUNT(*)	COUNT(DISTINCTVIN)
ANNIVERSARY	1707	1180
AFTER_SERVICE	27539	11671
THANKS	766	339
STATE_INSPECTION	1773	1014
MONTHLY	74837	27463
ACCESSORIES	3862	697
MAINT	56734	13930

/*   q12021
ACT_GROUP	COUNT(*)	COUNT(DISTINCTVIN)
ANNIVERSARY	1326	956
THANKS	403	208
AFTER_SERVICE	21451	9601
STATE_INSPECTION	951	566
MONTHLY	61828	22215
ACCESSORIES	38	12
MAINT	48228	11583*/
/*  Q4 NUMBERS
ACT_GROUP	COUNT(*)	COUNT(DISTINCTVIN)
ANNIVERSARY	940	665
THANKS	458	225
AFTER_SERVICE	15782	7120
STATE_INSPECTION	453	263
MONTHLY	52950	19105
ACCESSORIES	6	2
MAINT	31905	7822
*/
/*
ACT_GROUP	COUNT(*)	COUNT(DISTINCTVIN)
ANNIVERSARY	1071	844
AFTER_SERVICE	20938	9184
STATE_INSPECTION	964	561
THANKS	637	298
MONTHLY	64872	23694
ACCESSORIES	12	1
MAINT	47866	11333
*/

/*
STATE_INSPECTION	1155	699
MAINT	41103	11178
AFTER_SERVICE	19440	8261
THANKS	363	275
ACCESSORIES	2	2
*/



--OnDemand contaminated VINs
SELECT campaign_name, count(*)
FROM &Curr_cntrl Q
JOIN &CurrER E ON Q.VIN = E.VIN  where E.line_item is not null and E.line_item not in ('On Demand')
group by campaign_name
;

SELECT COUNT(DISTINCT Q.VIN)
FROM &Curr_cntrl Q
JOIN &CurrER E ON Q.VIN = E.VIN where E.line_item is not null and E.line_item not in ('On Demand')
;

----------------------------------Retired Code-----
----------------------------------- Manual -----------------------------------------------------------------------------------
--Now, the fun part, manually update the drop dates based off data in ER
--Since we are estimating, we can just grab the first EM/DM drop for that campaign
-- in the future, automate this!
-- update for each month the min date and the appropriate runids
UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '06-OCT-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100052,
123294,
123295,
123296,
123297
);COMMIT;

UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '08-NOV-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100053,
124294,
124295,
124296,
124297
);COMMIT;

UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '13-SEP-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100054,
125294,
125295,
125296,
125297
);COMMIT;


/*UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '15-DEC-19'
WHERE RTRIM(ACT_GROUP) IN ('WB')
AND OBJID IN (
100030,
101295,
101296,
101294,
101297
);COMMIT;
*/

---- new code to replace manual piece below...

-- the select that feeds below manual work:
select RUN_ID, TO_DATE(RUN_DATE, 'DD-MON-YY'), channel, RTRIM(ANA_OFFER_COM), COUNT(*), COUNT(DISTINCT VIN)
FROM HMA_ANALYTICS_CAMPAIGN_HIST
WHERE RUN_DATE BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
GROUP BY RUN_ID, TO_DATE(RUN_DATE, 'DD-MON-YY'), channel, RTRIM(ANA_OFFER_COM)
ORDER BY 2, 1
;
/*
create table TMP_CNTRL_TEST
as select *
from &CURR_CNTRL;

UPDATE TMP_CNTRL_TEST
SET ACTION_COMP_DATE =
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
;COMMIT;
*/

-- the mysql version....oops....
/*
UPDATE TEST
SET TEST.action_comp_date =camps.min_run_date
FROM tmp_cntrl_test TEST
INNER JOIN (SELECT  EXTRACT(MONTH FROM run_date) AS mon, MIN(TO_DATE(run_date, 'DD-MON-YY')) AS min_run_date,  COUNT(*), COUNT(DISTINCT vin)
        FROM hma_analytics_campaign_hist
        WHERE run_date BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
        AND offer_com IN ('LB','WB')
        GROUP BY EXTRACT(MONTH FROM run_date)) CAMPS ON CAMPS.mon = EXTRACT(MONTH FROM TEST.action_comp_date)
WHERE RTRIM(TEST.act_group) IN ('LB','WB');
*/

-- the oracle version....
UPDATE &CURR_CNTRL TEST
SET TEST.action_comp_date =
        (SELECT   MIN(TO_DATE(run_date, 'DD-MON-YY'))
        FROM hma_analytics_campaign_hist CAMPS
        WHERE run_date BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
        AND offer_com IN ('LB','WB')
        AND EXTRACT(MONTH FROM CAMPS.run_date) = EXTRACT(MONTH FROM TEST.action_comp_date)
        AND RTRIM(TEST.act_group) IN ('LB','WB')
        GROUP BY EXTRACT(MONTH FROM run_date))
;



camps.min_run_date
FROM tmp_cntrl_test TEST
INNER JOIN (SELECT  EXTRACT(MONTH FROM run_date) AS mon, MIN(TO_DATE(run_date, 'DD-MON-YY')) AS min_run_date,  COUNT(*), COUNT(DISTINCT vin)
        FROM hma_analytics_campaign_hist
        WHERE run_date BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date'
        AND offer_com IN ('LB','WB')
        GROUP BY EXTRACT(MONTH FROM run_date)) CAMPS ON CAMPS.mon = EXTRACT(MONTH FROM TEST.action_comp_date)
WHERE RTRIM(TEST.act_group) IN ('LB','WB');








select * FROM HMA_ANALYTICS_CAMPAIGN_HIST where rownum <20;

select extract(month from action_comp_date)
select * from TMP_CNTRL_TEST

select * from TMP_CNTRL_TEST
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
order by action_comp_date

----------------------------------- Manual -----------------------------------------------------------------------------------
--Now, the fun part, manually update the drop dates based off data in ER
--Since we are estimating, we can just grab the first EM/DM drop for that campaign
-- in the future, automate this!
-- update for each month the min date and the appropriate runids
UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '06-OCT-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100052,
123294,
123295,
123296,
123297
);COMMIT;

UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '08-NOV-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100053,
124294,
124295,
124296,
124297
);COMMIT;

UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '13-SEP-21'
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
AND OBJID IN (
100054,
125294,
125295,
125296,
125297
);COMMIT;


/*UPDATE &Curr_cntrl
SET ACTION_COMP_DATE = '15-DEC-19'
WHERE RTRIM(ACT_GROUP) IN ('WB')
AND OBJID IN (
100030,
101295,
101296,
101294,
101297
);COMMIT;
*/
--small clean-up
UPDATE &Curr_cntrl
SET ACT_GROUP = RTRIM(ACT_GROUP)
WHERE RTRIM(ACT_GROUP) IN ('LB','WB')
;COMMIT;

--EXTRA OLD CODE VDPS contaminated VINs

/*
SELECT count(*) from &Curr_cntrl
WHERE VIN IN (SELECT VIN
              FROM HYUNDAI.HYU_TP_VDPS_CAMP_COMM_DTL
              WHERE TO_DATE(BATCH_PROCESSING_DATE, 'DD-MON-YY') BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date' and campaign_name not in ('ON_DEMAND','OnDemand - DM') );

DELETE FROM &Curr_cntrl
WHERE VIN IN (SELECT VIN
              FROM HYUNDAI.HYU_TP_VDPS_CAMP_COMM_DTL
              WHERE TO_DATE(BATCH_PROCESSING_DATE, 'DD-MON-YY') BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date');
COMMIT;
--2,795 rows deleted. 43 rows on dec 17 20
select count(*) from &Curr_cntrl;

DELETE FROM &Curr_cntrl
WHERE VIN IN (SELECT VIN
              FROM HYUNDAI.HYU_TP_VDPS_CAMP_COMM_DTL
              WHERE TO_DATE(BATCH_PROCESSING_DATE, 'DD-MON-YY') BETWEEN '&&begin_qtr_date' AND '&&end_qtr_date' and campaign_name not in ('ON_DEMAND','OnDemand - DM') );
commit;
select count(*) from &Curr_cntrl;*/
/*
--REMOVE THE CONTROL VINS NOT FROM ENROLLED DEALERS
DELETE FROM TMP_QBR_Q219_CONTROL_COMM
WHERE VIN IN (
select VIN
from TMP_QBR_Q219_CONTROL_COMM q
WHERE OEM_DEALERID NOT IN (SELECT OEM_DEALERID FROM QBR_CONTROL_COMM4)
);COMMIT;
*/
/* --Taken Care you in INSERT
-- REMOVE THE CONTROL VINS TRIGGERED FROM NON-OM DEALERS
DELETE FROM TMP_QBR_Q219_CONTROL_COMM
WHERE VIN IN (
SELECT DISTINCT Q.VIN
FROM TMP_QBR_Q219_CONTROL_COMM Q
JOIN HMA_HX_TIER_CURR H
ON Q.OEM_DEALERID = H.DEALER_CODE AND TO_DATE(H.LIVE_DATE, 'MM/DD/YY') <= '30-JUN-19' AND H.ENROLL_STATUS <> 'ACTIVE'
WHERE Q.ACT_GROUP IN ('LB', 'WB', 'CQ')
UNION
SELECT DISTINCT Q.VIN
FROM TMP_QBR_Q219_CONTROL_COMM Q
JOIN HMA_HX_TIER_CURR H
ON Q.OEM_DEALERID = H.DEALER_CODE AND TO_DATE(H.LIVE_DATE, 'MM/DD/YY') > '30-JUN-19'
WHERE Q.ACT_GROUP IN ('LB', 'WB', 'CQ'));COMMIT;
*/
--select * from HYUNDAI.HYU_DEALER_IMPRINT Where BAC_CODE = 'NY123'

select ACT_GROUP, COUNT(*), COUNT(DISTINCT VIN)
FROM &Curr_cntrl
GROUP BY ACT_GROUP
;
