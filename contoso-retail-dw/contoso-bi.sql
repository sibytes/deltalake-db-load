-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC # not compatible or needed on a passthrough AD cluster!
-- MAGIC from fathom.ConnectStorage import connect_storage
-- MAGIC from fathom import Configuration as config
-- MAGIC 
-- MAGIC connect_storage()
-- MAGIC config.help()

-- COMMAND ----------

create database if not exists ContosoBI;
use ContosoBI;

-- COMMAND ----------

create or replace view ContosoBi.Date
as

with dates
as
(
  select distinct 
    add_months(DateKey, 12*6) as DateKey
  from ContosoRetailDW.dimdate
  union all
  select make_date(2012, 02, 29) as DateKey
  union all
  select make_date(2016, 02, 29) as DateKey
)

select
  cast(date_format(DateKey, 'yMMdd') as int) as DateKey,
  DateKey as Date,
  year(DateKey) as Year,
  date_format(now(), 'MMM') as MonthName,
  day(DateKey) as DayOfMonth
from dates

-- COMMAND ----------

create or replace view ContosoBi.Product
as

select
  p.ProductKey,
  p.ProductLabel as `Product Code`,
  p.ProductName as `Product Name`,
  pc.ProductCategoryName as Category,
  ps.ProductSubcategoryName as Subcategory
from ContosoRetailDW.DimProduct p
left join ContosoRetailDW.DimProductSubcategory ps
  on ps.ProductSubcategoryKey = p.ProductSubcategoryKey
left join ContosoRetailDW.DimProductCategory pc
  on pc.ProductCategoryKey = ps.ProductCategoryKey;




-- COMMAND ----------

create or replace view ContosoBi.Sales
as

select
    StoreKey,
    ProductKey,
    CustomerKey,
    (CAST (OrderDateKey / 10000 AS INT) + 6) * 10000 + OrderDateKey % 10000 AS DateKey,
    Quantity,
    `Unit Price`,
    `Unit Discount`,
    `Unit Cost`,
    `Net Price`
from
    ContosoRetailDW.vSales
where
    OrderDateKey <= 20091031
    and OrderDateKey <> 20080229


-- COMMAND ----------

-- SELECT
--     CountryRegion,
--     Brand,
--     Month,
--     `2007` AS [Sales2013],
--     `2008` AS [Sales2014],
--     `2009` AS [Sales2015]
-- FROM
--     (
    select
        s.CountryRegion,
        p.Brand,
        d.`Calendar Year Number`,
        d.Month,
        d.`Month Number`,
        SUM(f.Quantity * f.`Unit Price`) AS SalesAmount
     from
        ContosoRetailDW.vSales f
        join DaxBook.Date d on d.DateKey = f.OrderDateKey
        join DaxBook.Store s on s.StoreKey = f.StoreKey
        join DaxBook.Product p on p.ProductKey = f.ProductKey
     WHERE
        f.DateKey <= 20091031
     GROUP BY
        d.`Calendar Year Number`,
        d.Month,
        d.`Month Number`,
        p.Brand,
        s.CountryRegion
--     ) Sales PIVOT ( SUM([SalesAmount]) FOR [Calendar Year Number] IN ([2007], [2008], [2009]) )
-- AS PivotTable;

-- COMMAND ----------

create or replace view ContosoBi.Store
as

select 
    s.StoreKey,
    s.StoreManager,
    StoreType,
    StoreName,
    g.ContinentName,
    g.CityName,
    g.StateProvinceName,
    g.RegionCountryName,
    EmployeeCount,
    SellingAreaSize
from ContosoRetailDW.DimStore s
left join ContosoRetailDW.DimGeography g on s.GeographyKey = g.GeographyKey;
