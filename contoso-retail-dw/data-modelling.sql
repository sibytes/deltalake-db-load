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


create or replace view ContosoRetailDw.vDate as

with fiscalCalendar
          as ( 
                select   Datekey,
                        FullDateLabel,
                        DateDescription,
                        CalendarYear,
                        CalendarYearLabel,
                        CalendarHalfYear,
                        CalendarHalfYearLabel,
                        CalendarQuarter,
                        CalendarQuarterLabel,
                        CalendarMonth,
                        CalendarMonthLabel,
                        CalendarWeek,
                        CalendarWeekLabel,
                        CalendarDayOfWeek,
                        CalendarDayOfWeekLabel,
                        CalendarYear + if(MONTH(Datekey) > 6, 1, 0) as FiscalYear,
                        cast(( ( MONTH(Datekey) - 1 ) / 3 + 2 ) % 4 + 1 as int) as FiscalQuarter,
                        ( MONTH(Datekey) + 5 ) % 12 + 1 as FiscalMonthNumber, 
                        IsWorkDay,
                        IsHoliday,
                        HolidayName,
                        EuropeSeason,
                        NorthAmericaSeason,
                        AsiaSeason
               from     ContosoRetailDW.DimDate
             )
    select  cast (Datekey as DATE) as Date,
            cast(date_format(DateKey, 'yMMdd') as int)      as DateKey,
            CalendarYear                                    as `Calendar Year Number`,
            concat('CY ', CalendarYear)                     as `Calendar Year`,
            CalendarQuarter                                 as `Calendar Year Quarter Number`,
            concat(CalendarQuarterLabel, '-', CalendarYear) as `Calendar Year Quarter`,
            CalendarMonth                                   as `Calendar Year Month Number`,
            concat(CalendarMonthLabel, ' ', CalendarYear)   as `Calendar Year Month`,
            Month(Datekey)                                  as `Month Number`,
            CalendarMonthLabel                              as `Month`,
            WeekOfYear(Datekey)                             as `Day of Week Number`,
            CalendarDayOfWeekLabel                          as `Day of Week`,
            FiscalYear                                      as `Fiscal Year Number`,
            concat('FY ', FiscalYear)                       as `Fiscal Year`,
            FiscalQuarter                                   as `Fiscal Quarter Number`,
            concat('Q', FiscalQuarter)                      as `Fiscal Quarter`,
            FiscalYear * 10 + FiscalQuarter                 as `Fiscal Year Quarter Number`,
            concat('Q' , FiscalQuarter , '-', FiscalYear)   as `Fiscal Year Quarter`,
            FiscalMonthNumber                               as `Fiscal Month Number`,
            CalendarMonthLabel                              as `Fiscal Month`,
            IsWorkDay                                       as `Working Day`,
            IsHoliday                                       as `Is Holiday`,
            HolidayName                                     as `Holiday Name`,
            EuropeSeason                                    as `Europe Season`,
            NorthAmericaSeason                              as `North America Season`,
            AsiaSeason                                      as `Asia Season`
    from    fiscalCalendar

-- COMMAND ----------

create or replace view ContosoRetailDW.vCustomer
as

select  c.CustomerKey,
        c.GeographyKey,
        c.CustomerLabel                               as `Customer Code`,
        c.Title,
        concat(COALESCE(concat(c.LastName, ', '), '')
              ,COALESCE(c.FirstName, ''))             as Name,
        cast(c.BirthDate as DATE)                     as `Birth Date`,
        c.MaritalStatus                               as `Marital Status`,
        c.Gender,
        c.YearlyIncome                                as `Yearly Income`,
        c.TotalChildren                               as `Total Children`,
        c.NumberChildrenAtHome                        as `Children At Home`,
        c.Education,
        c.Occupation,
        case c.HouseOwnerFlag
          when 1 then 'House Owner'
          when 0 then 'No Owner'
        end                                          as `House Ownership`,
        c.NumberCarsOwned                            as `Cars Owned`,
        g.ContinentName                              as Continent,
        g.CityName                                   as City,
        g.StateProvinceName                          as State,
        g.RegionCountryName                          as CountryRegion,
        c.AddressLine1                               as `Address Line 1`,
        c.AddressLine2                               as `Address Line 2`,
        c.Phone,
        c.DateFirstPurchase                          as `Date First Purchase`,
        c.CustomerType                               as `Customer Type`,
        c.CompanyName                                as `Company Name`
from    ContosoRetailDw.DimCustomer c
left join ContosoRetailDw.DimGeography g
        on c.GeographyKey = g.GeographyKey

-- COMMAND ----------

create or replace view ContosoRetailDw.vProductCategory
as
select 
  ProductCategoryKey,
  ProductCategoryLabel as `Categoryt Code`,
  ProductCategoryName as `Category`
from ContosoRetailDw.DimProductCategory


-- COMMAND ----------

create or replace view ContosoRetailDw.vProductSubcategory
as
select
  ProductSubcategoryKey,
  ProductSubcategoryLabel as `SubcategoryCode`,
  ProductSubcategoryName as `Subcategory`,
  ProductCategoryKey
from ContosoRetailDw.DimProductSubcategory


-- COMMAND ----------

create or replace view ContosoRetailDw.vProduct
as

select
  ProductKey,
  ProductLabel as `Product Code`,
  ProductName as `Product Name`,
  ProductDescription as `Product Description`,
  ProductSubcategoryKey,
  Manufacturer,
  BrandName as Brand,
  ClassName as Class,
  StyleName as Style,
  ColorName as Color,
  Size,
  Weight,
  nullif(WeightUnitMeasureID, '') as `Weight Unit Measure`,
  StockTypeID as `Stock Type Code`,
  StockTypeName as `Stock Type Name`,
  UnitCost as `Unit Cost`,
  UnitPrice as `Unit Price`,
  AvailableForSaleDate as `Available Date`
from ContosoRetailDw.DimProduct


-- COMMAND ----------

create or replace view ContosoRetailDw.vPromotion
as

select
  PromotionKey,
  PromotionLabel as `Promotion Code`,
  PromotionName as Promotion,
  DiscountPercent as Discount,
  PromotionType as `Promotion Type`,
  PromotionCategory as `Promotion Category`,
  StartDate as `Start Date`,
  EndDate as `End Date`
from ContosoRetailDw.DimPromotion

-- COMMAND ----------

create or replace view ContosoRetailDw.vFilterRatio
as
select  100000 as SampleRows_OnlineSales    ,
        100000 as SampleRows_StoreSales     ,
		100000 as SampleRows_InventorySales 
        -- where 1 = 0 -- Use this WHERE condition to show all the values (around 10M rows)


-- COMMAND ----------

create or replace view ContosoRetailDw.vPurchases
as

with filtered_sales as
(
  select *
  from ContosoRetailDw.FactSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (SalesKey / 10) % 1000 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactSales)
    )
),
s as 
(
  select
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    cast(DateKey as Date) as `Order Date`,
    date_add(cast(DateKey as Date), 
        cast(((ProductKey + SalesKey) % 8 + 6) as int)
      ) as `Due Date`,
    -- delivery improves over time
    date_add(cast(DateKey as Date), 
        cast(((StoreKey + ProductKey + SalesKey) % ( 2014 - year(DateKey) ) + 6) as int)
      ) as `Delivery Date`,
    SalesQuantity as Quantity,
    UnitPrice as `Unit Price`,
    DiscountAmount as `Unit Discount`,
    UnitCost as `Unit Cost`,
    UnitPrice - DiscountAmount as `Net Price`
    from filtered_sales   
)
select  
  s.StoreKey,
  s.ProductKey,
  s.PromotionKey,
  s.CurrencyKey,
  cast(date_format(`Order Date`, 'yMMdd') as int)    as OrderDateKey,
  cast(date_format(`Due Date`, 'yMMdd') as int)      as DueDateKey,
  cast(date_format(`Delivery Date`, 'yMMdd') as int) as DeliveryDateKey,
  s.`Order Date`,
  s.`Due Date`,
  s.`Delivery Date`,
  s.`Quantity`,
  s.`Unit Price`,
  s.`Unit Discount`,
  s.`Unit Cost`,
  s.`Net Price`
from    s

-- COMMAND ----------

create or replace view ContosoRetailDw.vSales
as

with filtered_sales as
(
  select *
  from ContosoRetailDw.FactOnlineSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (OnlineSalesKey / 10) % 1000 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactOnlineSales)
    )
),
s as 
(
  select
    OnlineSalesKey,
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    CustomerKey,
    cast(DateKey as Date) as `Order Date`,
    date_add(cast(DateKey as Date), 
        cast(((ProductKey + OnlineSalesKey + CustomerKey) % 8 + 6) as int)
      ) as `Due Date`,
    -- delivery improves over time
    date_add(cast(DateKey as Date), 
        cast(((StoreKey + ProductKey + OnlineSalesKey + CustomerKey) % ( 2014 - year(DateKey) ) + 6) as int)
      ) as `Delivery Date`,
    SalesOrderNumber as `Order Number`,
    SalesOrderLineNumber as `Order Line Number`,
    SalesQuantity * 
    case when SalesQuantity = 1
         and ( ProductKey + OnlineSalesKey + CustomerKey ) % 5 = 0
         then ( ProductKey + StoreKey + ( OnlineSalesKey * 4 ) + CustomerKey ) % 3 + 2
         else SalesQuantity
    end as `Quantity`,
    UnitPrice as `Unit Price`,
    DiscountAmount as `Unit Discount`,
    UnitCost as `Unit Cost`,
    UnitPrice - DiscountAmount as `Net Price`
  from filtered_sales
  -- Ignore returned items
  where SalesQuantity = 1
)
select  
  s.StoreKey,
  s.ProductKey,
  s.PromotionKey,
  s.CurrencyKey,
  s.CustomerKey,
  cast(date_format(`Order Date`, 'yMMdd') as int)    as OrderDateKey,
  cast(date_format(`Due Date`, 'yMMdd') as int)      as DueDateKey,
  cast(date_format(`Delivery Date`, 'yMMdd') as int) as DeliveryDateKey,
  s.`Order Date`,
  s.`Due Date`,
  s.`Delivery Date`,
  s.`Order Number`,
  s.`Order Line Number`,
  s.`Quantity`,
  s.`Unit Price`,
  s.`Unit Discount`,
  s.`Unit Cost`,
  s.`Net Price`
from s

-- COMMAND ----------

create or replace view ContosoRetailDw.vSalesByCategory
as

select 
    FullDateLabel,
    Manufacturer,
    BrandName,
    ProductSubcategoryName,
    ProductCategoryName,
    sum( SalesQuantity ) as SalesQuantity,
    sum( SalesAmount   ) as SalesAmount,
    sum( TotalCost     ) as TotalCost

from ContosoRetailDw.FactOnlineSales s
join ContosoRetailDw.DimCustomer            c  on c.CustomerKey = s.CustomerKey
join ContosoRetailDw.DimProduct             p  on p.ProductKey = s.ProductKey
join ContosoRetailDw.DimProductSubcategory  ps on ps.ProductSubcategoryKey = p.ProductSubcategoryKey
join ContosoRetailDw.DimProductCategory     pc on pc.ProductCategoryKey = ps.ProductCategoryKey
join ContosoRetailDw.DimDate                d  on d.Datekey = s.DateKey
group by 
    FullDateLabel,
    Manufacturer,
    BrandName,
    ProductSubcategoryName,
    ProductCategoryName

-- COMMAND ----------

create or replace view ContosoRetailDw.vStore
as

select  s.StoreKey,
        s.GeographyKey,
        s.StoreManager        as `Store Manager`,
        StoreType             as `Store Type`,
        StoreName             as `Store Name`,
        Status,
        OpenDate              as `Open Date`,
        CloseDate             as `Close Date`,
        ZipCode               as `Zip Code`, 
        ZipCodeExtension      as `Zip Code Extension`,
        StorePhone            as `Store Phone`,
        StoreFax,
        g.ContinentName       as `Continent`,
        g.CityName            as `City`,
        g.StateProvinceName   as `State`,
        g.RegionCountryName   as `CountryRegion`,
        AddressLine1          as `Address Line 1`,
        AddressLine2          as `Address Line 2`,
        CloseReason           as `Close Reason`,
        EmployeeCount         as `Employees`,
        SellingAreaSize       as `Selling Area`,
        LastRemodelDate       as `Last Remodel Date`
from ContosoRetailDw.DimStore s
left join ContosoRetailDw.DimGeography g on s.GeographyKey = g.GeographyKey

-- COMMAND ----------

create or replace view ContosoRetailDw.AllPurchases
as

with filtered_sales as
(
  select *
  from ContosoRetailDw.FactSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (SalesKey / 10) % 100 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactOnlineSales)
    )
),
Purchases as 
(
  select
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    cast(date_format(`DateKey`, 'yMMdd') as int) as OrderDateKey,
    SalesQuantity as Quantity,
    UnitCost as `Unit Cost`
  from filtered_sales
)
select  
    Quantity,
	`Unit Cost`,
    ProductKey,
	OrderDateKey
from Purchases

-- COMMAND ----------

create or replace view ContosoRetailDw.vAllPurchasesDenormalized
as

with filtered_sales as
(
  select *
  from ContosoRetailDw.FactSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (SalesKey / 10) % 100 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactOnlineSales)
    )
),
Purchases as 
(
  select
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    cast(date_format(`DateKey`, 'yMMdd') as int) as OrderDateKey,
    SalesQuantity as Quantity,
    UnitCost as `Unit Cost`
  from filtered_sales
)
select  
    Quantity,
	`Unit Cost`,
    ProductName,
    ColorName,
    Manufacturer,
	d.Date,
    BrandName,
    ProductSubcategoryName,
    ProductCategoryName
from Purchases pu
join ContosoRetailDw.DimProduct            p  on p.ProductKey = pu.ProductKey
join ContosoRetailDw.DimProductSubcategory ps on ps.ProductSubcategoryKey = p.ProductSubcategoryKey
join ContosoRetailDw.DimProductCategory    pc on pc.ProductCategoryKey = ps.ProductCategoryKey
join ContosoRetailDw.vDate                 d  on d.Datekey = pu.OrderDateKey

-- COMMAND ----------

create or replace view ContosoRetailDw.vAllSales
as

with filtered_sales as
(
  select *
  from ContosoRetailDw.FactOnlineSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (OnlineSalesKey / 10) % 100 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactOnlineSales)
    )
),
Purchases as 
(
  select
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    cast(date_format(`DateKey`, 'yMMdd') as int) as OrderDateKey,
    SalesQuantity as Quantity,
    UnitPrice as `Unit Price`
  from filtered_sales
)
select  
    Quantity,
	`Unit Price`,
    OrderDateKey
from Purchases pu

-- COMMAND ----------

create or replace view ContosoRetailDw.AllSalesDenormalized
as


with filtered_sales as
(
  select *
  from ContosoRetailDw.FactOnlineSales 
  where
    (select SampleRows_OnlineSales
    from ContosoRetailDw.vFilterRatio) is null
    or
    (OnlineSalesKey / 10) % 100 <
    (
      (select cast(SampleRows_OnLineSales as float) * 1000
       from ContosoRetailDw.vFilterRatio
      ) / (select count(1) from ContosoRetailDw.FactOnlineSales)
    )
),
Purchases as 
(
  select
    StoreKey,
    ProductKey,
    PromotionKey,
    CurrencyKey,
    cast(date_format(`DateKey`, 'yMMdd') as int) as OrderDateKey,
    SalesQuantity as Quantity,
    UnitPrice as `Unit Price`
  from filtered_sales
)
select  
    Quantity,
	`Unit Price`,
    ProductName,
    ColorName,
    Manufacturer,
	d.Date,
    BrandName,
    ProductSubcategoryName,
    ProductCategoryName
from Purchases pu
join ContosoRetailDw.DimProduct            p  on p.ProductKey = pu.ProductKey
join ContosoRetailDw.DimProductSubcategory ps on ps.ProductSubcategoryKey = p.ProductSubcategoryKey
join ContosoRetailDw.DimProductCategory    pc on pc.ProductCategoryKey = ps.ProductCategoryKey
join ContosoRetailDw.vDate                 d  on d.Datekey = pu.OrderDateKey
