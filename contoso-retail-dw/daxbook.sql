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

create database if not exists daxbook;

-- COMMAND ----------

create or replace view daxbook.Channel
as

select 
  ChannelKey,
  ChannelLabel as `Channel Code`,
  ChannelName as `Channel Name`
from ContosoRetailDW.DimChannel


-- COMMAND ----------

create or replace view daxbook.Currency
as

select
  CurrencyKey,
  CurrencyName as `Currency Code`,
  CurrencyDescription as `Currency`
from ContosoRetailDw.DimCurrency

-- COMMAND ----------

create or replace view daxbook.Customer
as

select  CustomerKey,
        c.GeographyKey,
        CustomerLabel as `Customer Code`,
        Title,
        concat(ifnull(LastName + ', ', ''), ifnull(FirstName, '')) as `Name`,
        cast(BirthDate as DATE) as `Birth Date`,
        MaritalStatus as `Marital Status`,
        Gender,
        YearlyIncome as `Yearly Income`,
        TotalChildren as `Total Children`,
        NumberChildrenAtHome as `Children At Home`,
        Education,
        Occupation,
        case HouseOwnerFlag
          when 1 then 'House Owner'
          when 0 then 'No Owner'
        end `House Ownership`,
        NumberCarsOwned as `Cars Owned`,
        g.ContinentName as Continent,
        g.CityName as City,
        g.StateProvinceName as State,
        g.RegionCountryName as Country,
        AddressLine1 as `Address Line 1`,
        AddressLine2 as `Address Line 2`,
        Phone,
        DateFirstPurchase as `Date First Purchase`,
        CustomerType as `Customer Type`,
        CompanyName as `Company Name`
from    ContosoRetailDw.DimCustomer c
left join ContosoRetailDw.DimGeography g on c.GeographyKey = g.GeographyKey

-- COMMAND ----------

create or replace view daxbook.Date
as

with fiscalCalendar
  as ( select   
             Datekey,
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
             cast(CalendarYear + if(MONTH(Datekey) > 6, 1, 0) as int) as FiscalYear,
             cast(( ( MONTH(Datekey) - 1 ) / 3 + 2 ) % 4 + 1 as int) as FiscalQuarter,
             cast(( MONTH(Datekey) + 5 ) % 12 + 1 as int) as FiscalMonthNumber,
             IsWorkDay,
             IsHoliday,
             HolidayName,
             EuropeSeason,
             NorthAmericaSeason,
             AsiaSeason
  from ContosoRetailDw.DimDate
)
select  cast (Datekey as date)                         as `Date`,
        cast(date_format(DateKey, 'yMMdd') as int)     as `DateKey`,
        CalendarYear                                   as `Calendar Year Number`,
        concat('CY ', CalendarYear)                    as `Calendar Year`,
        CalendarQuarter                                as `Calendar Year Quarter Number`,
        concat(CalendarQuarterLabel,'-',CalendarYear)  as `Calendar Year Quarter`,
        CalendarMonth                                  as `Calendar Year Month Number`,
        concat(CalendarMonthLabel,' ',CalendarYear)    as `Calendar Year Month`,
        Month(Datekey)                                 as `Month Number`,
        CalendarMonthLabel                             as `Month`,
        weekday(Datekey)                               as `Day of Week Number`,
        CalendarDayOfWeekLabel                         as `Day of Week`,
        FiscalYear                                     as `Fiscal Year Number`,
        concat('FY ', FiscalYear)                      as `Fiscal Year`,
        FiscalQuarter                                  as `Fiscal Quarter Number`,
        concat('Q', FiscalQuarter)                     as `Fiscal Quarter`,
        FiscalYear * 10 + FiscalQuarter                as `Fiscal Year Quarter Number`,
        concat('Q', FiscalQuarter, '-', FiscalYear)    as `Fiscal Year Quarter`,
        FiscalMonthNumber                              as `Fiscal Month Number`,
        CalendarMonthLabel                             as `Fiscal Month`,
        IsWorkDay                                      as `Working Day`,
        IsHoliday                                      as `Is Holiday`,
        HolidayName                                    as `Holiday Name`,
        EuropeSeason                                   as `Europe Season`,
        NorthAmericaSeason                             as `North America Season`,
        AsiaSeason                                     as `Asia Season`
from    fiscalCalendar

-- COMMAND ----------

create or replace view daxbook.Delivery_Date
as

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
           CalendarYear + if(CalendarMonth > 6, 1, 0) as `FiscalYear`,
           cast(( ( month(Datekey) - 1 ) / 3 + 2 ) % 4 + 1 as int) as `FiscalQuarter`,
           cast(( month(Datekey) + 5 ) % 12 + 1 as int) as `FiscalMonthNumber`,
           IsWorkDay,
           IsHoliday,
           HolidayName,
           EuropeSeason,
           NorthAmericaSeason,
           AsiaSeason
  from ContosoRetailDw.DimDate
)
select  
  cast (Datekey AS date)                         as `Delivery Date`,
  cast(date_format(DateKey, 'yMMdd') as int)     as `Delivery DateKey`,
  CalendarYear                                   as `Delivery Year Number`,
  concat('DY ', CalendarYear)                    as `Delivery Year`,
  CalendarQuarter                                as `Delivery Year Quarter Number`,
  concat(CalendarQuarterLabel,'-',CalendarYear)  as `Delivery Year Quarter`,
  CalendarMonth                                  as `Delivery Year Month Number`,
  concat(CalendarMonthLabel,' ',CalendarYear)    as `Delivery Year Month`,
  month(Datekey)                                 as `Delivery Month Number`,
  CalendarMonthLabel                             as `Delivery Month`
from fiscalCalendar

-- COMMAND ----------

create or replace view daxbook.Due_Date
as

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
           CalendarYear + if(CalendarMonth > 6, 1, 0) as `FiscalYear`,
           cast(( ( month(Datekey) - 1 ) / 3 + 2 ) % 4 + 1 as int) as `FiscalQuarter`,
           cast(( month(Datekey) + 5 ) % 12 + 1 as int) as `FiscalMonthNumber`,
           IsWorkDay,
           IsHoliday,
           HolidayName,
           EuropeSeason,
           NorthAmericaSeason,
           AsiaSeason
  from ContosoRetailDw.DimDate
)
select  
  cast (Datekey AS date)                         as `Due Date`,
  cast(date_format(DateKey, 'yMMdd') as int)     as `Due DateKey`,
  CalendarYear                                   as `Due Year Number`,
  concat('EY ', CalendarYear)                    as `Due Year`,
  CalendarQuarter                                as `Due Year Quarter Number`,
  concat(CalendarQuarterLabel,'-',CalendarYear)  as `Due Year Quarter`,
  CalendarMonth                                  as `Due Year Month Number`,
  concat(CalendarMonthLabel,' ',CalendarYear)    as `Due Year Month`,
  month(Datekey)                                 as `Due Month Number`,
  CalendarMonthLabel                             as `Due Month`
from fiscalCalendar

-- COMMAND ----------

create or replace view daxbook.Order_Date
as

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
           CalendarYear + if(CalendarMonth > 6, 1, 0) as `FiscalYear`,
           cast(( ( month(Datekey) - 1 ) / 3 + 2 ) % 4 + 1 as int) as `FiscalQuarter`,
           cast(( month(Datekey) + 5 ) % 12 + 1 as int) as `FiscalMonthNumber`,
           IsWorkDay,
           IsHoliday,
           HolidayName,
           EuropeSeason,
           NorthAmericaSeason,
           AsiaSeason
  from ContosoRetailDw.DimDate
)
select  
  cast (Datekey AS date)                         as `Order Date`,
  cast(date_format(DateKey, 'yMMdd') as int)     as `Order DateKey`,
  CalendarYear                                   as `Order Year Number`,
  concat('OY ', CalendarYear)                    as `Order Year`,
  CalendarQuarter                                as `Order Year Quarter Number`,
  concat(CalendarQuarterLabel,'-',CalendarYear)  as `Order Year Quarter`,
  CalendarMonth                                  as `Order Year Month Number`,
  concat(CalendarMonthLabel,' ',CalendarYear)    as `Order Year Month`,
  month(Datekey)                                 as `Order Month Number`,
  CalendarMonthLabel                             as `Order Month`
from fiscalCalendar

-- COMMAND ----------

create or replace view daxbook.Employee
as

select
  employeeKey,
  ParentEmployeeKey,
  concat(FirstName, ' ', LastName) as Name
from ContosoRetailDw.DimEmployee

-- COMMAND ----------

create or replace view daxbook.Inventory
as

with    filtered_inventory
          -- Filter a sample of the rows in the table
          as ( select   *
               from     ContosoRetailDw.FactInventory
               where    ( select    SampleRows_InventorySales
                          from      ContosoRetailDw.vFilterRatio
                        ) is NULL
                        or ( InventoryKey / 10 ) % 1000 < (
                              (select cast(SampleRows_InventorySales as FLOAT) * 1000 
                               from ContosoRetailDw.vFilterRatio) / ( select COUNT (1) from ContosoRetailDw.FactInventory) 
                        )
             ),
        i as (select InventoryKey,
                     StoreKey,
                     ProductKey,
                     CurrencyKey,
                     cast (DateKey AS date) as `date`,
                     cast(date_format(DateKey, 'yMMdd') as int) as DateKey,
                     OnHandQuantity as `Quantity`,
                     UnitCost as `Unit Cost`
               from  filtered_inventory
             )
    select  i.InventoryKey,
            i.StoreKey,
            i.ProductKey,
            i.CurrencyKey,
            i.Date,
            i.Quantity,
            i.`Unit Cost`
       -- Total line values are not included in the view - should be calculated in DAX measures
       -- i.Quantity * i.[Unit Cost] as `Total Cost`
    from    i


-- COMMAND ----------


create or replace view daxbook.Product_Category
as

select  ProductCategoryKey,
        ProductCategoryLabel as `Category Code`,
        ProductCategoryName
from    ContosoRetailDw.DimProductCategory




-- COMMAND ----------


create or replace view daxbook.Product_Subcategory
as 

select  ProductSubcategoryKey,
        ProductSubcategoryLabel as `Subcategory Code`,
        ProductSubcategoryName as Subcategory,
        ProductCategoryKey
from    ContosoRetailDw.DimProductSubcategory




-- COMMAND ----------

create or replace view daxbook.Product
as

select  ProductKey,
        ProductLabel as `Product Code`,
        ProductName as `Product Name`,
        ProductDescription as `Product Description`,
        ProductSubcategoryKey,
        Manufacturer,
        BrandName as `Brand`,
        ClassName as Class,
        StyleName as Style,
        ColorName as Color,
        Size,
        Weight,
        NULLIF(WeightUnitMeasureID, '') as `Weight Unit Measure`,
        StockTypeID as `Stock Type Code`,
        StockTypeName as `Stock Type`,
        UnitCost as `Unit Cost`,
        UnitPrice as `Unit Price`,
        AvailableForSaleDate as `Available Date`,
        Status
from ContosoRetailDw.DimProduct

-- COMMAND ----------


create or replace view daxbook.Promotion
as

select PromotionKey,
       PromotionLabel as `Promotion Code`,
       PromotionName as Promotion,
       DiscountPercent as Discount,
       PromotionType as `Promotion Type`,
       PromotionCategory as `Promotion Category`,
       StartDate as `Start Date`,
       EndDate as `End Date`
FROM ContosoRetailDw.DimPromotion;



-- COMMAND ----------


create or replace view daxbook.Sales
as 
with    filtered_sales
          -- Filter a sample of the rows in the table
          as ( select   *
               from     ContosoRetailDw.FactOnlineSales
               where    ( select SampleRows_OnlineSales from ContosoRetailDw.vFilterRatio ) is NULL
                        or ( OnlineSalesKey / 10 ) % 1000 < ( 
                            ( select cast(SampleRows_OnlineSales as FLOAT) * 1000 
                              from ContosoRetailDw.vFilterRatio) / 
                            ( select COUNT (1) from ContosoRetailDw.FactOnlineSales) 
                           )
             ),
        s as ( select   DateKey,OnlineSalesKey,
                        StoreKey,
                        ProductKey,
                        PromotionKey,
                        CurrencyKey,
                        CustomerKey,
                        cast (DateKey as date)     as `Order Date`,
                        date_add(
                          DateKey, 
                          cast(( ProductKey + OnlineSalesKey + CustomerKey ) % 8 + 6 as int) 
                        ) as `Due Date`,
                        -- Delivery improves over time
                        date_add(
                          DateKey, 
                          cast(( StoreKey + ProductKey + OnlineSalesKey + CustomerKey ) % ( 2014 - YEAR(DateKey) ) + 6 as int) 
                        ) as `Delivery Date`,
                        SalesOrderNumber           as `Order Number`,
                        SalesOrderLineNumber       as `Order Line Number`,
                        -- Increase quantity to 20% of sales transactions
                        SalesQuantity
                        * case when SalesQuantity = 1
                                    and ( ProductKey + OnlineSalesKey + CustomerKey ) % 5 = 0
                               then ( ProductKey + StoreKey + ( OnlineSalesKey * 4 ) + CustomerKey ) % 3 + 2
                               else SalesQuantity
                          end                      as Quantity,
                        UnitPrice                  as `Unit Price`,
                        DiscountAmount             as `Unit Discount`,
                        UnitCost                   as `Unit Cost`,
                        UnitPrice - DiscountAmount as `Net Price`
               from     filtered_sales
               where    SalesQuantity = 1 -- Ignore Returned items
             )
    select  s.OnlineSalesKey,
            s.StoreKey,
            s.ProductKey,
            s.PromotionKey,
            s.CurrencyKey,
            s.CustomerKey,
            cast(date_format(`Order Date`, 'yMMdd') as int) as OrderDateKey,
            cast(date_format(`Due Date`, 'yMMdd') as int) as DueDateKey,
            cast(date_format(`Delivery Date`, 'yMMdd') as int) as DeliveryDateKey,
            s.`Order Date`,
            s.`Due Date`,
            s.`Delivery Date`,
            s.`Order Number`,
            s.`Order Line Number`,
            s.Quantity,
            s.`Unit Price`,
            s.`Unit Discount`,
            s.`Unit Cost`,
            s.`Net Price`
       -- Total line values are not included in the view - should be calculated in DAX measures
       --[Sales Amount] = s.Quantity * s.[Net Price],
       --[Discount Amount] = s.Quantity * s.[Unit Discount],
       --[Total Cost] = s.Quantity * s.[Unit Cost]
    from    s




-- COMMAND ----------


create or replace view daxbook.Store_Sales
as
with    filtered_sales
          -- Filter a sample of the rows in the table
          as ( select   *
               from     ContosoRetailDw.FactSales
               where   ( select    SampleRows_StoreSales
                         from      ContosoRetailDw.vFilterRatio
                       ) is NULL or ( SalesKey / 10 ) % 1000 < 
                         ( 
                           ( select cast (SampleRows_StoreSales AS FLOAT) * 1000
                             from ContosoRetailDw.vFilterRatio) 
                             / 
                           ( select COUNT(1) from ContosoRetailDw.FactSales ) 
                         )
             ),
        s as ( select   SalesKey,
                        channelKey,
                        StoreKey,
                        ProductKey,
                        PromotionKey,
                        CurrencyKey,
                        cast (DateKey AS DATE) as `Date`,
                        cast(date_format(`DateKey`, 'yMMdd') as int) as `DateKey`,
                        SalesQuantity as `Quantity`,
                        UnitPrice as `Unit Price`,
                        UnitCost as `Unit Cost`,
                        DiscountQuantity as `Discount Quantity`,
                        DiscountAmount as `Discount Amount`
               from     filtered_sales
             )
    SELECT  s.SalesKey,
            s.ChannelKey,
            s.StoreKey,
            s.ProductKey,
            s.PromotionKey,
            s.CurrencyKey,
            s.Date,
            s.Quantity,
            s.`Unit Price`,
            s.`Unit Cost`,
            s.`Discount Quantity`,
            s.`Discount Amount`
       -- Total line values are not included in the view - should be calculated in DAX measures
       --[Sales Amount] = s.Quantity * s.[Unit Price] - s.[Discount Amount,
       --[Total Cost] = s.Quantity * s.[Unit Cost]
    from s



-- COMMAND ----------



create or replace view daxbook.Store
as
select  StoreKey,
        s.GeographyKey,
        s.StoreManager           as `Store Manager`,
        StoreType                as `Store Type`,
        StoreName                as `Store Name`,
        Status,
        OpenDate                 as `Open Date`,
        CloseDate                as `Close Date`,
        ZipCode                  as `Zip Code`,
        ZipCodeExtension         as `Zip Code Extension`,
        StorePhone               as `Store Phone`,
        StoreFax                 as `StoreFax`,
        g.ContinentName          as `Continent`,
        g.CityName               as `City`,
        g.StateProvinceName      as `State`,
        g.RegionCountryName      as `CountryRegion`,
        AddressLine1             as `Address Line 1`,
        AddressLine2             as `Address Line 2`,
        CloseReason              as `Close Reason`,
        EmployeeCount            as `Employees`,
        SellingAreaSize          as `Selling Area`,
        LastRemodelDate          as `Last Remodel Date`
from    ContosoRetailDw.DimStore s
left join ContosoRetailDw.DimGeography g
        on s.GeographyKey = g.GeographyKey;

