select * from {{ ref('base_statcast__2008') }}

union all

select * from {{ ref('base_statcast__2009') }}

union all

select * from {{ ref('base_statcast__2010') }}

union all

select * from {{ ref('base_statcast__2011') }}

union all

select * from {{ ref('base_statcast__2012') }}

union all

select * from {{ ref('base_statcast__2013') }}

union all

select * from {{ ref('base_statcast__2014') }}

union all

select * from {{ ref('base_statcast__2015') }}

union all

select * from {{ ref('base_statcast__2016') }}

union all

select * from {{ ref('base_statcast__2017') }}

union all

select * from {{ ref('base_statcast__2018') }}

union all

select * from {{ ref('base_statcast__2019') }}
