-- DateDiff Macro Not Available For PostgreSql, hence unable to use DateDiff function, hence unable to correctly calculate
-- Average number of comments per month

{{ config(materialized='table') }}

select users.*,
NumberOfPost,
LastCreatedPostDateWithComments,
NumberOfPostLast30Days,
IsCritic,
IsEditor
--,AvgCommentsPerMonth
 from {{ref('users')}} users
LEFT JOIN
(
    select OwnerUserId,count(distinct Id) as NumberOfPost 
    from {{ref('post')}} 
    where PostTypeId = 1
    group by OwnerUserId
) NumberOfPost
ON users.Id = NumberOfPost.OwnerUserId
LEFT JOIN
(
    select OwnerUserId,max(CreationDate) as LastCreatedPostDateWithComments
    from {{ref('post')}} 
    where PostTypeId = 1 and CommentCount > 0
    group by OwnerUserId
) LastCreatedPostDateWithComments
ON users.Id = LastCreatedPostDateWithComments.OwnerUserId
LEFT JOIN
(
    select OwnerUserId,
    count(distinct Id) as NumberOfPostLast30Days
    from {{ref('post')}} 
    where PostTypeId = 1 and DATE(CreationDate) > 
    {{ dbt_utils.dateadd(datepart='day', interval=-30, from_date_or_timestamp=dbt_utils.current_timestamp()) }}
    group by OwnerUserId
) NumberOfPostLast30Days
ON users.Id = NumberOfPostLast30Days.OwnerUserId
LEFT JOIN
(
    select UserId,
    max(Case when Name='Critic' then 1 else 0 end) as IsCritic,
    max(Case when Name='Editor' then 1 else 0 end) as IsEditor
    from {{ref('badges')}}
    Group by 1
) Bool_Values
ON users.Id = Bool_Values.UserId
-- LEFT JOIN
-- (
-- Select UserId,NumberOfComments/NumberOfMonths as AvgCommentsPerMonth from
-- (
--   select comm.UserId,
--    max(datediff(usr.CreationDate, current_timestamp(), 'month')) as NumberOfMonths
--    ,count(distinct comm.Id) as NumberOfComments
--   from {{ref('comments')}} comm
--   left join {{ref('users')}} usr
--   on comm.UserId = usr.Id
--   group by 1
-- ) MonthlyAggregate
-- group by 1
-- ) AvgCommentsPerMonth
-- ON users.Id = AvgCommentsPerMonth.UserId
