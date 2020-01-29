{{ config(materialized='view') }}

SELECT 
    "_UserId" as UserId,
    "_Name" as Name,
    "_Date" as Date
FROM public.badges_landing