{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_PostId" as PostId,
    "_VoteTypeId" as VoteTypeId,
    "_CreationDate" as CreationDate,
    "_UserId" as UserId,
    "_BountyAmount" as BountyAmount
FROM public.votes_landing
