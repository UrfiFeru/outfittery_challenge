{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_CreationDate" as CreationDate,
    "_PostId" as PostId,
    "_RelatedPostId" as RelatedPostId,
    "_PostLinkTypeId" as PostLinkTypeId,
    "_UserId" as UserId
FROM public.postlinks_landing