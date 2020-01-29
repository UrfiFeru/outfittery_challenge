{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_PostId" as PostId,
    "_Score" as Score,
    "_Text" as Text,
    "_CreationDate" as CreationDate,
    "_UserId" as UserId
FROM public.comments_landing

