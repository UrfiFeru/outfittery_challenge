{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_PostHistoryTypeId" as PostHistoryTypeId,
    "_PostId" as PostId,
    "_RevisionGUID" as RevisionGUID,
    "_CreationDate" as CreationDate,
    "_UserId" as UserId,
    "_UserDisplayName" as UserDisplayName,
    "_Comment" as Comment,
    "_Text" as Text,
    "_CloseReasonId" as CloseReasonId
FROM public.posthistory_landing

