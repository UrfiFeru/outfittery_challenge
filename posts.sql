{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_PostTypeId" as PostTypeId,
    "_ParentId" as ParentId,
    "_AcceptedAnswerId" as AcceptedAnswerId,
    "_CreationDate" as CreationDate,
    "_Score" as Score,
    "_ViewCount" as ViewCount,
    "_Body" as Body,
    "_OwnerUserId" as OwnerUserId,
    "_LastEditorUserId" as LastEditorUserId,
    "_LastEditorDisplayName" as LastEditorDisplayName,
    "_LastEditDate" as LastEditDate,
    "_LastActivityDate" as LastActivityDate,
    "_CommunityOwnedDate" as CommunityOwnedDate,
    "_ClosedDate" as ClosedDate,
    "_Title" as Title,
    "_Tags" as Tags,
    "_AnswerCount" as AnswerCount,
    "_CommentCount" as CommentCount,
    "_FavoriteCount" as FavoriteCount
FROM public.posts_landing