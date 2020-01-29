{{ config(materialized='view') }}

SELECT 
    "_Id" as Id,
    "_Reputation" as Reputation,
    "_CreationDate" as CreationDate,
    "_DisplayName" as DisplayName,
    "_EmailHash" as EmailHash,
    "_LastAccessDate" as LastAccessDate,
    "_WebsiteUrl" as WebsiteUrl,
    "_Location" as Location,
    "_Age" as Age,
    "_AboutMe" as AboutMe,
    "_Views" as Views,
    "_UpVotes" as UpVotes,
    "_DownVotes" as DownVotes,
    "_ProfileImageUrl" as ProfileImageUrl,
    "_AccountId" as AccountId
FROM public.users_landing