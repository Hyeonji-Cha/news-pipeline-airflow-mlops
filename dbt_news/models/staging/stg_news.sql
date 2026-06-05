select
    id,
    title,
    content,
    published_at,
    cast(published_at as date) as published_date
from {{ source('raw', 'news') }}
