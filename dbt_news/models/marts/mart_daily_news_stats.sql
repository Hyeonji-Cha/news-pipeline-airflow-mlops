select
    published_date,
    count(*) as news_count
from {{ ref('stg_news') }}
group by published_date
order by published_date
