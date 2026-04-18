select
    a.id,
    a.val as a_val,
    b.val as b_val
from {{ ref('int_a') }} a
join {{ ref('int_b') }} b on a.id != b.id
