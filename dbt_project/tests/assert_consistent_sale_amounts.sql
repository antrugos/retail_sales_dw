-- Test de negocio: una cantidad negativa representa una devolución, pero el
-- monto total debe ser consistente con esa cantidad (no debe haber montos
-- positivos absurdos en filas marcadas como devolución, ni cantidades en cero).
-- dbt considera el test fallido si esta query retorna alguna fila.
 
select *
from {{ ref('fct_sales') }}
where quantity = 0
   or (quantity < 0 and total_amount > 0)
   or (quantity > 0 and total_amount < 0)