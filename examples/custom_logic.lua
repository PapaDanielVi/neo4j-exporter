-- Example Lua script: compute sales volume by payment method
local records = neo4j.query([[
  MATCH (o:Order)
  WHERE o.created_at > timestamp() - 60000
  RETURN o.payment_method, sum(o.amount) as total
]])

for _, row in ipairs(records) do
    prometheus_record_gauge("neo4j_sales_volume_bytes", row["total"], {
        method = row["payment_method"]
    })
end
