SELECT supplier_id
     , rating
     , product_id
     , COUNT(*)
FROM (VALUES ('supplier1', 'product1', 4),
             ('supplier1', 'product2', 3),
             ('supplier2', 'product3', 3),
             ('supplier2', 'product4', 4))
-- 供应商id、产品id、评级
         AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SETS ( (supplier_id, product_id, rating),
                         (supplier_id, product_id),
                         (supplier_id, rating),
                         (supplier_id),
                         (product_id, rating),
                         (product_id),
                         (rating),
                         ()
    );