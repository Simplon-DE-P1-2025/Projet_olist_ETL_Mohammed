--Chiffre d'Affaires par période
SELECT 
    DATE_TRUNC('day', o.order_purchase_timestamp) AS jour,
    DATE_TRUNC('month', o.order_purchase_timestamp) AS mois,
    EXTRACT(YEAR FROM o.order_purchase_timestamp) AS annee,
    SUM(oi.price) AS ca_total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1, 2, 3
ORDER BY 1 DESC;


--Évolution CA vs N-1 (Comparaison mensuelle) 
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_purchase_timestamp) AS mois,
        SUM(price) AS ca
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY 1
)
SELECT 
    mois,
    ca AS ca_actuel,
    LAG(ca) OVER (ORDER BY mois) AS ca_mois_precedent,
    (ca - LAG(ca) OVER (ORDER BY mois)) / LAG(ca) OVER (ORDER BY mois) * 100 AS evolution_pourcentage
FROM monthly_sales;


--Top 10 Produits
SELECT 
    product_id,
    COUNT(*) AS nb_ventes,
    SUM(price) AS revenu_genere
FROM order_items
GROUP BY 1
ORDER BY 3 DESC
LIMIT 10;

--2. Clients (Comportement & Segmentation)
--Nouveaux clients vs Récurrents 
WITH client_orders AS (
    SELECT 
        c.customer_unique_id,
        COUNT(o.order_id) AS nb_commandes
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 1
)
SELECT 
    CASE WHEN nb_commandes = 1 THEN 'Nouveau' ELSE 'Récurrent' END AS type_client,
    COUNT(*) AS total_clients
FROM client_orders
GROUP BY 1;


--Panier Moyen (AOV) 
SELECT 
    SUM(price) / COUNT(DISTINCT order_id) AS panier_moyen
FROM order_items;

--Analyse RFM (Récence, Fréquence, Montant) 
SELECT 
    c.customer_unique_id,
    MAX(o.order_purchase_timestamp) AS derniere_achat,
    COUNT(o.order_id) AS frequence,
    SUM(oi.price) AS montant_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY 1;

--3. Cohortes (Rétention & LTV)
--Rétention par mois de première commande 
WITH first_purchase AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS mois_cohorte
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 1
),
subsequent_purchases AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS mois_achat
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
)
SELECT 
    fp.mois_cohorte,
    sp.mois_achat,
    COUNT(DISTINCT fp.customer_unique_id) AS nb_clients
FROM first_purchase fp
JOIN subsequent_purchases sp ON fp.customer_unique_id = sp.customer_unique_id
GROUP BY 1, 2
ORDER BY 1, 2;

--LTV (Lifetime Value) par cohorte
WITH cohorte_data AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS mois_cohorte
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 1
)
SELECT 
    cd.mois_cohorte,
    SUM(oi.price) / COUNT(DISTINCT cd.customer_unique_id) AS ltv_moyen
FROM cohorte_data cd
JOIN customers c ON cd.customer_unique_id = c.customer_unique_id
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY 1
ORDER BY 1;

