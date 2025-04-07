from connection_postgres import conn

cursor = conn.cursor()

# 1. Ventas Diarias
cursor.execute("DROP TABLE IF EXISTS ventas_diarias;")
cursor.execute(
    """
    CREATE TABLE ventas_diarias AS
    SELECT date_trunc('day', "PAGADO_EN") AS fecha,
           SUM("TOTAL") AS ventas_totales
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY fecha
    ORDER BY fecha;
"""
)
conn.commit()
print("Tabla ventas_diarias creada y datos insertados.")

# 2. Ventas Semanales
cursor.execute("DROP TABLE IF EXISTS ventas_semanales;")
cursor.execute(
    """
    CREATE TABLE ventas_semanales AS
    SELECT date_trunc('week', "PAGADO_EN") AS semana,
           SUM("TOTAL") AS ventas_totales
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY semana
    ORDER BY semana;
"""
)
conn.commit()
print("Tabla ventas_semanales creada y datos insertados.")

# 3. Ventas Mensuales
cursor.execute("DROP TABLE IF EXISTS ventas_mensuales;")
cursor.execute(
    """
    CREATE TABLE ventas_mensuales AS
    SELECT date_trunc('month', "PAGADO_EN") AS mes,
           SUM("TOTAL") AS ventas_totales
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY mes
    ORDER BY mes;
"""
)
conn.commit()
print("Tabla ventas_mensuales creada y datos insertados.")

# 4. Ventas Anuales
cursor.execute("DROP TABLE IF EXISTS ventas_anuales;")
cursor.execute(
    """
    CREATE TABLE ventas_anuales AS
    SELECT date_trunc('year', "PAGADO_EN") AS anio,
           SUM("TOTAL") AS ventas_totales
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY anio
    ORDER BY anio;
"""
)
conn.commit()
print("Tabla ventas_anuales creada y datos insertados.")

# 5. Productos Más Vendidos por Mes
cursor.execute("DROP TABLE IF EXISTS productos_mas_vendidos;")
cursor.execute(
    """
    CREATE TABLE productos_mas_vendidos AS
    SELECT date_trunc('month', "PAGADO_EN") AS mes,
           "PRODUCTO_NOMBRE",
           SUM("CANTIDAD") AS total_ventas
    FROM ventatickets_articulos_clean
    WHERE "FUE_DEVUELTO" = 'f'
    GROUP BY mes, "PRODUCTO_NOMBRE"
    ORDER BY mes, total_ventas DESC;
"""
)
conn.commit()
print("Tabla productos_mas_vendidos creada y datos insertados.")

# 6. Ventas por Departamento (Movimiento)
cursor.execute("DROP TABLE IF EXISTS ventas_por_departamento;")
cursor.execute(
    """
    CREATE TABLE ventas_por_departamento AS
    SELECT d."NOMBRE" AS nombre,
           SUM(vta."CANTIDAD" * vta."PRECIO_USADO") AS ventas_totales
    FROM ventatickets_articulos_clean vta
    JOIN departamentos_clean d ON vta."DEPARTAMENTO_ID" = d."ID"
    WHERE vta."FUE_DEVUELTO" = 'f'
    GROUP BY d."NOMBRE"
    ORDER BY ventas_totales DESC;
"""
)
conn.commit()
print("Tabla ventas_por_departamento creada y datos insertados.")

# 7. Ventas por Hora (Horas Pico)
cursor.execute("DROP TABLE IF EXISTS ventas_por_hora;")
cursor.execute(
    """
    CREATE TABLE ventas_por_hora AS
    SELECT EXTRACT(hour FROM "PAGADO_EN") AS hora,
           COUNT(*) AS cantidad_ventas
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY hora
    ORDER BY hora;
"""
)
conn.commit()
print("Tabla ventas_por_hora creada y datos insertados.")

# 8. Ventas por Día de la Semana
cursor.execute("DROP TABLE IF EXISTS ventas_por_dia_semana;")
cursor.execute(
    """
    CREATE TABLE ventas_por_dia_semana AS
    SELECT EXTRACT(dow FROM "PAGADO_EN") AS dia_semana,
           COUNT(*) AS cantidad_ventas
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY dia_semana
    ORDER BY dia_semana;
"""
)
conn.commit()
print("Tabla ventas_por_dia_semana creada y datos insertados.")

# 9. Evolución de Ventas Acumuladas (Sobre ventas_diarias)
# Nota: La suma acumulada se puede calcular en una consulta posterior,
# pero aquí se muestra cómo crear la tabla con los datos diarios y luego
# hacer la acumulación desde Python si se desea.
cursor.execute("DROP TABLE IF EXISTS ventas_diarias_acumuladas;")
cursor.execute(
    """
    CREATE TABLE ventas_diarias_acumuladas AS
    SELECT fecha, ventas_totales
    FROM ventas_diarias;
"""
)
conn.commit()
print("Tabla ventas_diarias_acumuladas creada y datos insertados.")

# 10. Comparación de Períodos (Mes a Mes, Año a Año)
cursor.execute("DROP TABLE IF EXISTS comparacion_periodos;")
cursor.execute(
    """
    CREATE TABLE comparacion_periodos AS
    SELECT date_trunc('month', "PAGADO_EN") AS mes,
           EXTRACT(year FROM "PAGADO_EN") AS anio,
           SUM("TOTAL") AS ventas
    FROM ventatickets_clean
    WHERE "ESTA_ABIERTO" = 'f' AND "ESTA_CANCELADO" = 'f'
    GROUP BY mes, anio
    ORDER BY mes, anio;
"""
)
conn.commit()
print("Tabla comparacion_periodos creada y datos insertados.")

# 11. Variación Porcentual para Detectar Crecimientos o Caídas
# La variación se calcula comparando períodos. Para ello, se puede crear una tabla
# adicional con este cálculo usando funciones de ventana o hacerlo desde Python.
cursor.execute("DROP TABLE IF EXISTS variacion_ventas;")
cursor.execute(
    """
    CREATE TABLE variacion_ventas AS
    SELECT mes, anio, ventas,
           LAG(ventas) OVER (PARTITION BY anio ORDER BY mes) AS ventas_periodo_anterior,
           CASE
             WHEN LAG(ventas) OVER (PARTITION BY anio ORDER BY mes) > 0
             THEN ((ventas - LAG(ventas) OVER (PARTITION BY anio ORDER BY mes)) / LAG(ventas) OVER (PARTITION BY anio ORDER BY mes)) * 100
             ELSE NULL
           END AS variacion_pct
    FROM comparacion_periodos;
"""
)
conn.commit()
print("Tabla variacion_ventas creada y datos insertados.")

# Cerrar el cursor y la conexión
cursor.close()
conn.close()
