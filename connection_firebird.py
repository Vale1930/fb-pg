import fdb


def conectar():
    """
    Devuelve una conexión activa a la base de datos Firebird.
    """
    try:
        con = fdb.connect(
            host="127.0.0.1",
            database="/firebird/data/PDVDATA_LAST.FDB",
            user="sysdba",
            password="masterkey",
            port=3050,
            charset="ISO8859_1",
        )
        return con
    except (Exception, IOError) as error:
        print("Error al conectar a la base de datos:")
        print(str(error))
        return None


# Este bloque es solo para pruebas rápidas desde este archivo
if __name__ == "__main__":
    con = conectar()
    if con:
        cur = con.cursor()
        cur.execute('SELECT * FROM "CLIENTES"')
        for row in cur:
            print(row)
        cur.close()
        con.close()
        print("Conexión y lectura de CLIENTES finalizada.")
