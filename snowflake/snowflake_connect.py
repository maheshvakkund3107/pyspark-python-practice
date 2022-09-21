import snowflake.connector

if __name__ == '__main__':
    ctx = snowflake.connector.connect(
        user='maheshvakkund96',
        password='Mahesh1234',
        account='dg03778.us-east-2.aws'
    )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT * from call_center")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()



