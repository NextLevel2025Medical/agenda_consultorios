import sqlite3

con = sqlite3.connect("agenda.db")  # ajuste se o nome do arquivo for outro
cur = con.cursor()

cur.execute("ALTER TABLE surgicalmapentry ADD COLUMN time_hhmm TEXT")
con.commit()
con.close()

print("OK: coluna time_hhmm criada.")
