import sqlite3


sql_transaction =[]
prodcution_connection = sqlite3.connect('wework19_02_19.sqlite3')
development_connection = sqlite3.connect('weworkserver.sqlite3')
# new_connection = sqlite3.connect('wework_15_02_19.sqlite3')
p=prodcution_connection.cursor()
d=development_connection.cursor()


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    print(len(sql_transaction))
    print(len(sql_transaction))
    if len(sql_transaction) >= 1:
        d.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                d.execute(s)
                # print(d.fetchone())
            except:
                print("fail")
                pass
        development_connection.commit()
        sql_transaction = []


def sql_insert(id, email, phoneno, firstname, lastname, company, industrial, city_id, building_id,
               leadsource, leadsourcedetail, utm_campaign, utm_content, updated_date,
               updated_by_id, status, moveindate, noofpeople, countrycode, utm_medium, utm_term, leadformid, referal,created_date):
    try:
        # sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        sql = """INSERT INTO Customers 
        (id,email,phoneno,firstname,lastname,company,industrial,city_id,building_id,leadsource,leadsourcedetail,utm_campaign,utm_content,updated_date,updated_by_id,status,moveindate,
        noofpeople,countrycode,utm_medium,utm_term,leadformid,referal,created_date) 
        VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}");""".\
            format(id,email,phoneno,firstname,lastname,company,industrial,city_id,building_id,leadsource,leadsourcedetail,utm_campaign,utm_content,updated_date,
                   updated_by_id,status,moveindate,noofpeople,countrycode,utm_medium,utm_term,leadformid,referal,created_date)
        print(sql)
        transaction_bldr(sql)
    except Exception as e:
        print("FAILED")
        print('s0 insertion',str(e))





sql = """SELECT id,
email,
phoneno,
firstname,
lastname,
company,
industrial,
city_id,
building_id,
leadsource,
leadsourcedetail,
utm_campaign,
utm_content,
updated_date,
updated_by_id,
status,
moveindate,
noofpeople,
countrycode,
utm_medium,
utm_term,
leadformid,
referal,
created_date
FROM Customers """
sqldel = "DELETE  FROM Customers "
# d.execute(sqldel)

#================================= Delete all the existing data ==================================>>>>>>>>>>>>>>>>>>
transaction_bldr(sqldel)
#===================================================================>>>>>>>>>>>>>>>>>>

# d.execute(sql)


p.execute(sql)
print(p.fetchone())
# transaction_bldr(sql)
# print(p.fetchmany(2))
print("fetche Starting",d.fetchone())
c=0
for row in p.fetchall():
    id= row[0]
    email=row[1]
    phoneno=row[2]
    firstname=row[3]
    lastname=row[4]
    company=row[5]
    industrial=row[6]
    city_id=row[7]
    building_id=row[8]
    leadsource=row[9]
    leadsourcedetail=row[10]
    utm_campaign=row[11]
    utm_content=row[12]
    updated_date=row[13]
    updated_by_id=row[14]
    status=row[15]
    moveindate=row[16]
    noofpeople=row[17]
    countrycode=row[18]
    utm_medium=row[19]
    utm_term=row[20]
    leadformid=row[21]
    referal=row[22]
    created_date = row[23]
    # print(created_date)

    #
    sql_insert(id, email, phoneno, firstname, lastname, company, industrial, city_id, building_id,
               leadsource, leadsourcedetail, utm_campaign, utm_content, updated_date,
               updated_by_id, status, moveindate, noofpeople, countrycode, utm_medium, utm_term, leadformid, referal,created_date)

d.execute(sql)
print("fetche End",d.fetchone())