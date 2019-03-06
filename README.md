# Navidog

Python连接Hive，Mysql，Presto数据库，集成pandas。让数据分析更便捷，轻松。

## 连接数据源

- 连接mysql

-----
    from Navidog.api import *
    mysql = Mysql(host='127.0.0.1', username='root', password='root', database='oozie')
    
- 连接hive

-----
    from Navidog.api import *
    hive = Hive('127.0.0.1', database='all_users_db', port=10001, username='admin')
    

- 连接presto

-----
    from Navidog.api import *
    prestos = Presto(host='127.0.0.1',
                     port=8081,
                     catalog='hive',
                     username='admin',
                     schema='table') 
                     
                     
   
## 增删改查
 
### 以mysql为例

- 增

-----
    from Navidog.api import *
    mysql = Mysql(host='127.0.0.1', username='root', password='root', database='oozie')
    items={'name': "dddd"}
    lastid = mysql.insert('tables', items=items)
    #lastid = mysql.insert('tables', name="dddd") #返回自增id
    
    
- 删

-----
    from Navidog.api import *
    mysql = Mysql(host='127.0.0.1', username='root', password='root', database='oozie')
    rows = mysql.delete("tables", where="id=1") #返回影响行数
    
    
- 改

-----
    from Navidog.api import *
    mysql = Mysql(host='127.0.0.1', username='root', password='root', database='oozie')
    rows = mysql.update('tables', where='id=2', name="sssss") #返回影响行数
    
- 查

-----
    from Navidog.api import *
    mysql = Mysql(host='127.0.0.1', username='root', password='root', database='oozie')
    data = mysql.select("tables", limit=10, use_pandas=1)
    
    print(type(data))
    <class 'pandas.core.frame.DataFrame'>

    print(data)
       id   name
    0   2  sssss
    1   3   dddd
    2   5   dddd
    3   6   dddd
    4   7   dddd
 
    
    
    
    


