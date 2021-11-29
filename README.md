# 金融大数据实验3

姓名：盛祺晨			学号：191220093

[TOC]

## 环境配置

Mac本机配置环境会出现RegionServer不存在的情况，我更换了hadoop和HBase的版本号、使用外部zookeeper都不能解决，最终采用了申请BDKit的方法，非常有效。

对于在本地配置中出现的问题，将在问题部分描述。

拉取Hadoop3.1.4+Hbase2.2.6的镜像。并能正确进去shell编程。

<img src="/Users/shengqichen/Library/Application Support/typora-user-images/image-20211128152159180.png" alt="image-20211128152159180" style="zoom:50%;" />

## 设计表格

![image-20211128143735523](/Users/shengqichen/Library/Application Support/typora-user-images/image-20211128143735523.png)

## SHELL

### 􏰿􏱀􏰈􏰓􏱁􏰼􏰻􏰐􏰌设计合适的表

```shell
> create 'student','S_No','info','course1','course2','course3'
# 基础信息
> put 'student','2015001','info:S_Name','Li Lei'
> put 'student','2015001','info:S_Sex','male'
> put 'student','2015001','info:S_Age','23'

> put 'student','2015002','info:S_Name','Han Meimei'
> put 'student','2015002','info:S_Sex','female'
> put 'student','2015002','info:S_Age','22'

> put 'student','2015003','info:S_Name','Zhang San'
> put 'student','2015003','info:S_Sex','male'
> put 'student','2015003','info:S_Age','24'
# 课程信息
# student 2015001
> put 'student','2015001','course1:C_No','123001'
> put 'student','2015001','course1:C_Name','Math'
> put 'student','2015001','course1:C_Credit','2.0'
> put 'student','2015001','course1:C_Score','86'

> put 'student','2015001','course3:C_No','123003'
> put 'student','2015001','course3:C_Name','English'
> put 'student','2015001','course3:C_Credit','3.0'
> put 'student','2015001','course3:C_Score','69'
# student 2015002
> put 'student','2015002','course2:C_No','123002'
> put 'student','2015002','course2:C_Name','Computer Science'
> put 'student','2015002','course2:C_Credit','5.0'
> put 'student','2015002','course2:C_Score','77'

> put 'student','2015002','course3:C_No','123003'
> put 'student','2015002','course3:C_Name','English'
> put 'student','2015002','course3:C_Credit','3.0'
> put 'student','2015002','course3:C_Score','99'
# student 2015003
> put 'student','2015003','course1:C_No','123001'
> put 'student','2015003','course1:C_Name','Math'
> put 'student','2015003','course1:C_Credit','2.0'
> put 'student','2015003','course1:C_Score','98'

> put 'student','2015003','course2:C_No','123002'
> put 'student','2015003','course2:C_Name','Computer Science'
> put 'student','2015003','course2:C_Credit','5.0'
> put 'student','2015003','course2:C_Score','95'

```

### 查询选修Computer Science的学生的成绩

```shell
hbase(main):045:0> scan 'student',{COLUMN=>'course2:C_Score'}
ROW                            COLUMN+CELL                                                                          
 2015002                       column=course2:C_Score, timestamp=1638083414463, value=77                            
 2015003                       column=course2:C_Score, timestamp=1638083712088, value=95                            
2 row(s)
Took 0.0275 seconds  
```

### 增加新的列族和新列Contact:Email,并添加数据

```shell
#联系方式
> alter 'student','Contact'
> put 'student','2015001','Contact:Email','lilei@qq.com'
> put 'student','2015002','Contact:Email','hmm@qq.com'
> put 'student','2015003','Contact:Email','zs@qq.com'
```

<img src="/Users/shengqichen/Library/Application Support/typora-user-images/image-20211128151629308.png" alt="image-20211128151629308" style="zoom:50%;" />

### 删除学号为2015003的学生的选课记录

```shell
# student 2015003
> delete 'student','2015003','course1:C_No'
> delete 'student','2015003','course1:C_Name'
> delete 'student','2015003','course1:C_Credit'
> delete 'student','2015003','course1:C_Score'

> delete 'student','2015003','course3:C_No'
> delete 'student','2015003','course3:C_Name'
> delete 'student','2015003','course3:C_Credit'
> delete 'student','2015003','course3:C_Score'

> get 'student','2015003' # 查询剩下的info
COLUMN                         CELL                                                                                 
 Contact:Email                 timestamp=1638083767501, value=zs@qq.com                                             
 course2:C_Credit              timestamp=1638083707487, value=5.0                                                   
 course2:C_Name                timestamp=1638083702874, value=Computer Science                                      
 course2:C_No                  timestamp=1638083698085, value=123002                                                
 course2:C_Score               timestamp=1638083712088, value=95                                                    
 info:S_Age                    timestamp=1638083345604, value=24                                                    
 info:S_Name                   timestamp=1638083335992, value=Zhang San                                             
 info:S_Sex                    timestamp=1638083340980, value=male                                                  
1 row(s)
Took 0.0787 seconds             
```

###  删除所创建的表

```shell
> disable 'student'
> drop 'student'
> list
TABLE                                                                                                               
0 row(s)
Took 0.0278 seconds                                                                                                 
=> []
```

<img src="/Users/shengqichen/Library/Application Support/typora-user-images/image-20211128152008834.png" alt="image-20211128152008834" style="zoom:50%;" />

## JAVA

将程序打包后上传到BDKit后，在BDKit中运行

```shell
hadoop jar Hbase-1.0-SNAPSHOT.jar Hbase
```

### 运行结果

结果如下

![image-20211128170129408](/Users/shengqichen/Library/Application Support/typora-user-images/image-20211128170129408.png)

和上述shell中的思想一样，先连接habse

```java
//配置Configuration
configuration = HBaseConfiguration.create();
configuration.set("hbase.zookeeper.quorum","localhost");
configuration.set("hbase.zookeeper.property.clientPort", "2181");

connection = ConnectionFactory.createConnection(configuration);
admin = connection.getAdmin();
```

### 设计并创建合适的表

```java
HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
for (String familyName : familyNames) {
		tableDescriptor.addFamily(new HColumnDescriptor(familyName));// 增加列族
}
admin.createTable(tableDescriptor);//创建表
```

### 插入数据

```java
Table table =connection.getTable(TableName.valueOf(tableName)); // 获取table
Put put = new Put(Bytes.toBytes(rowKey)); // 定位rowkey
put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value)); // 增加列族，列，值
table.put(put);
```

### 查询选修Computer Science的学生的成绩

```java
Table table = connection.getTable(TableName.valueOf(tableName)); //获取表的名字
scanner = table.getScanner(Bytes.toBytes(family),Bytes.toBytes(qualifier));//根据列族：列名来定位
printScanResults(); //打印结果
```

### 增加新的列族和新列Contact:Email,并添加数据

```java
TableName tableName1 = TableName.valueOf(tableName); //获取表名
HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnName); 
admin.addColumn(tableName1,hColumnDescriptor); //增加表名：列族名
```

### 删除学号为2015003的学生的选课记录

```java
Table table = connection.getTable(TableName.valueOf(tableName)); // 获取表名
Delete deleteColumn = new Delete(Bytes.toBytes(rowKey)); // 找到rowkey
deleteColumn.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName)); // 要删除的column增加列族名：列名
table.delete(deleteColumn); // 删除
```

### 删除所创建的表

```java
admin.disableTable(TableName.valueOf(tableName)); // 将表禁用
admin.deleteTable(TableName.valueOf(tableName)); // 将表删除
```

## 问题描述

在后面使用了BDKit之后基本没有问题产生

在本地配置hbase环境时，出现了以下问题：

1.stop-hbase.sh 一直处于等待状态

参考文章：https://blog.csdn.net/weixin_45462732/article/details/106909501

输入：hbase-daemon.sh stop master关闭master，就可以stop-hbase.sh正常关闭hbase

2.jps中出现Hmaster和HRegionServer，但是点击localhost:16010网站后，发现并没有连接成功，于是采用外部zookeeper解决。

![image-20211129150256715](/Users/shengqichen/Library/Application Support/typora-user-images/image-20211129150256715.png)

参考文章：https://www.cxybb.com/article/dc666/82866442

3.进程中有Hmaster和HRegionServer，点击localhost:16010网站后，能连接成功，但是

![image-20211129150512033](/Users/shengqichen/Library/Application Support/typora-user-images/image-20211129150512033.png)

发现RegionServer并没有run起来，不能解决，于是采用BDkit。
