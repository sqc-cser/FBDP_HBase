import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.*;
import java.io.IOException;

public class hbase{
    // 进行连接
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    private static ResultScanner scanner;
    static {
        //配置Configuration
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //获得admin
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("creating student table...");
        String  familyNames[]={"info","course1","course2","course3"};
        createTable("student",familyNames);
        // set infomation
        insert("student","2015001","info","S_Name","Li Lei");
        insert("student","2015001","info","S_Sex","male");
        insert("student","2015001","info","S_Age","23");
        insert("student","2015002","info","S_Name","Han Meimei");
        insert("student","2015002","info","S_Sex","female");
        insert("student","2015002","info","S_Age","22");
        insert("student","2015003","info","S_Name","Zhang San");
        insert("student","2015003","info","S_Sex","male");
        insert("student","2015003","info","S_Age","24");
        // set courses
        insert("student","2015001","course1","C_No","123001");
        insert("student","2015001","course1","C_Name","Math");
        insert("student","2015001","course1","C_Credit","2.0");
        insert("student","2015001","course1","C_Score","86");
        insert("student","2015001","course3","C_No","123003");
        insert("student","2015001","course3","C_Name","English");
        insert("student","2015001","course3","C_Credit","3.0");
        insert("student","2015001","course3","C_Score","69");
        insert("student","2015002","course2","C_No","123002");
        insert("student","2015002","course2","C_Name","Computer Science");
        insert("student","2015002","course2","C_Credit","5.0");
        insert("student","2015002","course2","C_Score","77");
        insert("student","2015002","course3","C_No","123003");
        insert("student","2015002","course3","C_Name","English");
        insert("student","2015002","course3","C_Credit","3.0");
        insert("student","2015002","course3","C_Score","99");
        insert("student","2015003","course1","C_No","123001");
        insert("student","2015003","course1","C_Name","Math");
        insert("student","2015003","course1","C_Credit","2.0");
        insert("student","2015003","course1","C_Score","98");
        insert("student","2015003","course2","C_No","123003");
        insert("student","2015003","course2","C_Name","Computer Science");
        insert("student","2015003","course2","C_Credit","5.0");
        insert("student","2015003","course2","C_Score","95");
        System.out.println("end creating !");
        System.out.println("searching scores of course Computer Science ...");
        scanByColumnKey("student","course2","C_Score");
        System.out.println("end searching !");
        System.out.println("add new column family Contact:Email");
        addColumnFamily("student","Contact");
        insert("student","2015001","Contact","Email","lilei@qq.com");
        insert("student","2015002","Contact","Email","hmm@qq.com");
        insert("student","2015003","Contact","Email","zs@qq.com");
        System.out.println("end adding !");
        System.out.println("deleting courses of student 2015003...");
        deleteColumn("student","2015003","course1","C_No");
        deleteColumn("student","2015003","course1","C_Name");
        deleteColumn("student","2015003","course1","C_Credit");
        deleteColumn("student","2015003","course1","C_Score");
        deleteColumn("student","2015003","course2","C_No");
        deleteColumn("student","2015003","course2","C_Name");
        deleteColumn("student","2015003","course2","C_Credit");
        deleteColumn("student","2015003","course2","C_Score");
        deleteColumn("student","2015003","course3","C_No");
        deleteColumn("student","2015003","course3","C_Name");
        deleteColumn("student","2015003","course3","C_Credit");
        deleteColumn("student","2015003","course3","C_Score");
        System.out.println("end deleting ");
        System.out.println("deleting the total table...");
        dropTable("student");
        System.out.println("end deleting the total table");
    }
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[]) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table exists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor);
        System.out.println("createtable success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("deletetable " + tableName + " ok.");
    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     * TODO: 批量PUT
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
    }

    public static void deleteColumn(String tableName,String rowKey,String familyName,String columnName)
            throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println("delete "+rowKey+":"+familyName+":"+columnName+" "+"susscess");
    }

    public static void addColumnFamily(String tableName,String columnName)throws IOException {
        TableName tableName1 = TableName.valueOf(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnName);
        admin.addColumn(tableName1,hColumnDescriptor);
        System.out.println("add column family "+columnName+" ok");
    }


    public static void scanByColumnKey(String tableName,String family,String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 通过列键（family:qualifier）创建扫描器，得到基于列键扫描的数据
        scanner = table.getScanner(Bytes.toBytes(family),Bytes.toBytes(qualifier));
        printScanResults();
    }

    // 格式化打印扫描到的数据
    private static void printScanResults() {
        for (Result row : scanner) {
            System.out.println(row);
            for (Cell cell : row.listCells()) {
                System.out.println(
                        "RowKey:"
                                + Bytes.toString(row.getRow())
                                + " Family:"
                                + Bytes.toString(CellUtil.cloneFamily(cell))
                                + " Qualifier:"
                                + Bytes.toString(CellUtil.cloneQualifier(cell))
                                + " Value:"
                                + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}