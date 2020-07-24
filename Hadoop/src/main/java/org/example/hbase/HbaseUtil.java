package org.example.hbase;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseUtil {
    public static Configuration conf;
    public static Connection con;
    private static long counter = 0;

    //init
    static {
//        System.setProperty("HADOOP_USER_NAME","hbase");
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://192.168.1.131:8020/apps/hbase/data");
        conf.set("hbase.zookeeper.quorum", "192.168.1.131,192.168.1.132,192.168.1.166");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");


        try {
            con = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection GetCon() {
        if (con == null || con.isClosed())
            try {
                con = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        return con;
    }

    public static void Close() {
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void CreateTable(String tableName, String... FamilyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = GetCon().getAdmin();
            //判断表名是否存在，如果存在则跳出本次操作
            if (admin.tableExists(tn))
                return;
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
            for (String s : FamilyColumn) {
                ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(s));
                tableDescriptorBuilder.setColumnFamily(hcd.build());
            }
            tableDescriptorBuilder.build();

            admin.createTable(tableDescriptorBuilder.build());
            admin.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void DropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);

        try {
            Admin admin = GetCon().getAdmin();
            admin.disableTable(tn);
            admin.deleteTable(tn);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean Insert(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Table t = GetCon().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            t.put(put);
            t.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Close();
        }
        return false;
    }

    public static boolean Delete(String tableName, String rowKey, String family, String qualifier) {
        try {
            Table t = GetCon().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (qualifier != null)
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            else if (family != null)
                del.addFamily(Bytes.toBytes(family));
            t.delete(del);
            return true;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Close();
        }
        return false;
    }

    public static boolean Delete(String tableName, String rowKey) {
        return Delete(tableName, rowKey, null, null);
    }

    /**
     * 获取单元格式数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     */
    public static String Get(String tableName, String rowKey, String family, String qualifier) {
        try {
            Table t = GetCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

            Result r = t.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取列族数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     */
    public static Map<String, String> Get(String tableName, String rowKey, String family) {
        Map<String, String> result = null;

        try {
            Table t = GetCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();

            result = cs.size() > 0 ? new HashMap<>() : null;
            for (Cell c : cs) {
                result.put(Bytes.toString(CellUtil.cloneQualifier(c)), Bytes.toString(CellUtil.cloneValue(c)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Map<String, Map<String, String>> Get(String tableName, String rowKey) {
        Map<String, Map<String, String>> results = null;
        try {
            Table t = GetCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            results = cs.size() > 0 ? new HashMap<>() : null;

            for (Cell cell : cs) {
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                if (results.get(familyName) == null) {
                    results.put(familyName, new HashMap<>());
                }
                results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /**
     * 通过表名查找数据，扫描表
     *
     * @param tableName
     * @return
     */
    public static List<String> Scan(String tableName) {
        try {
            Admin admin = GetCon().getAdmin();
            List<String> list = new ArrayList<>();
            TableName tn = TableName.valueOf(tableName);
            if (!admin.tableExists(tn)) {
                return list;
            }
            Table t = GetCon().getTable(tn);

            Scan scan = new Scan();

            ResultScanner scanner = t.getScanner(scan);

            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    list.add(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            scanner.close();
            admin.close();
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static List<String> ScanScope(String tableName, String start, String stop) {

        try {
            Admin admin = GetCon().getAdmin();
            List<String> list = new ArrayList<>();
            TableName tn = TableName.valueOf(tableName);
            if (!admin.tableExists(tn)) {
                return list;
            }
            Table t = GetCon().getTable(tn);
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(start));
            scan.withStopRow(Bytes.toBytes(stop));
            scan.setBatch(1000);

            ResultScanner scanner = t.getScanner(scan);

            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    list.add(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            scanner.close();
            admin.close();
            return list;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
