#ifndef MANAGE_SYSTEM_H
#define MANAGE_SYSTEM_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "../recordsystem/RecordSystem.h"
#include "../indexsystem/IndexSystem.h"

struct AttrInfo {
    std::string _attrName;//列名称
    AttrType _attrType;//列类型
    int _attrLength;//数据长度，单位：字节
    int _offset;//字段在record中的偏移
    bool _notNull, _hasDefault, _isPrimary;//基本属性
    void *_defaultValue;//默认值
};

struct TableInfo {
    std::string _tableName;//表名称
    int _attrNum;//列数量
    int _recordSize;//记录长度(含NULL位图)，单位：字节
    int _indexNum;//索引数量
    int _foreignKeyNum;//外键数量
    int _uniqueNum;//unique数量
    std::vector<AttrInfo> _attrs;//列
    std::vector<std::vector<std::string>> _indexes;//索引包含的列名称
    std::vector<std::string> _primaryKeys;//主键包含的列名称
    std::vector<std::string> _foreignKeyNames;//外键名称
    std::vector<std::vector<std::string>> _foreignKeys;//外键列表，每个元素记录外键包含的列名称
    std::vector<std::string> _references;//外键关联的表名
    std::vector<std::vector<string>> _uniques;//unique包含的列名称
};

class SystemManager {
private:
    BufPageManager *_bufPageManager;//缓存页面管理
    IndexManager *_indexManager;//索引管理
    RecordManager *_recordManager;//记录管理
    std::string _dbName;//当前使用的数据库名称
    std::vector<std::string> _dbNames;//所有数据库名称
    int _tableNum;//表数量
    std::vector<TableInfo> _tables;//表
    std::unordered_map<std::string, int> _tableName2fileID;//表名到文件标识符的映射
    bool checkForeignConstraint(const TableInfo &tableInfo, const TableInfo &refTableInfo, const std::vector<std::string> &foreignKey);//检查外键约束
public:
    SystemManager(BufPageManager *bufPageManager, IndexManager *indexManager, RecordManager *recordManager);
    ~SystemManager() {};
    int getTableIDByName(const std::string &tableName);//根据表名获得表id
    int getAttrIDByName(const TableInfo &tableInfo, const string &attrName);//根据列名获得列id
    int getFileIDByName(const std::string &tableName);//根据表名获得文件描述符
    const TableInfo &getTableInfoByID(int id);
    std::string getDBName();//获得当前数据库名称
    int getTableNum();//获得当前数据库表数量
    bool createDB(const std::string &dbName);//创建数据库
    bool dropDB(const std::string &dbName);//删除数据库
    bool openDB(const std::string &dbName);//打开数据库
    bool closeDB();//关闭数据库
    bool createTable(const TableInfo &tableInfo);//创建表，tableInfo应已完成初始化
    bool dropTable(const std::string &tableName);//删除表
    bool createIndex(const std::string &tableName, const std::vector<std::string> &attrNames);//创建索引
    bool dropIndex(const std::string &tableName, const std::vector<std::string> &attrNames);//删除索引
    bool createPrimary(const std::string &tableName, const std::vector<std::string> &attrNames);//创建主键
    bool dropPrimary(const std::string &tableName);//删除主键
    bool createForeign(const std::string &tableName, const std::string &foreignKeyName, const std::vector<std::string> &attrNames, const std::string &reference, const std::vector<std::string> &referenceKeys);//创建外键
    bool dropForeign(const std::string &tableName, const std::string &foreignKeyName);//删除外键
    bool createUnique(const std::string &tableName, const std::vector<std::string> &attrNames);//创建unique
    void show(const std::string &tableName);
    void showDBNames();//输出所有数据库名称
    void showTableNames();//输出当前数据库所有表名称
    void showIndexNames();//输出所有索引
};

#endif //MANAGE_SYSTEM_H
