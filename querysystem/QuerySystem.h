#ifndef QUERY_SYSTEM_H
#define QUERY_SYSTEM_H

#include <vector>
#include <functional>
#include "../recordsystem/RecordSystem.h"
#include "../indexsystem/IndexSystem.h"
#include "../managesystem/ManageSystem.h"

enum CompOp {
    NO_OP, EQ_OP, NE_OP, LT_OP, GT_OP, LE_OP, GE_OP, IS_NULL, IS_NOT_NULL
};

struct RelAttr {
    std::string _relName;
    std::string _attrName;
};

struct Value {
    AttrType _attrType;
    void* _data;
};

struct Condition {
    RelAttr _lhsAttr;
    CompOp _op;
    bool _rhsIsAttr;
    RelAttr _rhsAttr;
    Value _rhsValue;
    std::vector<Value> _rhsValues;
};

class QueryManager {
private:
    BufPageManager *_bufPageManager;//缓存页面管理
    IndexManager *_indexManager;//索引管理
    RecordManager *_recordManager;//记录管理
    SystemManager *_systemManager;//系统管理
    bool compareData(const char *data1, const char *data2, const CompOp &op, const AttrType &attrType);//比较数据关系
    std::string getKeyData(const TableInfo &tableInfo, const std::vector<Value> &values, const std::vector<std::string> &keys);//获得键数据
    std::string getKeyData(const TableInfo &tableInfo, const char *data, const std::vector<std::string> &keys);//获得键数据
    void updateKeyData(const TableInfo &tableInfo, const RID &rid, IndexHandle indexHandle, const char *data, const char *newData, const std::vector<std::string> &keys, bool isUnique);//更新键数据
    bool checkPrimaryConstraint(const TableInfo &tableInfo, const std::vector<Value> &values);//检查主键约束
    bool checkForeignConstraint(const TableInfo &tableInfo, const std::vector<Value> &values);//检查外键约束
    bool checkUniqueConstraint(const TableInfo &tableInfo, const std::vector<Value> &values);//检查唯一性约束
    bool checkConditions(const TableInfo &tableInfo, const std::vector<Condition> &conditions);//检查过滤条件是否合法
    void intersection(const std::vector<std::string> &attrs1, const std::vector<std::string> &attrs2, std::vector<std::string> &attrs);//求两个向量的交集
public:
    QueryManager(BufPageManager *bufPageManager, IndexManager *indexManager, RecordManager *recordManager, SystemManager *systemManager);
    ~QueryManager() {};
    bool filterTable(const TableInfo &tableInfo, const std::vector<Condition> &conditions, const std::function<bool(const RID &, const char *)> &callback);//根据条件筛选符合的数据，用传入的函数对象进行操作
    bool insertData(const std::string &tableName, const std::vector<std::vector<Value>> &value_list);//插入数据
    bool deleteData(const std::string &tableName, const std::vector<Condition> &conditions);//删除数据
    bool updateData(const std::string &tableName, const std::vector<RelAttr> &relAttrs, const std::vector<Value> &values, const std::vector<Condition> &conditions);//更新数据
    bool selectData(const std::vector<std::string> &tableNames, const std::vector<RelAttr> &relAttrs, const std::vector<Condition> &conditions, int limit, int offset);//查询数据
};

#endif //QUERY_SYSTEM_H
