#include "QuerySystem.h"
#include <cstring>
#include <cmath>
#include <iomanip>

using namespace std;

QueryManager::QueryManager(BufPageManager *bufPageManager, IndexManager *indexManager, RecordManager *recordManager, SystemManager *systemManager) {
    _bufPageManager = bufPageManager;
    _indexManager = indexManager;
    _recordManager = recordManager;
    _systemManager = systemManager;
}

bool QueryManager::compareData(const char *data1, const char *data2, const CompOp &op, const AttrType &attrType) {
    switch (attrType) {
        case INTEGER:
            int int_a, int_b;
            memcpy(&int_a, data1, 4);
            memcpy(&int_b, data2, 4);
            switch (op) {
                case EQ_OP: return int_a == int_b;
                case NE_OP: return int_a != int_b;
                case LT_OP: return int_a < int_b;
                case LE_OP: return int_a <= int_b;
                case GT_OP: return int_a > int_b;
                case GE_OP: return int_a >= int_b;
                default: break;
            }
            break;
        case FLOAT:
            float float_a, float_b;
            memcpy(&float_a, data1, 4);
            memcpy(&float_b, data2, 4);
            switch (op) {
                case EQ_OP: return float_a == float_b;
                case NE_OP: return float_a != float_b;
                case LT_OP: return float_a < float_b;
                case LE_OP: return float_a <= float_b;
                case GT_OP: return float_a > float_b;
                case GE_OP: return float_a >= float_b;
                default: break;
            }
            break;
        case STRING:
            int result;
            result = strcmp(data1, data2);
            switch (op) {
                case EQ_OP: return result == 0;
                case NE_OP: return result != 0;
                case LT_OP: return result < 0;
                case LE_OP: return result <= 0;
                case GT_OP: return result > 0;
                case GE_OP: return result >= 0;
                default: break;
            }
            break;
        default:
            break;
    }
    cerr << "Invalid compare data!" << endl;
    return false;
}

string QueryManager::getKeyData(const TableInfo &tableInfo, const vector<Value> &values, const vector<string> &keys) {
    int keySize = 0;//键总大小，单位：字节
    auto iter = keys.begin();
    for (const auto &attr : tableInfo._attrs) {
        if (attr._attrName == *iter) {
            keySize += attr._attrLength;
            iter++;
            if (iter == keys.end()) break;
        }
    }
    string key(keySize, 0);
    int offset = 0;//数据偏移
    iter = keys.begin();
    for (int i = 0; i < tableInfo._attrs.size(); i++) {
        if (tableInfo._attrs[i]._attrName == *iter) {
            //拷贝到键值对应位置
            if (values[i]._attrType == STRING) {
                memcpy((void *) (key.c_str() + offset), values[i]._data, min(tableInfo._attrs[i]._attrLength - 1, (int) strlen((char *) values[i]._data)));
            } else {
                memcpy((void *) (key.c_str() + offset), values[i]._data, tableInfo._attrs[i]._attrLength);
            }
            offset += tableInfo._attrs[i]._attrLength;
            iter++;
            if (iter == keys.end()) break;
        }
    }
    return key;
}

string QueryManager::getKeyData(const TableInfo &tableInfo, const char *data, const vector<string> &keys) {
    int keySize = 0;//键总大小，单位：字节
    auto iter = keys.begin();
    for (const auto &attr : tableInfo._attrs) {
        if (attr._attrName == *iter) {
            keySize += attr._attrLength;
            iter++;
            if (iter == keys.end()) break;
        }
    }
    string key(keySize, 0);
    int offset = 0;//数据偏移
    iter = keys.begin();
    for (const auto &attr : tableInfo._attrs) {
        if (attr._attrName == *iter) {
            //拷贝到键值对应位置
            memcpy((void *) (key.c_str() + offset), data + attr._offset, attr._attrLength);
            offset += attr._attrLength;
            iter++;
            if (iter == keys.end()) break;
        }
    }
    return key;
}

void QueryManager::updateKeyData(const TableInfo &tableInfo, const RID &rid, IndexHandle indexHandle, const char *data, const char *newData, const vector<string> &keys, bool isUnique) {
    auto oldKey = getKeyData(tableInfo, data, keys);
    auto newKey = getKeyData(tableInfo, newData, keys);
    if (oldKey != newKey) {
        indexHandle.deleteEntry((BufType) oldKey.c_str(), rid);//删除原索引
        indexHandle.insertEntry((BufType) newKey.c_str(), rid, isUnique, false);//创建新索引
    }
}

bool QueryManager::checkPrimaryConstraint(const TableInfo &tableInfo, const vector<Value> &values) {
    if (!tableInfo._primaryKeys.empty()) {
        int fileID;
        //打开主键文件
        _indexManager->openIndex(tableInfo._tableName.c_str(), vector<string>(1, "primary"), fileID);
        IndexHandle indexHandle(_bufPageManager, fileID);
        RID rid(-1, -1);
        auto primary = getKeyData(tableInfo, values, tableInfo._primaryKeys);
        //检查是否重复，若重复则创建主键失败
        if (!indexHandle.insertEntry((BufType) primary.c_str(), rid, true, true)) {
            _indexManager->closeIndex(fileID);//关闭主键文件
            cerr << "Repetitive primary keys!" << endl;
            return false;
        }
        _indexManager->closeIndex(fileID);
    }
    return true;
}

bool QueryManager::checkForeignConstraint(const TableInfo &tableInfo, const vector<Value> &values) {
    for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
        const auto &reference = tableInfo._references[i];
        const auto &foreignKey = tableInfo._foreignKeys[i];
        int fileID;
        //打开参照表的主键文件
        _indexManager->openIndex(reference.c_str(), vector<string>(1, "primary"), fileID);
        IndexHandle indexHandle(_bufPageManager, fileID);
        RID rid(-1, -1);
        auto foreign = getKeyData(tableInfo, values, foreignKey);
        //检查主键文件是否插入成功，若成功说明外键值没有出现在参照表的主键中
        if (indexHandle.insertEntry((BufType) foreign.c_str(), rid, true, true)) {
            _indexManager->closeIndex(fileID);//关闭主键文件
            cerr << "Foreign key value is not in the reference table!" << endl;
            return false;
        }
        _indexManager->closeIndex(fileID);
    }
    return true;
}

bool QueryManager::checkUniqueConstraint(const TableInfo &tableInfo, const vector<Value> &values) {
    for (int i = 0; i < tableInfo._uniqueNum; i++) {
        int fileID;
        //打开unique文件
        vector<string> uniqueAttrNames = vector<string>(tableInfo._uniques[i]);
        uniqueAttrNames.emplace_back("unique");
        _indexManager->openIndex(tableInfo._tableName.c_str(), uniqueAttrNames, fileID);
        IndexHandle indexHandle(_bufPageManager, fileID);
        RID rid(-1, -1);
        auto unique = getKeyData(tableInfo, values, tableInfo._uniques[i]);
        //检查unique文件是否插入成功，若失败说明出现重复值
        if (!indexHandle.insertEntry((BufType) unique.c_str(), rid, true, true)) {
            _indexManager->closeIndex(fileID);//关闭unique文件
            cerr << "Unique columns have duplicated values!" << endl;
            return false;
        }
        _indexManager->closeIndex(fileID);
    }
    return true;
}

bool QueryManager::checkConditions(const TableInfo &tableInfo, const vector<Condition> &conditions) {
    for (const auto &condition : conditions) {
        int lhsAttrID = _systemManager->getAttrIDByName(tableInfo, condition._lhsAttr._attrName);
        //检查列是否存在
        if (lhsAttrID == -1) {
            cerr << "Column " + condition._lhsAttr._attrName + " does not exist!" << endl;
            return false;
        }
        if (condition._rhsIsAttr) {
            int rhsAttrID = _systemManager->getAttrIDByName(tableInfo, condition._rhsAttr._attrName);
            //检查列是否存在
            if (rhsAttrID == -1) {
                cerr << "Column " + condition._rhsAttr._attrName + " does not exist!" << endl;
                return false;
            }
            //检查左右类型是否匹配
            if (tableInfo._attrs[lhsAttrID]._attrType != tableInfo._attrs[rhsAttrID]._attrType) {
                cerr << "Column types do not match in filter condition!" << endl;
                return false;
            }
        } else if (condition._op != IS_NULL && condition._op != IS_NOT_NULL) {
            //检查左右类型是否匹配
            if (condition._rhsValues.empty()) {
                if (tableInfo._attrs[lhsAttrID]._attrType != condition._rhsValue._attrType) {
                    cerr << "Column type does not match value type in filter condition!" << endl;
                    return false;
                }
            } else {
                for (const auto &value : condition._rhsValues) {
                    if (tableInfo._attrs[lhsAttrID]._attrType != value._attrType && value._data != nullptr) {
                        cerr << "Column type does not match value type in filter condition!" << endl;
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

void QueryManager::intersection(const vector<string> &attrs1, const vector<string> &attrs2, vector<string> &attrs) {
    for (const auto &attr : attrs1) {
        if (find(attrs2.begin(), attrs2.end(), attr) != attrs2.end()) {
            attrs.push_back(attr);
        }
    }
}

bool QueryManager::filterTable(const TableInfo &tableInfo, const vector<Condition> &conditions, const function<bool(const RID &, const char *)> &callback) {
    int fileID, _fileID;
    //打开数据表文件
    _recordManager->openFile(tableInfo._tableName.c_str(), fileID);
    RecordHandle handle(_bufPageManager, fileID);
    RID rid;
    char *data = new char[tableInfo._recordSize];
    bool success = true;
    IndexHandle *indexHandle = nullptr;
    void *filterData;
    AttrType filterType;
    //检查是否可以使用索引加速
    for (const auto &condition : conditions) {
        if (condition._op == EQ_OP && !condition._rhsIsAttr) {
            vector<string> conditionKey(1, condition._lhsAttr._attrName);
            //普通索引
            if (find(tableInfo._indexes.begin(), tableInfo._indexes.end(), conditionKey) != tableInfo._indexes.end()) {
                _indexManager->openIndex(tableInfo._tableName.c_str(), conditionKey, _fileID);
                indexHandle = new IndexHandle(_bufPageManager, _fileID);
                filterData = condition._rhsValue._data;
                filterType = condition._rhsValue._attrType;
                break;
            }
            //主键索引
            if (tableInfo._primaryKeys == conditionKey) {
                _indexManager->openIndex(tableInfo._tableName.c_str(), vector<string>(1, "primary"), _fileID);
                indexHandle = new IndexHandle(_bufPageManager, _fileID);
                filterData = condition._rhsValue._data;
                filterType = condition._rhsValue._attrType;
                break;
            }
            //唯一索引
            if (find(tableInfo._uniques.begin(), tableInfo._uniques.end(), conditionKey) != tableInfo._uniques.end()) {
                conditionKey.emplace_back("unique");
                _indexManager->openIndex(tableInfo._tableName.c_str(), conditionKey, _fileID);
                indexHandle = new IndexHandle(_bufPageManager, _fileID);
                filterData = condition._rhsValue._data;
                filterType = condition._rhsValue._attrType;
                break;
            }
        }
    }
    bool hasNext;
    RID end(-1, -1);
    if (indexHandle != nullptr) {
        //先找到终止位置
        indexHandle->openScan((BufType) filterData, false);
        indexHandle->getNextEntry(end);
        //从等于的位置开始
        indexHandle->openScan((BufType) filterData, true);
        hasNext = indexHandle->getNextEntry(rid);
        if (hasNext) handle.getRecord(rid, (BufType) data);
    } else {
        //退化为普通情形
        handle.openScan();
        hasNext = handle.getNextRecord(rid, (BufType) data);
    }
    while (hasNext) {
        //到达终止位置，提前结束
        if (indexHandle != nullptr && rid == end) break;
        bool ok = true;
        for (const auto &condition : conditions) {
            int lhsAttrID = _systemManager->getAttrIDByName(tableInfo, condition._lhsAttr._attrName);
            const auto lhsAttr = tableInfo._attrs[lhsAttrID];
            if (condition._rhsIsAttr) {
                //情况1: 其它列
                int rhsAttrID = _systemManager->getAttrIDByName(tableInfo, condition._rhsAttr._attrName);
                const auto rhsAttr = tableInfo._attrs[rhsAttrID];
                if (((data[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ||
                    ((data[rhsAttrID >> 3] >> (rhsAttrID & 7)) & 1) ||
                    !compareData(data + lhsAttr._offset, data + rhsAttr._offset, condition._op, lhsAttr._attrType)) {
                    ok = false;
                    break;
                }
            } else if (condition._op != IS_NULL && condition._op != IS_NOT_NULL) {
                //情况2: 数值
                if (condition._rhsValues.empty()) {
                    if (((data[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ||
                        !compareData(data + lhsAttr._offset, (char *) condition._rhsValue._data, condition._op, lhsAttr._attrType)) {
                        ok = false;
                        break;
                    }
                } else {
                    ok = false;
                    if ((data[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) {
                        for (const auto &value: condition._rhsValues) {
                            if (value._data == nullptr) {
                                ok = true;
                                break;
                            }
                        }
                    } else {
                        for (const auto &value: condition._rhsValues) {
                            if (value._data != nullptr && compareData(data + lhsAttr._offset, (char *) value._data, EQ_OP, lhsAttr._attrType)) {
                                ok = true;
                                break;
                            }
                        }
                    }
                    if (!ok) break;
                }
            } else {
                //情况3: 是否为空
                if (((data[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ^ (condition._op == IS_NULL)) {
                    ok = false;
                    break;
                }
            }
        }
        //如果符合条件，执行函数操作
        if (ok && !callback(rid, data)) {
            success = false;
            break;
        }
        if (indexHandle != nullptr) {
            //使用索引找到下一条记录
            hasNext = indexHandle->getNextEntry(rid);
            if (hasNext) handle.getRecord(rid, (BufType) data);
        } else hasNext = handle.getNextRecord(rid, (BufType) data);
    }
    delete[] data;
    if (indexHandle != nullptr) {
        _indexManager->closeIndex(_fileID);
        delete indexHandle;
    }
    _recordManager->closeFile(fileID);
    return success;
}

bool QueryManager::insertData(const string &tableName, const vector<vector<Value>> &value_list) {
    //检查表是否存在
    int table_id = _systemManager->getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
    RecordHandle recordHandle(_bufPageManager, _systemManager->getFileIDByName(tableName));
    char *data = new char[tableInfo._recordSize];
    int count = 0;
    bool ok = true;
    for (const auto &values : value_list) {
        //检查列数量是否匹配
        if (values.size() != tableInfo._attrNum) {
            cerr << "Column number does not match!" << endl;
            ok = false;
            break;
        }
        //检查非空约束、列类型是否匹配
        for (int i = 0; i < values.size(); i++) {
            if (values[i]._data == nullptr) {
                if (tableInfo._attrs[i]._notNull) {
                    cerr << "Column " + tableInfo._attrs[i]._attrName + " is not null!" << endl;
                    ok = false;
                    break;
                }
            } else if (values[i]._attrType != tableInfo._attrs[i]._attrType) {
                cerr << "The data type of column " + tableInfo._attrs[i]._attrName + " does not match!" << endl;
                ok = false;
                break;
            }
        }
        if (!ok) break;
        //检查主键约束
        if (!checkPrimaryConstraint(tableInfo, values)) {
            ok = false;
            break;
        }
        //检查外键约束
        if (!checkForeignConstraint(tableInfo, values)) {
            ok = false;
            break;
        }
        //检查唯一性约束
        if (!checkUniqueConstraint(tableInfo, values)) {
            ok = false;
            break;
        }
        //准备数据
        memset(data, 0, tableInfo._recordSize);
        for (int i = 0; i < values.size(); i++) {
            if (values[i]._data == nullptr) data[i >> 3] |= (1 << (i & 7));//标记空位图
            else if (values[i]._attrType == STRING)
                memcpy(data + tableInfo._attrs[i]._offset, values[i]._data, min(tableInfo._attrs[i]._attrLength - 1, (int) strlen((char *) values[i]._data)));
            else
                memcpy(data + tableInfo._attrs[i]._offset, values[i]._data, tableInfo._attrs[i]._attrLength);
        }
        //插入数据
        RID rid;
        recordHandle.insertRecord((BufType) data, rid);
        //插入主键
        if (!tableInfo._primaryKeys.empty()) {
            int fileID;
            //打开主键文件
            _indexManager->openIndex(tableInfo._tableName.c_str(), vector<string>(1, "primary"), fileID);
            IndexHandle indexHandle(_bufPageManager, fileID);
            auto primary = getKeyData(tableInfo, values, tableInfo._primaryKeys);
            indexHandle.insertEntry((BufType) primary.c_str(), rid, true, false);
            _indexManager->closeIndex(fileID);
        }
        //插入外键
        for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
            int fileID;
            //打开foreign文件
            vector<string> foreignAttrNames = vector<string>(tableInfo._foreignKeys[i]);
            foreignAttrNames.emplace_back("foreign");
            _indexManager->openIndex(tableName.c_str(), foreignAttrNames, fileID);
            IndexHandle indexHandle(_bufPageManager, fileID);
            auto foreign = getKeyData(tableInfo, values, tableInfo._foreignKeys[i]);
            indexHandle.insertEntry((BufType) foreign.c_str(), rid, false, false);
            _indexManager->closeIndex(fileID);
        }
        //插入索引
        for (int i = 0; i < tableInfo._indexNum; i++) {
            int fileID;
            //打开索引文件
            _indexManager->openIndex(tableName.c_str(), tableInfo._indexes[i], fileID);
            IndexHandle indexHandle(_bufPageManager, fileID);
            auto index = getKeyData(tableInfo, values, tableInfo._indexes[i]);
            indexHandle.insertEntry((BufType) index.c_str(), rid, false, false);
            _indexManager->closeIndex(fileID);
        }
        //插入unique
        for (int i = 0; i < tableInfo._uniqueNum; i++) {
            int fileID;
            //打开unique文件
            vector<string> uniqueAttrNames = vector<string>(tableInfo._uniques[i]);
            uniqueAttrNames.emplace_back("unique");
            _indexManager->openIndex(tableName.c_str(), uniqueAttrNames, fileID);
            IndexHandle indexHandle(_bufPageManager, fileID);
            auto unique = getKeyData(tableInfo, values, tableInfo._uniques[i]);
            indexHandle.insertEntry((BufType) unique.c_str(), rid, true, false);
            _indexManager->closeIndex(fileID);
        }
        count++;
    }
    delete[] data;
    cout << count << " row(s) affected" << endl;
    return ok;
}

bool QueryManager::deleteData(const string &tableName, const vector<Condition> &conditions) {
    //检查表是否存在
    int table_id = _systemManager->getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
    //检查过滤条件
    if (!checkConditions(tableInfo, conditions)) return false;
    RecordHandle recordHandle(_bufPageManager, _systemManager->getFileIDByName(tableName));
    IndexHandle *primaryHandle = nullptr;
    int fileID;
    vector<int> fileIDs;
    //主键索引文件
    if (!tableInfo._primaryKeys.empty()) {
        _indexManager->openIndex(tableInfo._tableName.c_str(), vector<string>(1, "primary"), fileID);
        primaryHandle = new IndexHandle(_bufPageManager, fileID);
        fileIDs.push_back(fileID);
    }
    //是否有其它表外键依赖本表
    vector<IndexHandle> referenceHandles;
    for (int i = 0; i < _systemManager->getTableNum(); i++) {
        const auto &foreignTableInfo = _systemManager->getTableInfoByID(i);
        for (int j = 0; j < foreignTableInfo._foreignKeyNum; j++) {
            if (foreignTableInfo._references[j] == tableName) {
                vector<string> foreignAttrNames = vector<string>(foreignTableInfo._foreignKeys[j]);
                foreignAttrNames.emplace_back("foreign");
                _indexManager->openIndex(foreignTableInfo._tableName.c_str(), foreignAttrNames, fileID);
                referenceHandles.emplace_back(_bufPageManager, fileID);
                fileIDs.push_back(fileID);
            }
        }
    }
    //外键索引文件
    vector<IndexHandle> foreignHandles;
    for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
        vector<string> foreignAttrNames = vector<string>(tableInfo._foreignKeys[i]);
        foreignAttrNames.emplace_back("foreign");
        _indexManager->openIndex(tableName.c_str(), foreignAttrNames, fileID);
        foreignHandles.emplace_back(_bufPageManager, fileID);
        fileIDs.push_back(fileID);
    }
    //索引文件
    vector<IndexHandle> indexHandles;
    for (int i = 0; i < tableInfo._indexNum; i++) {
        _indexManager->openIndex(tableName.c_str(), tableInfo._indexes[i], fileID);
        indexHandles.emplace_back(_bufPageManager, fileID);
        fileIDs.push_back(fileID);
    }
    //unique文件
    vector<IndexHandle> uniqueHandles;
    for (int i = 0; i < tableInfo._uniqueNum; i++) {
        vector<string> uniqueAttrNames = vector<string>(tableInfo._uniques[i]);
        uniqueAttrNames.emplace_back("unique");
        _indexManager->openIndex(tableName.c_str(), uniqueAttrNames, fileID);
        uniqueHandles.emplace_back(_bufPageManager, fileID);
        fileIDs.push_back(fileID);
    }
    //先检查是否有非法操作
    bool ok = filterTable(tableInfo, conditions,
                [&tableInfo, &referenceHandles, this]
                (const RID &rid, const char *data) -> bool {
        //检查主外键约束，有依赖的记录不能删除
        for (auto &referenceHandle : referenceHandles) {
            auto reference = getKeyData(tableInfo, data, tableInfo._primaryKeys);
            if (!referenceHandle.insertEntry((BufType) reference.c_str(), rid, true, true)) {
                cerr << "Foreign key on data!" << endl;
                return false;
            }
        }
        return true;
    });
    //若合法则完成删除
    if (ok) {
        int count = 0;//记录删除数量
        filterTable(tableInfo, conditions,
                    [&count, &tableInfo, &recordHandle, primaryHandle, &foreignHandles, &indexHandles, &uniqueHandles, this]
                    (const RID &rid, const char *data) -> bool {
            //删除数据记录
            recordHandle.deleteRecord(rid);
            //删除主键索引
            if (primaryHandle != nullptr) {
                auto primary = getKeyData(tableInfo, data, tableInfo._primaryKeys);
                primaryHandle->deleteEntry((BufType) primary.c_str(), rid);
            }
            //删除外键索引
            for (int i = 0; i < foreignHandles.size(); i++) {
                auto foreign = getKeyData(tableInfo, data, tableInfo._foreignKeys[i]);
                foreignHandles[i].deleteEntry((BufType) foreign.c_str(), rid);
            }
            //删除索引
            for (int i = 0; i < indexHandles.size(); i++) {
                auto index = getKeyData(tableInfo, data, tableInfo._indexes[i]);
                indexHandles[i].deleteEntry((BufType) index.c_str(), rid);
            }
            //删除unique
            for (int i = 0; i < uniqueHandles.size(); i++) {
                auto unique = getKeyData(tableInfo, data, tableInfo._uniques[i]);
                uniqueHandles[i].deleteEntry((BufType) unique.c_str(), rid);
            }
            count++;
            return true;
        });
        cout << count << " row(s) affected" << endl;
    }
    if (!tableInfo._primaryKeys.empty()) delete primaryHandle;
    //关闭所有索引文件
    for (const auto &_fileID: fileIDs) {
        _indexManager->closeIndex(_fileID);
    }
    return ok;
}

bool QueryManager::updateData(const string &tableName, const vector<RelAttr> &relAttrs, const vector<Value> &values, const vector<Condition> &conditions) {
    //检查表是否存在
    int table_id = _systemManager->getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
    vector<string> attrNames;
    //检查列是否存在，非空约束、列类型是否匹配
    for (int i = 0; i < values.size(); i++) {
        const auto &attr_id = _systemManager->getAttrIDByName(tableInfo, relAttrs[i]._attrName);
        if (attr_id == -1) {
            cerr << "Column " + relAttrs[i]._attrName + " does not exist!" << endl;
            return false;
        }
        const auto &attr = tableInfo._attrs[attr_id];
        if (values[i]._data == nullptr) {
            if (attr._notNull) {
                cerr << "Column " + attr._attrName + " is not null!" << endl;
                return false;
            }
        } else if (values[i]._attrType != attr._attrType) {
            cerr << "The data type of column " + attr._attrName + " does not match!" << endl;
            return false;
        }
        attrNames.push_back(attr._attrName);
    }
    //检查过滤条件
    if (!checkConditions(tableInfo, conditions)) return false;
    RecordHandle recordHandle(_bufPageManager, _systemManager->getFileIDByName(tableName));
    IndexHandle *primaryHandle = nullptr;
    int fileID;
    vector<int> fileIDs;
    //主键索引文件
    if (!tableInfo._primaryKeys.empty()) {
        vector<string> intersect;
        intersection(tableInfo._primaryKeys, attrNames, intersect);
        //只有更新的字段与主键有交集才需考虑
        if (!intersect.empty()) {
            _indexManager->openIndex(tableName.c_str(), vector<string>(1, "primary"), fileID);
            primaryHandle = new IndexHandle(_bufPageManager, fileID);
            fileIDs.push_back(fileID);
        }
    }
    //是否有其它表外键依赖本表，只有主键被更新时才需考虑
    vector<IndexHandle> referenceHandles;
    if (primaryHandle != nullptr) {
        for (int i = 0; i < _systemManager->getTableNum(); i++) {
            const auto &foreignTableInfo = _systemManager->getTableInfoByID(i);
            for (int j = 0; j < foreignTableInfo._foreignKeyNum; j++) {
                if (foreignTableInfo._references[j] == tableName) {
                    vector<string> foreignAttrNames = vector<string>(foreignTableInfo._foreignKeys[j]);
                    foreignAttrNames.emplace_back("foreign");
                    _indexManager->openIndex(foreignTableInfo._tableName.c_str(), foreignAttrNames, fileID);
                    referenceHandles.emplace_back(_bufPageManager, fileID);
                    fileIDs.push_back(fileID);
                }
            }
        }
    }
    //外键索引文件
    vector<pair<IndexHandle, IndexHandle>> foreignHandles;
    for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
        vector<string> intersect;
        intersection(tableInfo._foreignKeys[i], attrNames, intersect);
        //只有更新的字段与外键有交集才需考虑
        if (!intersect.empty()) {
            vector<string> foreignAttrNames = vector<string>(tableInfo._foreignKeys[i]);
            foreignAttrNames.emplace_back("foreign");
            _indexManager->openIndex(tableName.c_str(), foreignAttrNames, fileID);
            IndexHandle indexHandle1(_bufPageManager, fileID);
            fileIDs.push_back(fileID);
            _indexManager->openIndex(tableInfo._references[i].c_str(), vector<string>(1, "primary"), fileID);
            IndexHandle indexHandle2(_bufPageManager, fileID);
            fileIDs.push_back(fileID);
            foreignHandles.emplace_back(indexHandle1, indexHandle2);
        }
    }
    //索引文件
    vector<IndexHandle> indexHandles;
    for (int i = 0; i < tableInfo._indexNum; i++) {
        vector<string> intersect;
        intersection(tableInfo._indexes[i], attrNames, intersect);
        //只有更新的字段与索引文件有交集才需考虑
        if (!intersect.empty()) {
            _indexManager->openIndex(tableName.c_str(), tableInfo._indexes[i], fileID);
            indexHandles.emplace_back(_bufPageManager, fileID);
            fileIDs.push_back(fileID);
        }
    }
    //unique文件
    vector<IndexHandle> uniqueHandles;
    for (int i = 0; i < tableInfo._uniqueNum; i++) {
        vector<string> intersect;
        intersection(tableInfo._uniques[i], attrNames, intersect);
        //只有更新的字段与unique文件有交集才需考虑
        if (!intersect.empty()) {
            vector<string> uniqueAttrNames = vector<string>(tableInfo._uniques[i]);
            uniqueAttrNames.emplace_back("unique");
            _indexManager->openIndex(tableName.c_str(), uniqueAttrNames, fileID);
            uniqueHandles.emplace_back(_bufPageManager, fileID);
            fileIDs.push_back(fileID);
        }
    }
    char *newData = new char[tableInfo._recordSize];
    int count = 0;//记录更新数量
    bool ok = filterTable(tableInfo, conditions,
                          [&count, &tableInfo, newData, &relAttrs, &values, &recordHandle, primaryHandle, &foreignHandles, &indexHandles, &uniqueHandles, &referenceHandles, this]
                          (const RID &rid, const char *data) -> bool {
        //构造更新后的数据
        memcpy(newData, data, tableInfo._recordSize);
        for (int i = 0; i < relAttrs.size(); i++) {
                for (int j = 0; j < tableInfo._attrs.size(); j++) {
                    if (tableInfo._attrs[j]._attrName == relAttrs[i]._attrName) {
                        //首先更新空位图
                        if (values[i]._data == nullptr) newData[j >> 3] |= (1 << (j & 7));
                        else {
                            newData[j >> 3] &= ~(1 << (j & 7));
                            if (values[i]._attrType == STRING) {
                                //先将字符串清零，避免原来的字符串残留
                                memset(newData + tableInfo._attrs[j]._offset, 0, tableInfo._attrs[j]._attrLength);
                                memcpy(newData + tableInfo._attrs[j]._offset, values[i]._data, min(tableInfo._attrs[j]._attrLength - 1, (int) strlen((char *) values[i]._data)));
                            } else {
                                memcpy(newData + tableInfo._attrs[j]._offset, values[i]._data, tableInfo._attrs[j]._attrLength);
                            }
                        }
                        break;
                    }
                }
            }
        if (primaryHandle != nullptr) {
            auto origin = getKeyData(tableInfo, data, tableInfo._primaryKeys);
            auto primary = getKeyData(tableInfo, newData, tableInfo._primaryKeys);
            //排除无效的更新
            if (origin != primary) {
                //检查是否有重复主键
                if (!primaryHandle->insertEntry((BufType) primary.c_str(), rid, true, true)) {
                    cerr << "Repetitive primary keys!" << endl;
                    return false;
                }
                //检查主外键约束，有依赖的记录不能更新
                for (auto &referenceHandle : referenceHandles) {
                    if (!referenceHandle.insertEntry((BufType) origin.c_str(), rid, true, true)) {
                        cerr << "Foreign key on data!" << endl;
                        return false;
                    }
                }
            }
        }
        for (int i = 0; i < foreignHandles.size(); i++) {
            auto foreign = getKeyData(tableInfo, newData, tableInfo._foreignKeys[i]);
            //检查新外键是否出现在参照表中
            if (foreignHandles[i].second.insertEntry((BufType) foreign.c_str(), rid, true, true)) {
                cerr << "Foreign key value is not in the reference table!" << endl;
                return false;
            }
        }
        for (int i = 0; i < uniqueHandles.size(); i++) {
            auto origin = getKeyData(tableInfo, data, tableInfo._uniques[i]);
            auto unique = getKeyData(tableInfo, newData, tableInfo._uniques[i]);
            //排除无效的更新
            if (origin != unique) {
                //检查是否破坏唯一性冲突
                if (!uniqueHandles[i].insertEntry((BufType) unique.c_str(), rid, true, true)) {
                    cerr << "Unique columns have duplicated values!" << endl;
                    return false;
                }
            }
        }
        //更新数据记录
        recordHandle.updateRecord(rid, (BufType) newData);
        //更新所有键值
        if (primaryHandle != nullptr) {
            updateKeyData(tableInfo, rid, *primaryHandle, data, newData, tableInfo._primaryKeys, true);
        }
        for (int i = 0; i < foreignHandles.size(); i++) {
            updateKeyData(tableInfo, rid, foreignHandles[i].first, data, newData, tableInfo._foreignKeys[i], false);
        }
        for (int i = 0; i < indexHandles.size(); i++) {
            updateKeyData(tableInfo, rid, indexHandles[i], data, newData, tableInfo._indexes[i], false);
        }
        for (int i = 0; i < uniqueHandles.size(); i++) {
            updateKeyData(tableInfo, rid, uniqueHandles[i], data, newData, tableInfo._uniques[i], true);
        }
        count++;
        return true;
    });
    cout << count << " row(s) affected" << endl;
    if (!tableInfo._primaryKeys.empty()) delete primaryHandle;
    for (const auto &_fileID : fileIDs) {
        _indexManager->closeIndex(_fileID);
    }
    delete[] newData;
    return ok;
}

bool QueryManager::selectData(const vector<string> &tableNames, const vector<RelAttr> &relAttrs, const vector<Condition> &conditions, int limit, int offset) {
    if (tableNames.size() == 1) {
        //检查表是否存在
        int table_id = _systemManager->getTableIDByName(tableNames[0]);
        if (table_id == -1) {
            cerr << "Table " << tableNames[0] << " does not exist!" << endl;
            return false;
        }
        const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
        vector<string> attrNames;
        for (const auto &relAttr : relAttrs) {
            if (_systemManager->getAttrIDByName(tableInfo, relAttr._attrName) == -1) {
                cerr << "Column " + relAttr._attrName + " does not exist!" << endl;
                return false;
            }
            attrNames.push_back(relAttr._attrName);
        }
        if (!checkConditions(tableInfo, conditions)) return false;
        //记录每列长度
        vector<int> headerLength;
        vector<string> colNames;
        for (const auto &attr : tableInfo._attrs) {
            if (relAttrs.empty() || find(attrNames.begin(), attrNames.end(), attr._attrName) != attrNames.end()) {
                if (attr._attrType == INTEGER) headerLength.push_back(max(10, (int) attr._attrName.length()));
                else if (attr._attrType == FLOAT) headerLength.push_back(max(12, (int) attr._attrName.length()));
                else headerLength.push_back(max(attr._attrLength, (int) attr._attrName.length()));
                colNames.push_back(attr._attrName);
            }
        }
        //输出表头
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        cout << "| ";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setw(headerLength[i]) << colNames[i] << " | ";
        }
        cout << endl;
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        int count = 0;
        clock_t start = clock();
        filterTable(tableInfo, conditions, [&headerLength, &count, &tableInfo, &colNames, &limit, &offset](const RID &rid, const char *data) -> bool {
            if (offset == 0) {
                cout << "|";
                int pos = 0;
                auto iter = colNames.begin();
                for (int i = 0; i < tableInfo._attrs.size(); i++) {
                    if (*iter == tableInfo._attrs[i]._attrName) {
                        cout << " ";
                        if ((data[i >> 3] >> (i & 7)) & 1) cout << setw(headerLength[pos]) << "NULL";
                        else if (tableInfo._attrs[i]._attrType == INTEGER) {
                            int a;
                            memcpy(&a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                            cout << setw(headerLength[pos]) << a;
                        } else if (tableInfo._attrs[i]._attrType == FLOAT) {
                            float a;
                            memcpy(&a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                            cout << setw(headerLength[pos]) << a;
                        } else {
                            char *a = new char[tableInfo._attrs[i]._attrLength];
                            memcpy(a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                            cout << setw(headerLength[pos]) << a;
                            delete[] a;
                        }
                        cout << " |";
                        pos++;
                        if (iter++ == colNames.end()) break;
                    }
                }
                cout << endl;
                count++;
            } else offset--;
            return count < limit;
        });
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        cout << count << " row(s) in set (" << (double)(clock() - start) / CLOCKS_PER_SEC << " sec)" << endl;
    } else if (tableNames.size() == 2) {
        //检查表是否存在
        int table_id_1 = _systemManager->getTableIDByName(tableNames[0]);
        int table_id_2 = _systemManager->getTableIDByName(tableNames[1]);
        if (table_id_1 == -1) {
            cerr << "Table " << tableNames[0] << " does not exist!" << endl;
            return false;
        }
        if (table_id_2 == -1) {
            cerr << "Table " << tableNames[1] << " does not exist!" << endl;
            return false;
        }
        const TableInfo &tableInfo1 = _systemManager->getTableInfoByID(table_id_1);
        const TableInfo &tableInfo2 = _systemManager->getTableInfoByID(table_id_2);
        //检查列是否在对应表中存在
        for (const auto &relAttr : relAttrs) {
            if (relAttr._relName == tableInfo1._tableName) {
                if (_systemManager->getAttrIDByName(tableInfo1, relAttr._attrName) == -1) {
                    cerr << "Column " + relAttr._attrName + " does not exist in " << tableInfo1._tableName << "!" << endl;
                    return false;
                }
            } else if (relAttr._relName == tableInfo2._tableName) {
                if (_systemManager->getAttrIDByName(tableInfo2, relAttr._attrName) == -1) {
                    cerr << "Column " + relAttr._attrName + " does not exist in " << tableInfo2._tableName << "!" << endl;
                    return false;
                }
            } else {
                cerr << "Table " << relAttr._relName << " is undefined!" << endl;
                return false;
            }
        }
        //检查条件是否合法
        for (const auto &condition : conditions) {
            int lhsAttrID;
            if (condition._lhsAttr._relName == tableInfo1._tableName) {
                lhsAttrID = _systemManager->getAttrIDByName(tableInfo1, condition._lhsAttr._attrName);
            } else if (condition._lhsAttr._relName == tableInfo2._tableName) {
                lhsAttrID = _systemManager->getAttrIDByName(tableInfo2, condition._lhsAttr._attrName);
            } else {
                cerr << "Table " << condition._lhsAttr._relName << " is undefined!" << endl;
                return false;
            }
            //条件左值对应表
            const TableInfo &lTableInfo = condition._lhsAttr._relName == tableInfo1._tableName ? tableInfo1 : tableInfo2;
            if (lhsAttrID == -1) {
                cerr << "Column " + condition._lhsAttr._attrName + " does not exist in " << lTableInfo._tableName << "!" << endl;
                return false;
            }
            if (condition._rhsIsAttr) {
                int rhsAttrID;
                if (condition._rhsAttr._relName == tableInfo1._tableName) {
                    rhsAttrID = _systemManager->getAttrIDByName(tableInfo1, condition._rhsAttr._attrName);
                } else if (condition._rhsAttr._relName == tableInfo2._tableName) {
                    rhsAttrID = _systemManager->getAttrIDByName(tableInfo2, condition._rhsAttr._attrName);
                } else {
                    cerr << "Table " << condition._rhsAttr._relName << " is undefined!" << endl;
                    return false;
                }
                //条件右值对应表
                const TableInfo &rTableInfo = condition._rhsAttr._relName == tableInfo1._tableName ? tableInfo1 : tableInfo2;
                if (rhsAttrID == -1) {
                    cerr << "Column " + condition._rhsAttr._attrName + " does not exist in " << rTableInfo._tableName << "!" << endl;
                    return false;
                }
                if (lTableInfo._attrs[lhsAttrID]._attrType != rTableInfo._attrs[rhsAttrID]._attrType) {
                    cerr << "Column types do not match in filter condition!" << endl;
                    return false;
                }
            } else if (condition._op != IS_NULL && condition._op != IS_NOT_NULL) {
                //检查值类型是否匹配
                if (condition._rhsValues.empty()) {
                    if (lTableInfo._attrs[lhsAttrID]._attrType != condition._rhsValue._attrType) {
                        cerr << "Column type does not match value type in filter condition!" << endl;
                        return false;
                    }
                } else {
                    for (const auto &value : condition._rhsValues) {
                        if (lTableInfo._attrs[lhsAttrID]._attrType != value._attrType) {
                            cerr << "Column type does not match value type in filter condition!" << endl;
                            return false;
                        }
                    }
                }
            }
        }
        //统计两张表需要输出的列
        vector<string> attrNames1;
        vector<string> attrNames2;
        if (relAttrs.empty()) {
            //如果是*则输出所有列
            for (const auto &attr : tableInfo1._attrs) {
                attrNames1.push_back(attr._attrName);
            }
            for (const auto &attr : tableInfo2._attrs) {
                attrNames2.push_back(attr._attrName);
            }
        } else {
            for (const auto &relAttr: relAttrs) {
                if (relAttr._relName == tableInfo1._tableName) attrNames1.push_back(relAttr._attrName);
                else attrNames2.push_back(relAttr._attrName);
            }
        }
        //检查table2是否有对应条件列的索引
        bool hasIndex = false;
        for (const auto &condition : conditions) {
            if (condition._op == EQ_OP) {
                vector<string> conditionKey(1, condition._lhsAttr._attrName);
                if (find(tableInfo2._indexes.begin(), tableInfo2._indexes.end(), conditionKey) != tableInfo2._indexes.end()) {
                    hasIndex = true;
                    break;
                }
                if (tableInfo2._primaryKeys == conditionKey) {
                    hasIndex = true;
                    break;
                }
                if (find(tableInfo2._uniques.begin(), tableInfo2._uniques.end(), conditionKey) != tableInfo2._uniques.end()) {
                    hasIndex = true;
                    break;
                }
            }
        }
        //循环内的的表尽量有索引
        const TableInfo &inTableInfo = hasIndex ? tableInfo2 : tableInfo1;
        const TableInfo &outTableInfo = hasIndex ? tableInfo1 : tableInfo2;
        const vector<string> &inRelAttrs = hasIndex ? attrNames2 : attrNames1;
        const vector<string> &outRelAttrs = hasIndex ? attrNames1 : attrNames2;
        //记录每列长度
        vector<int> headerLength;
        vector<string> colNames;
        vector<string> attrNames;
        for (const auto &attr : outTableInfo._attrs) {
            if (find(outRelAttrs.begin(), outRelAttrs.end(), attr._attrName) != outRelAttrs.end()) {
                string colName = outTableInfo._tableName + "." + attr._attrName;
                if (attr._attrType == INTEGER) headerLength.push_back(max(10, (int) colName.length()));
                else if (attr._attrType == FLOAT) headerLength.push_back(max(12, (int) colName.length()));
                else headerLength.push_back(max(attr._attrLength, (int) colName.length()));
                colNames.push_back(colName);
                attrNames.push_back(attr._attrName);
            }
        }
        for (const auto &attr : inTableInfo._attrs) {
            if (find(inRelAttrs.begin(), inRelAttrs.end(), attr._attrName) != inRelAttrs.end()) {
                string colName = inTableInfo._tableName + "." + attr._attrName;
                if (attr._attrType == INTEGER) headerLength.push_back(max(10, (int) colName.length()));
                else if (attr._attrType == FLOAT) headerLength.push_back(max(12, (int) colName.length()));
                else headerLength.push_back(max(attr._attrLength, (int) colName.length()));
                colNames.push_back(colName);
                attrNames.push_back(attr._attrName);
            }
        }
        //输出表头
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        cout << "| ";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setw(headerLength[i]) << colNames[i] << " | ";
        }
        cout << endl;
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        int count = 0;
        clock_t start = clock();
        //打开外表的记录文件
        int fileID;
        _recordManager->openFile(outTableInfo._tableName.c_str(), fileID);
        RecordHandle handle(_bufPageManager, fileID);
        RID rid;
        char *outData = new char[outTableInfo._recordSize];
        //遍历外表，筛选出符合条件的记录，将内表的条件更新为对应数据
        handle.openScan();
        while (handle.getNextRecord(rid, (BufType) outData)) {
            bool ok = true;
            //内表的所有筛选条件
            vector<Condition> inConditions;
            for (const auto &condition : conditions) {
                if (condition._lhsAttr._relName == outTableInfo._tableName) {
                    int lhsAttrID = _systemManager->getAttrIDByName(outTableInfo, condition._lhsAttr._attrName);
                    const auto lhsAttr = outTableInfo._attrs[lhsAttrID];
                    if (condition._rhsIsAttr) {
                        if (condition._rhsAttr._relName == outTableInfo._tableName) {
                            //情况1: 外表 op 外表
                            int rhsAttrID = _systemManager->getAttrIDByName(outTableInfo, condition._rhsAttr._attrName);
                            const auto rhsAttr = outTableInfo._attrs[rhsAttrID];
                            if (((outData[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ||
                                ((outData[rhsAttrID >> 3] >> (rhsAttrID & 7)) & 1) ||
                                !compareData(outData + lhsAttr._offset, outData + rhsAttr._offset, condition._op, lhsAttr._attrType)) {
                                ok = false;
                                break;
                            }
                        } else {
                            //情况2: 外表 op 内表
                            //先检查外表数据是否为NULL,若是则认为不符合条件
                            if ((outData[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) {
                                ok = false;
                                break;
                            }
                            Condition inCondition;
                            //翻转内表位置
                            inCondition._lhsAttr._relName = condition._rhsAttr._relName;
                            inCondition._lhsAttr._attrName = condition._rhsAttr._attrName;
                            //翻转比较关系
                            if (condition._op == EQ_OP) inCondition._op = EQ_OP;
                            else if (condition._op == NE_OP) inCondition._op = NE_OP;
                            else if (condition._op == LE_OP) inCondition._op = GT_OP;
                            else if (condition._op == LT_OP) inCondition._op = GE_OP;
                            else if (condition._op == GE_OP) inCondition._op = LT_OP;
                            else if (condition._op == GT_OP) inCondition._op = LE_OP;
                            else cerr << "Invalid compare data!" << endl;
                            //将右值赋为具体数值
                            inCondition._rhsIsAttr = false;
                            inCondition._rhsValue._data = outData + lhsAttr._offset;
                            inCondition._rhsValue._attrType = lhsAttr._attrType;
                            inConditions.push_back(inCondition);
                        }
                    } else if (condition._op != IS_NULL && condition._op != IS_NOT_NULL) {
                        //情况3: 外表 op 数值
                        if (condition._rhsValues.empty()) {
                            if (((outData[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ||
                                !compareData(outData + lhsAttr._offset, (char *) condition._rhsValue._data, condition._op, lhsAttr._attrType)) {
                                ok = false;
                                break;
                            }
                        } else {
                            ok = false;
                            if ((outData[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) {
                                for (const auto &value: condition._rhsValues) {
                                    if (value._data == nullptr) {
                                        ok = true;
                                        break;
                                    }
                                }
                            } else {
                                for (const auto &value: condition._rhsValues) {
                                    if (value._data != nullptr && compareData(outData + lhsAttr._offset, (char *) value._data, EQ_OP, lhsAttr._attrType)) {
                                        ok = true;
                                        break;
                                    }
                                }
                            }
                            if (!ok) break;
                        }
                    } else {
                        //情况4: 外表 是否为空
                        if (((outData[lhsAttrID >> 3] >> (lhsAttrID & 7)) & 1) ^ (condition._op == IS_NULL)) {
                            ok = false;
                            break;
                        }
                    }
                } else if (condition._rhsIsAttr && condition._rhsAttr._relName == outTableInfo._tableName) {
                    //情况5: 内表 op 外表
                    int rhsAttrID = _systemManager->getAttrIDByName(outTableInfo, condition._rhsAttr._attrName);
                    const auto rhsAttr = outTableInfo._attrs[rhsAttrID];
                    //先检查外表数据是否为NULL,若是则认为不符合条件
                    if ((outData[rhsAttrID >> 3] >> (rhsAttrID & 7)) & 1) {
                        ok = false;
                        break;
                    }
                    Condition inCondition = condition;
                    //将右值赋为具体数值
                    inCondition._rhsIsAttr = false;
                    inCondition._rhsValue._data = outData + rhsAttr._offset;
                    inCondition._rhsValue._attrType = rhsAttr._attrType;
                    inConditions.push_back(inCondition);
                } else {
                    //情况6: 内表 op 数值
                    inConditions.push_back(condition);
                }
            }
            //对每条符合条件的外表记录，筛选出对应的符合条件的内表记录
            if (ok) {
                filterTable(inTableInfo, inConditions, [&headerLength, &count, &outTableInfo, &inTableInfo, &attrNames, outData, &limit, &offset](const RID &rid, const char *data) -> bool {
                    if (offset == 0) {
                        cout << "|";
                        int pos = 0;
                        //输出外表数据
                        auto iter = attrNames.begin();
                        for (int i = 0; i < outTableInfo._attrs.size(); i++) {
                            if (*iter == outTableInfo._attrs[i]._attrName) {
                                cout << " ";
                                if ((outData[i >> 3] >> (i & 7)) & 1) cout << setw(headerLength[pos]) << "NULL";
                                else if (outTableInfo._attrs[i]._attrType == INTEGER) {
                                    int a;
                                    memcpy(&a, outData + outTableInfo._attrs[i]._offset, outTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                } else if (outTableInfo._attrs[i]._attrType == FLOAT) {
                                    float a;
                                    memcpy(&a, outData + outTableInfo._attrs[i]._offset, outTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                } else {
                                    char *a = new char[outTableInfo._attrs[i]._attrLength];
                                    memcpy(a, outData + outTableInfo._attrs[i]._offset, outTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                    delete[] a;
                                }
                                cout << " |";
                                pos++;
                                if (iter++ == attrNames.end()) break;
                            }
                        }
                        //输出内表数据
                        for (int i = 0; i < inTableInfo._attrs.size(); i++) {
                            if (*iter == inTableInfo._attrs[i]._attrName) {
                                cout << " ";
                                if ((data[i >> 3] >> (i & 7)) & 1) cout << setw(headerLength[pos]) << "NULL";
                                else if (inTableInfo._attrs[i]._attrType == INTEGER) {
                                    int a;
                                    memcpy(&a, data + inTableInfo._attrs[i]._offset, inTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                } else if (inTableInfo._attrs[i]._attrType == FLOAT) {
                                    float a;
                                    memcpy(&a, data + inTableInfo._attrs[i]._offset, inTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                } else {
                                    char *a = new char[inTableInfo._attrs[i]._attrLength];
                                    memcpy(a, data + inTableInfo._attrs[i]._offset, inTableInfo._attrs[i]._attrLength);
                                    cout << setw(headerLength[pos]) << a;
                                    delete[] a;
                                }
                                cout << " |";
                                pos++;
                                if (iter++ == attrNames.end()) break;
                            }
                        }
                        cout << endl;
                        count++;
                    } else offset--;
                    return count < limit;
                });
                if (count == limit) break;
            }
        }
        cout << "+";
        for (int i = 0; i < colNames.size(); i++) {
            cout << setfill('-') << setw(headerLength[i] + 3) << "+";
        }
        cout << setfill(' ') << endl;
        cout << count << " row(s) in set (" << (double)(clock() - start) / CLOCKS_PER_SEC << " sec)" << endl;
        delete[] outData;
        _recordManager->closeFile(fileID);
    }
    return true;
}